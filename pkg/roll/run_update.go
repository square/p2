package roll

import (
	"fmt"
	"sync"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc"
	rcf "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
)

type update struct {
	fields.Update

	kps     kp.Store
	rcs     rcstore.Store
	hcheck  checker.ConsulHealthChecker
	labeler labels.Applicator
	sched   rc.Scheduler

	logger logging.Logger

	lock kp.Lock
}

// Create a new Update. The kp.Store, rcstore.Store, labels.Applicator and
// rc.Scheduler arguments should be the same as those of the RCs themselves. The
// session must be valid for the lifetime of the Update; maintaining this is the
// responsibility of the caller.
func NewUpdate(
	f fields.Update,
	kps kp.Store,
	rcs rcstore.Store,
	hcheck checker.ConsulHealthChecker,
	labeler labels.Applicator,
	sched rc.Scheduler,
	logger logging.Logger,
	lock kp.Lock,
) Update {
	return update{
		Update:  f,
		kps:     kps,
		rcs:     rcs,
		hcheck:  hcheck,
		labeler: labeler,
		sched:   sched,
		logger:  logger,
		lock:    lock,
	}
}

type Update interface {
	// Run will execute the Update and remove the old RC upon completion. Run
	// should claim exclusive ownership of both affected RCs, and release that
	// exclusivity upon completion. Run is long-lived and blocking; close the
	// quit channel to terminate it early. If an Update is interrupted, Run
	// should leave the RCs in a state such that it can later be called again to
	// resume. The return value indicates if the update completed (true) or if
	// it was terminated early (false).
	Run(quit <-chan struct{}) bool
}

// retries a given function until it returns a nil error or the quit channel is
// closed. returns true if it exited in the former case, false in the latter.
// errors are sent to the given logger with the given string as the message.
func RetryOrQuit(f func() error, quit <-chan struct{}, logger logging.Logger, errtext string) bool {
	for err := f(); err != nil; err = f() {
		logger.WithError(err).Errorln(errtext)
		select {
		case <-quit:
			return false
		case <-time.After(1 * time.Second):
			// unblock the select and loop again
		}
	}
	return true
}

func (u update) Run(quit <-chan struct{}) (ret bool) {
	u.logger.NoFields().Debugln("Locking")
	// TODO: implement API for blocking locks and use that instead of retrying
	if !RetryOrQuit(
		func() error { return u.lockRCs(quit) },
		quit,
		u.logger,
		"Could not lock update",
	) {
		return
	}
	defer u.unlockRCs(quit)

	u.logger.NoFields().Debugln("Enabling")
	if !RetryOrQuit(u.enable, quit, u.logger, "Could not enable/disable RCs") {
		return
	}

	u.logger.NoFields().Debugln("Launching health watch")
	var newFields rcf.RC
	var err error
	if !RetryOrQuit(func() error {
		newFields, err = u.rcs.Get(u.NewRC)
		return err
	}, quit, u.logger, "Could not read new RC") {
		return
	}

	hChecks := make(chan map[string]health.Result)
	hErrs := make(chan error)
	hQuit := make(chan struct{})
	defer close(hQuit)
	go u.hcheck.WatchService(string(newFields.Manifest.ID()), hChecks, hErrs, hQuit)

ROLL_LOOP:
	for {
		select {
		case <-quit:
			return
		case <-hErrs:
			u.logger.WithError(err).Errorln("Could not read health checks")
		case checks := <-hChecks:
			newNodes, err := u.countHealthy(u.NewRC, checks)
			if err != nil {
				u.logger.WithErrorAndFields(err, logrus.Fields{
					"new": newNodes,
				}).Errorln("Could not count nodes on new RC")
				break
			}
			oldNodes, err := u.countHealthy(u.OldRC, checks)
			if err != nil {
				u.logger.WithErrorAndFields(err, logrus.Fields{
					"old": oldNodes,
				}).Errorln("Could not count nodes on old RC")
				break
			}

			if newNodes.Desired > newNodes.Healthy {
				// we assume replication controllers do in fact "work", ie healthy
				// and current converge towards desired
				// in this case, they have not yet met, so we block
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes,
					"new": newNodes,
				}).Debugln("Blocking for more healthy new nodes")
				break
			}
			if newNodes.Desired >= u.DesiredReplicas {
				// note that we only exit the loop AFTER the desired nodes
				// become healthy - this ensures that, when the old RC is
				// cleaned up, we do not accidentally remove the nodes we just
				// assigned to the new RC
				// normal execution of this algorithm will never cause
				// newDesired > u.Desired, but if Run is resuming from an unusual
				// situation, we don't want to get stuck
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes,
					"new": newNodes,
				}).Debugln("Upgrade complete")
				break ROLL_LOOP
			}

			next := algorithm(oldNodes.Healthy, newNodes.Healthy, u.DesiredReplicas, u.MinimumReplicas)
			if next > 0 {
				u.logger.WithFields(logrus.Fields{
					"old":  oldNodes,
					"new":  newNodes,
					"next": next,
				}).Infoln("Undergoing next update")
				err = u.rcs.AddDesiredReplicas(u.OldRC, -next)
				if err != nil {
					u.logger.WithError(err).Errorln("Could not decrement old replica count")
					break
				}
				err = u.rcs.AddDesiredReplicas(u.NewRC, next)
				if err != nil {
					u.logger.WithError(err).Errorln("Could not increment new replica count")
					break
				}
			} else {
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes,
					"new": newNodes,
				}).Debugln("Blocking for more healthy old nodes")
			}
		}
	}

	// rollout complete, clean up old RC if told to do so
	if !u.LeaveOld {
		u.logger.NoFields().Infoln("Cleaning up old RC")
		if !RetryOrQuit(func() error { return u.rcs.SetDesiredReplicas(u.OldRC, 0) }, quit, u.logger, "Could not zero old replica count") {
			return
		}
		if !RetryOrQuit(func() error { return u.rcs.Enable(u.OldRC) }, quit, u.logger, "Could not enable old RC") {
			return
		}
		if !RetryOrQuit(func() error { return u.rcs.Delete(u.OldRC, false) }, quit, u.logger, "Could not delete old RC") {
			return
		}
	}
	return true // finally if we make it here, we can return true
}

func (u update) lockPath(id rcf.ID) string {
	// RUs want to lock the RCs they're mutating, but this lock is separate
	// from the RC lock (which is held by the rc.WatchDesires goroutine), so the
	// key being locked is different
	return kp.LockPath(kp.RCPath(id.String(), "update"))
}

func (u update) lockRCs(done <-chan struct{}) error {
	newPath := u.lockPath(u.NewRC)
	oldPath := u.lockPath(u.OldRC)

	err := u.lock.Lock(newPath)
	if _, ok := err.(kp.AlreadyLockedError); ok {
		return fmt.Errorf("could not lock new %s", u.NewRC)
	} else if err != nil {
		return err
	}

	err = u.lock.Lock(oldPath)
	if err != nil {
		// The second key couldn't be locked, so release the first key before retrying.
		RetryOrQuit(
			func() error { return u.lock.Unlock(newPath) },
			done,
			u.logger,
			fmt.Sprintf("unlocking %s", newPath),
		)
	}
	if _, ok := err.(kp.AlreadyLockedError); ok {
		return fmt.Errorf("could not lock old %s", u.OldRC)
	} else if err != nil {
		return err
	}

	return nil
}

// unlockRCs releases the locks on the old and new RCs. To avoid a system-wide deadlock in
// RCs,  this method ensures that the locks are always released, either by retying until
// individual releases are successful or until the session is reset.
func (u update) unlockRCs(done <-chan struct{}) {
	wg := sync.WaitGroup{}
	for _, path := range []string{u.lockPath(u.NewRC), u.lockPath(u.OldRC)} {
		wg.Add(1)
		go func(path string) {
			RetryOrQuit(
				func() error { return u.lock.Unlock(path) },
				done,
				u.logger,
				fmt.Sprintf("unlocking %s", path),
			)
			wg.Done()
		}(path)
	}
	wg.Wait()
}

// enable sets the old & new RCs to a known-good state to start a rolling update:
// the old RC should be disabled and the new RC should be enabled.
func (u update) enable() error {
	// Disable the old RC first to make sure that the two RCs don't fight each other.
	err := u.rcs.Disable(u.OldRC)
	if err != nil {
		return err
	}

	err = u.rcs.Enable(u.NewRC)
	if err != nil {
		return err
	}

	return nil
}

type rcNodeCounts struct {
	Desired int // the number of nodes the RC wants to be on
	Current int // the number of nodes the RC has scheduled itself on
	Real    int // the number of current nodes that have finished scheduling
	Healthy int // the number of real nodes that are healthy
}

func (u update) countHealthy(id rcf.ID, checks map[string]health.Result) (rcNodeCounts, error) {
	ret := rcNodeCounts{}
	rcFields, err := u.rcs.Get(id)
	if err != nil {
		return ret, err
	}
	ret.Desired = rcFields.ReplicasDesired

	nodes, err := rc.New(rcFields, u.kps, u.rcs, u.sched, u.labeler, u.logger).CurrentNodes()
	if err != nil {
		return ret, err
	}
	ret.Current = len(nodes)

	for _, node := range nodes {
		// TODO: is reality checking an rc-layer concern?
		realManifest, _, err := u.kps.Pod(kp.RealityPath(node, string(rcFields.Manifest.ID())))
		if err != nil {
			return ret, err
		}
		realSHA, _ := realManifest.SHA()
		targetSHA, _ := rcFields.Manifest.SHA()
		if targetSHA == realSHA {
			ret.Real++
		} else {
			// don't check health if the update isn't even done there yet
			continue
		}
		if hres, ok := checks[node]; ok && hres.Status == health.Passing {
			ret.Healthy++
		}
	}
	return ret, err
}

// the roll algorithm defines how to mutate RCs over time. it takes four args:
// - old: the number of nodes on the old RC
// - new: the number of nodes on the new RC
// - want: the number of nodes desired on the new RC (ie the target)
// - need: the number of nodes that must always be up (ie the minimum)
// given these four arguments, rollAlgorithm returns the number of nodes to add
// to the new RC and to delete from the old
//
// returns zero under the following circumstances:
// - new >= want (the update is done)
// - old+new <= need (at or below the minimum, update has to block)
func algorithm(old, new, want, need int) int {
	// how much "headroom" do we have between the number of nodes that are
	// currently healthy, and the number that must be healthy?
	// if we schedule more than this, we'll go below the minimum
	difference := old + new - need

	// how many nodes do we have left to go? we can't schedule more than this
	// or we'll go over the target
	// note that remaining < difference is possible depending on how many
	// old nodes are still alive
	remaining := want - new

	// heuristic time:
	if remaining <= 0 {
		// no nodes remaining, noop out
		return 0
	}
	if new >= need {
		// minimum is satisfied by new nodes, doesn't matter how many old ones
		// we kill. this includes the edge case where need==0
		return remaining
	}
	if difference <= 0 {
		// the case of need==0 was caught above. in this case, need>0 and
		// difference is non-positive. this means that we are at, or below, the
		// minimum, and we cannot schedule new nodes because we might take
		// down an old one (which would go below the minimum)
		return 0
	}
	// remaining>0 and difference>0. each one imposes an upper bound on how
	// many we can schedule at once, so return the minimum of the two
	if remaining < difference {
		return remaining
	}
	return difference
}
