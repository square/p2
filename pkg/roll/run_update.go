package roll

import (
	"fmt"

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

	session string
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
	session string,
) Update {
	return update{
		Update:  f,
		kps:     kps,
		rcs:     rcs,
		hcheck:  hcheck,
		labeler: labeler,
		sched:   sched,
		logger:  logger,
		session: session,
	}
}

type Update interface {
	// Prepare should be called immediately before Run. If it returns nil, then
	// Run can be called; otherwise it should not be called. This function should
	// do at least the following:
	//   - claim exclusive control of the RCs involved in this Update. If this
	//     exclusivity is somehow lost, Prepare should be called again.
	//   - enable or disable RCs as appropriate
	//   - verify the MinimumReplicas and DesiredReplicas of the Update, to
	//     make sure they are valid.
	Prepare() error
	// Run will execute the Update and remove the old RC upon completion. Run
	// assumes that it has exclusive control over the RCs in the Update. This
	// function will block until finished; close the quit channel to terminate
	// early. Run can be invoked again to resume after termination, if it did
	// not lose exclusivity of the RCs.
	Run(quit <-chan struct{}) error
}

func (u update) Prepare() error {
	u.logger.WithField("session", u.session).Debugln("Locking")
	if err := u.lock(u.session); err != nil {
		return err
	}

	u.logger.WithField("session", u.session).Debugln("Enabling/disabling")
	if err := u.enable(); err != nil {
		return err
	}

	newFields, err := u.rcs.Get(u.NewRC)
	if err != nil {
		return err
	}
	checks, err := u.hcheck.Service(newFields.Manifest.ID())
	if err != nil {
		return err
	}

	newNodes, err := u.countHealthy(u.NewRC, checks)
	if err != nil {
		return err
	}
	oldNodes, err := u.countHealthy(u.OldRC, checks)
	if err != nil {
		return err
	}

	if u.DesiredReplicas < u.MinimumReplicas {
		return fmt.Errorf("desired replicas (%d) less than minimum replicas (%d), update is invalid", u.DesiredReplicas, u.MinimumReplicas)
	}
	if newNodes.healthy+oldNodes.healthy <= u.MinimumReplicas {
		return fmt.Errorf("minimum (%d) is not satisfied by existing healthy nodes (%d old, %d new), update would block immediately", u.MinimumReplicas, oldNodes.healthy, newNodes.healthy)
	}
	return nil
}

func (u update) Run(quit <-chan struct{}) error {
	newFields, err := u.rcs.Get(u.NewRC)
	if err != nil {
		return err
	}
	hChecks := make(chan map[string]health.Result)
	hErrs := make(chan error)
	hQuit := make(chan struct{})
	defer close(hQuit)
	go u.hcheck.WatchService(newFields.Manifest.ID(), hChecks, hErrs, hQuit)

ROLL_LOOP:
	for {
		select {
		case <-quit:
			return nil
		case <-hErrs:
			return err
		case checks := <-hChecks:
			newNodes, err := u.countHealthy(u.NewRC, checks)
			if err != nil {
				return err
			}
			oldNodes, err := u.countHealthy(u.OldRC, checks)
			if err != nil {
				return err
			}

			if newNodes.desired > newNodes.healthy {
				// we assume replication controllers do in fact "work", ie healthy
				// and current converge towards desired
				// in this case, they have not yet met, so we block
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes,
					"new": newNodes,
				}).Debugln("Blocking for more healthy new nodes")
				break
			}
			if newNodes.desired >= u.DesiredReplicas {
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

			next := algorithm(oldNodes.healthy, newNodes.healthy, u.DesiredReplicas, u.MinimumReplicas)
			u.logger.WithFields(logrus.Fields{
				"old":  oldNodes,
				"new":  newNodes,
				"next": next,
			}).Infoln("Undergoing next update")

			if next > 0 {
				if u.DeletePods {
					u.rcs.AddDesiredReplicas(u.OldRC, -next)
				}
				u.rcs.AddDesiredReplicas(u.NewRC, next)
			}
		}
	}

	// rollout complete, clean up old RC
	u.logger.NoFields().Infoln("Cleaning up old RC")
	if err := u.rcs.SetDesiredReplicas(u.OldRC, 0); err != nil {
		return err
	}
	if err := u.rcs.Enable(u.OldRC); err != nil {
		return err
	}
	if err := u.rcs.Delete(u.OldRC, false); err != nil {
		return err
	}
	return nil
}

func (u update) lockPath(id rcf.ID) string {
	// RUs want to lock the RCs they're mutating, but this lock is separate
	// from the RC lock (which is held by the rc.WatchDesires goroutine), so the
	// key being locked is different
	return kp.LockPath(kp.RCPath(id.String(), "update"))
}

func (u update) lock(session string) error {
	l := u.kps.NewUnmanagedLock(session, "")
	newPath := u.lockPath(u.NewRC)
	oldPath := u.lockPath(u.OldRC)

	err := l.Lock(newPath)
	if _, ok := err.(kp.AlreadyLockedError); ok {
		return fmt.Errorf("could not lock new %s", u.NewRC)
	} else if err != nil {
		return err
	}

	err = l.Lock(oldPath)
	if _, ok := err.(kp.AlreadyLockedError); ok {
		// release the other lock - no point checking this error, we can't
		// really act on it
		l.Unlock(newPath)
		return fmt.Errorf("could not lock old %s", u.OldRC)
	} else if err != nil {
		return err
	}

	return nil
}

func (u update) enable() error {
	err := u.rcs.Enable(u.NewRC)
	if err != nil {
		return err
	}

	if !u.DeletePods {
		err = u.rcs.Disable(u.OldRC)
	} else {
		err = u.rcs.Enable(u.OldRC)
	}
	if err != nil {
		return err
	}

	return nil
}

type rcNodeCounts struct {
	desired int
	current int
	healthy int
}

func (u update) countHealthy(id rcf.ID, checks map[string]health.Result) (rcNodeCounts, error) {
	ret := rcNodeCounts{}
	rcFields, err := u.rcs.Get(id)
	if err != nil {
		return ret, err
	}
	ret.desired = rcFields.ReplicasDesired

	nodes, err := rc.New(rcFields, u.kps, u.rcs, u.sched, u.labeler, u.logger).CurrentNodes()
	if err != nil {
		return ret, err
	}
	ret.current = len(nodes)

	for _, node := range nodes {
		if hres, ok := checks[node]; ok && hres.Status == health.Passing {
			ret.healthy++
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
// to the new RC (and, if DeletePods=true, to delete from the old)
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
