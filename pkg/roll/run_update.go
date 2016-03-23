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
	"github.com/square/p2/pkg/util"
)

type update struct {
	fields.Update

	kps     kp.Store
	rcs     rcstore.Store
	hcheck  checker.ConsulHealthChecker
	labeler labels.Applicator
	sched   rc.Scheduler

	logger logging.Logger

	session kp.Session

	oldRCUnlocker kp.Unlocker
	newRCUnlocker kp.Unlocker
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
	session kp.Session,
) Update {
	return &update{
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
	// Run will execute the Update and remove the old RC upon completion. Run
	// should claim exclusive ownership of both affected RCs, and release that
	// exclusivity upon completion. Run is long-lived and blocking; close the
	// quit channel to terminate it early. If an Update is interrupted, Run
	// should leave the RCs in a state such that it can later be called again to
	// resume. The return value indicates if the update completed (true) or if
	// it was terminated early (false).
	Run(quit <-chan struct{}) bool
}

// returned by shouldStop
type ruStep int

const (
	ruShouldTerminate ruStep = iota
	ruShouldBlock
	ruShouldContinue
)

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

func (u *update) Run(quit <-chan struct{}) (ret bool) {
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
		if rcstore.IsNotExist(err) {
			return util.Errorf("Replication controller %s is unexpectedly empty", u.NewRC)
		} else if err != nil {
			return err
		}

		return nil
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

			if nextAction := u.shouldStop(oldNodes, newNodes); nextAction == ruShouldTerminate {
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes,
					"new": newNodes,
				}).Debugln("Upgrade complete")
				break ROLL_LOOP
			} else if nextAction == ruShouldBlock {
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes,
					"new": newNodes,
				}).Debugln("Upgrade almost complete, blocking for more healthy new nodes")
				break
			}

			nextRemove, nextAdd := rollAlgorithm(u.rollAlgorithmParams(oldNodes, newNodes))
			if nextRemove > 0 || nextAdd > 0 {
				// apply the delay only if we've already added to the new RC, since there's
				// no value in sitting around doing nothing before anything has happened.
				if newNodes.Desired > 0 && u.RollDelay > time.Duration(0) {
					u.logger.WithField("delay", u.RollDelay).Infof("Waiting %v before continuing deploy", u.RollDelay)

					select {
					case <-time.After(u.RollDelay):
					case <-quit:
						return
					}

					// determine the new value of `next`, which may have changed
					// following the delay.
					nextRemove, nextAdd, err = u.shouldRollAfterDelay(newFields)

					if err != nil {
						u.logger.NoFields().Errorln(err)
						break
					}
				}

				u.logger.WithFields(logrus.Fields{
					"old":        oldNodes,
					"new":        newNodes,
					"nextRemove": nextRemove,
					"nextAdd":    nextAdd,
				}).Infof("Adding %d new nodes and removing %d old nodes", nextAdd, nextRemove)
				if nextRemove > 0 {
					err = u.rcs.AddDesiredReplicas(u.OldRC, -nextRemove)
					if err != nil {
						u.logger.WithError(err).Errorln("Could not decrement old replica count")
						break
					}
				}
				if nextAdd > 0 {
					err = u.rcs.AddDesiredReplicas(u.NewRC, nextAdd)
					if err != nil {
						u.logger.WithError(err).Errorln("Could not increment new replica count")
						break
					}
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

func (u *update) shouldStop(oldNodes, newNodes rcNodeCounts) ruStep {
	if newNodes.Desired < u.DesiredReplicas {
		// Not enough nodes scheduled on the new side, so deploy should continue.
		return ruShouldContinue
	}

	// Enough nodes are scheduled on the new side.

	if newNodes.Current >= u.DesiredReplicas {
		// We only ask for the new RC to have labeled the desired number of nodes
		// (number of nodes labeled is reflected in newNodes.Current)
		// before declaring an upgrade to be complete.
		// No need to check health for two reasons:
		// 1) So that if a deployer intentionally deploys a known-bad SHA
		// for which no nodes become healthy (specifying minimum == 0),
		// we terminate and don't leave an RU and old RC lying around.
		// 2) rollAlgorithm is responsible for maintaining the min health guarantee,
		// not this code. If we reach this point, rollAlgorithm is finished anyway.
		// The only decision to make is whether to terminate or block.
		return ruShouldTerminate
	}

	// Fewer nodes are owned by the new RC than are desired.
	// We don't need to schedule new nodes, but we can't terminate the deploy yet.
	return ruShouldBlock
}

func (u *update) lockRCs(done <-chan struct{}) error {
	newUnlocker, err := u.rcs.LockForMutation(u.NewRC, u.session)
	if _, ok := err.(kp.AlreadyLockedError); ok {
		return fmt.Errorf("could not lock new %s", u.NewRC)
	} else if err != nil {
		return err
	}
	u.newRCUnlocker = newUnlocker

	oldUnlocker, err := u.rcs.LockForMutation(u.OldRC, u.session)
	if err != nil {
		// The second key couldn't be locked, so release the first key before retrying.
		RetryOrQuit(
			func() error { return newUnlocker.Unlock() },
			done,
			u.logger,
			fmt.Sprintf("unlocking %s", newUnlocker.Key()),
		)
	}
	if _, ok := err.(kp.AlreadyLockedError); ok {
		return fmt.Errorf("could not lock old %s", u.OldRC)
	} else if err != nil {
		return err
	}
	u.oldRCUnlocker = oldUnlocker

	return nil
}

// unlockRCs releases the locks on the old and new RCs. To avoid a system-wide deadlock in
// RCs, this method ensures that the locks are always released, either by retrying until
// individual releases are successful or until the session is reset.
func (u *update) unlockRCs(done <-chan struct{}) {
	wg := sync.WaitGroup{}
	for _, unlocker := range []kp.Unlocker{u.newRCUnlocker, u.oldRCUnlocker} {
		// unlockRCs is called whenever Run() exits, so we have to
		// handle the case where we didn't lock anything yet
		if unlocker != nil {
			wg.Add(1)
			go func(unlocker kp.Unlocker) {
				defer wg.Done()
				RetryOrQuit(
					func() error {
						return unlocker.Unlock()
					},
					done,
					u.logger,
					fmt.Sprintf("unlocking rc: %s", unlocker.Key()),
				)
			}(unlocker)
		}
	}
	wg.Wait()
}

// enable sets the old & new RCs to a known-good state to start a rolling update:
// the old RC should be disabled and the new RC should be enabled.
func (u *update) enable() error {
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
	Desired   int // the number of nodes the RC wants to be on
	Current   int // the number of nodes the RC has scheduled itself on
	Real      int // the number of current nodes that have finished scheduling
	Healthy   int // the number of real nodes that are healthy
	Unhealthy int // the number of real nodes that are unhealthy
	Unknown   int // the number of real nodes that are of unknown health
}

func (u *update) countHealthy(id rcf.ID, checks map[string]health.Result) (rcNodeCounts, error) {
	ret := rcNodeCounts{}
	rcFields, err := u.rcs.Get(id)
	if rcstore.IsNotExist(err) {
		err := util.Errorf("RC %s did not exist", id)
		return ret, err
	} else if err != nil {
		return ret, err
	}

	ret.Desired = rcFields.ReplicasDesired

	currentPods, err := rc.New(rcFields, u.kps, u.rcs, u.sched, u.labeler, u.logger).CurrentPods()
	if err != nil {
		return ret, err
	}
	ret.Current = len(currentPods)

	if ret.Desired > ret.Current {
		// This implies that the RC hasn't yet scheduled pods that it desires to have.
		// We consider their health to be unknown in this case.
		// Note that the below loop over `range currentPods` may also increase `ret.Unknown`.
		ret.Unknown = ret.Desired - ret.Current
	}

	for _, pod := range currentPods {
		node := pod.Node
		// TODO: is reality checking an rc-layer concern?
		realManifest, _, err := u.kps.Pod(kp.REALITY_TREE, node, rcFields.Manifest.ID())
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
		if hres, ok := checks[node]; ok {
			if hres.Status == health.Passing {
				ret.Healthy++
			} else if hres.Status == health.Unknown {
				ret.Unknown++
			} else {
				ret.Unhealthy++
			}
		} else {
			ret.Unknown++
		}
	}
	return ret, err
}

func (u *update) shouldRollAfterDelay(newFields rcf.RC) (int, int, error) {
	// Check health again following the roll delay. If things have gotten
	// worse since we last looked, or there is an error, we break this iteration.
	checks, err := u.hcheck.Service(newFields.Manifest.ID().String())
	if err != nil {
		return 0, 0, util.Errorf("Could not retrieve health following delay: %v", err)
	}

	afterDelayNew, err := u.countHealthy(u.NewRC, checks)
	if err != nil {
		return 0, 0, util.Errorf("Could not determine new service health: %v", err)
	}

	afterDelayOld, err := u.countHealthy(u.OldRC, checks)
	if err != nil {
		return 0, 0, util.Errorf("Could not determine old service health: %v", err)
	}

	afterDelayRemove, afterDelayAdd := rollAlgorithm(u.rollAlgorithmParams(afterDelayOld, afterDelayNew))

	if afterDelayRemove <= 0 && afterDelayAdd <= 0 {
		return 0, 0, util.Errorf("No nodes can be safely updated after %v roll delay, will wait again", u.RollDelay)
	}

	return afterDelayRemove, afterDelayAdd, nil
}

func (u *update) rollAlgorithmParams(oldHealth, newHealth rcNodeCounts) (oldHealthy, newHealthy, currentDesired, targetDesired, minHealthy int) {
	// We conservatively treat Unknown nodes as healthy on the old side.
	oldHealthy = oldHealth.Healthy + oldHealth.Unknown
	if oldHealth.Desired < oldHealthy {
		// Because of the non-atomicity of our KV stores,
		// we may run into this situation while decrementing old RC's count:
		// old RC has decremented desire, but not yet removed RC label from the pod.
		// We will see this as {Desired: 1, Healthy: 2}, or similar.
		// We assume that the old RC will eventually cause Healthy to go to 1 here.
		// So, we should assume that oldHealthy is is the lesser of the two.
		// Otherwise, we may over-eagerly remove more nodes from old RC.
		// That would cause a violation of minimum health.
		oldHealthy = oldHealth.Desired
	}
	newHealthy = newHealth.Healthy
	currentDesired = oldHealth.Desired + newHealth.Desired
	targetDesired = u.DesiredReplicas
	minHealthy = u.MinimumReplicas
	return
}

// the roll algorithm defines how to mutate RCs over time. it takes five args:
// - old: the number of healthy nodes on the old RC
// - new: the number of healthy nodes on the new RC
// - currentDesired: the number of nodes currently desired in total between the old and new RCs
// - targetDesired: the number of nodes desired on the new RC (ie the target) in the final state
// - minHealthy: the number of nodes that must always be up (ie the minimum)
// given these five arguments, rollAlgorithm returns the number of nodes to add
// to the new RC and to delete from the old
//
// Returns two values, both of which are a positive number or zero:
// The first value indicates how many nodes to remove from the old RC.
// The second value indicates how many nodes to add to the old RC.
//
// Under the following circumstances, both return values will be zero:
// - new >= desired (the update is done)
// - old+new <= minHealthy (at or below the minimum, update has to block)
func rollAlgorithm(old, new, currentDesired, targetDesired, minHealthy int) (nodesToRemove, nodesToAdd int) {
	// how much "headroom" do we have between the number of nodes that are
	// currently healthy, and the number that must be healthy?
	// if we schedule more than this, we'll go below the minimum
	// Note that this can go negative (if old + new don't satisfy minHealthy).
	headroom := old + new - minHealthy

	capacityIncrease := targetDesired - currentDesired
	if capacityIncrease > 0 {
		// If we intend to schedule new nodes (nodes not currently managed by either RC),
		// then we increase headroom by the capacity difference.
		// Note that headroom could previously have been negative.
		// This means we will schedule some number between 0 and capacityIncrease.
		// For example, with old = new = 0, minHealthy = 2, capacityIncrease = 3,
		// we'll schedule 0 - 2 + 3 = 1 node.
		// This conservatively respects minimum health,
		// on the assumption that there may be healthy nodes we don't know about.
		// This is particularly useful when migrating from other deployment systems.
		headroom += capacityIncrease
	} else {
		// We're not yet sure what the effects of a capacity decrease in a rolling update will be.
		capacityIncrease = 0
	}

	// how many nodes do we have left to go? we can't schedule more than this
	// or we'll go over the target
	// note that remaining < headroom is possible depending on how many
	// old nodes are still alive
	remaining := targetDesired - new

	// heuristic time:
	if remaining <= 0 {
		// no nodes remaining, noop out
		return 0, 0
	}
	if new >= minHealthy {
		// minimum is satisfied by new nodes, doesn't matter how many old ones
		// we kill. this includes the edge case where minHealthy==0
		return clampToZero(remaining - capacityIncrease), remaining
	}
	if headroom <= 0 {
		// the case of minHealthy==0 was caught above. in this case, minHealthy>0 and
		// headroom is non-positive. this means that we are at, or below, the
		// minimum, and we cannot schedule new nodes because we might take
		// down an old one (which would go below the minimum)
		return 0, 0
	}
	// remaining>0 and headroom>0. each one imposes an upper bound on how
	// many nodes we can add at once, so nodesToAdd is their minimum.
	// nodesToRemove is the same number minus any capacity increase
	// (so the net number of nodes added equals the capacity increase)
	if remaining < headroom {
		nodesToAdd = remaining
	} else {
		nodesToAdd = headroom
	}
	nodesToRemove = clampToZero(nodesToAdd - capacityIncrease)
	return
}

func clampToZero(n int) int {
	if n < 0 {
		return 0
	}
	return n
}
