package replication

import (
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/param"

	"github.com/Sirupsen/logrus"
)

type Labeler interface {
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
}

var (
	ensureRealityPeriodMillis = param.Int("ensure_in_reality_millis", 5000)
	ensureHealthyPeriodMillis = param.Int("ensure_healthy_millis", 1000)
)

type nodeUpdated struct {
	node types.NodeName
	err  error
}

type replicationError struct {
	err error
	// Indicates if the error halted replication or if it is recoverable
	isFatal bool
}

// Assert that replicationError implements the error interface
var _ error = replicationError{}

func (r replicationError) Error() string {
	return r.err.Error()
}

func IsFatalError(err error) bool {
	if replErr, ok := err.(replicationError); ok {
		return replErr.isFatal
	}
	return false
}

type Replication interface {
	// Proceed with the prescribed replication
	Enact()

	// Cancel the prescribed replication
	Cancel()

	// Will block until the r.quitCh is closed
	// this is used to synchronize updates which quickly cancel and re-enact the replicaton
	WaitForReplication()
}

type Store interface {
	SetPod(podPrefix consul.PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
	Pod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (manifest.Manifest, time.Duration, error)
	NewSession(name string, renewalCh <-chan time.Time) (consul.Session, chan error, error)
	LockHolder(key string) (string, string, error)
	DestroyLockHolder(id string) error
}

// A replication contains the information required to do a single replication (deploy).
type replication struct {
	active    int
	nodes     []types.NodeName
	store     Store
	labeler   Labeler
	manifest  manifest.Manifest
	health    checker.ConsulHealthChecker
	threshold health.HealthState // minimum state to treat as "healthy"
	logger    logging.Logger

	// Used to rate limit node updates. A node will not be updated
	// until a value can be read off of the channel.
	rateLimiter *time.Ticker

	// communicates errors back to the caller, such as an error renewing
	// the deploy lock
	errCh chan<- error
	// signals replication cancellation by the caller
	replicationCancelledCh chan struct{}
	// signals any supplementary goroutines to exit once the
	// replication has completed successfully
	replicationDoneCh chan struct{}
	// The enacted channel will allow us to know when the replication is ongoing
	// This is originally closed during initialization
	enactedCh chan struct{}
	// Used to cancel replication due to a lock renewal failure or a
	// cancellation by the caller
	quitCh chan struct{}
	// Semaphore that sets a maximum value on the number of concurrent
	// reality requests that can be fired simultaneously.
	concurrentRealityRequests chan struct{}

	// Used to timeout daemon set replications
	timeout time.Duration

	// Used to log replications that have timed out
	timedOutReplications      []types.NodeName
	timedOutReplicationsMutex sync.Mutex

	// Used to tune the reactiveness of the replication to health changes
	// to trade off with QPS and bandwidth. 1 second is the lower bound for
	// this value.
	healthWatchDelay time.Duration
}

// Attempts to claim a lock on replicating this pod. Other pkg/replication
// operations for this pod ID will not be able to take place.
// if overrideLock is true, will destroy any session holding any of the keys we
// wish to lock
func (r *replication) lockHosts(overrideLock bool, lockMessage string) (consul.Session, chan error, error) {
	session, renewalErrCh, err := r.store.NewSession(lockMessage, nil)
	if err != nil {
		return nil, nil, err
	}

	lockPath := consul.ReplicationLockPath(r.manifest.ID())

	// We don't keep a reference to the consulutil.Unlocker, because we just destroy
	// the session at the end of the replication anyway
	_, err = r.lock(session, lockPath, overrideLock)
	if err != nil {
		_ = session.Destroy()
		return nil, nil, err
	}

	return session, renewalErrCh, nil
}

// Attempts to claim a lock. If the overrideLock is set, any existing lock holder
// will be destroyed and one more attempt will be made to acquire the lock
func (r *replication) lock(session consul.Session, lockPath string, overrideLock bool) (consulutil.Unlocker, error) {
	unlocker, err := session.Lock(lockPath)

	if _, ok := err.(consulutil.AlreadyLockedError); ok {
		holder, id, err := r.store.LockHolder(lockPath)
		if err != nil {
			return nil, util.Errorf("Lock already held for %q, could not determine holder due to error: %s", lockPath, err)
		} else if holder == "" {
			// we failed to acquire this lock, but there is no outstanding
			// holder
			// this indicates that the previous holder had a LockDelay,
			// which prevents other parties from acquiring the lock for a
			// limited time
			return nil, util.Errorf("Lock for %q is blocked due to delay by previous holder", lockPath)
		} else if overrideLock {
			err = r.store.DestroyLockHolder(id)
			if err != nil {
				return nil, util.Errorf("Unable to destroy the current lock holder (%s) for %q: %s", holder, lockPath, err)
			}

			// try acquiring the lock again, but this time don't destroy holders so we don't try forever
			return r.lock(session, lockPath, false)

		} else {
			return nil, util.Errorf("Lock for %q already held by lock %q", lockPath, holder)
		}
	}

	return unlocker, err
}

// checkForManaged() checks whether there are any existing pods that this replication
// would modify that are already managed by a controller. If there is such a pod, the
// change should go through its controller, not here.
func (r *replication) checkForManaged() error {
	var badNodes []string
	for _, node := range r.nodes {
		podID := path.Join(node.String(), string(r.manifest.ID()))
		labels, err := r.labeler.GetLabels(labels.POD, podID)
		if err != nil {
			return err
		}
		if labels.Labels.Has(rc.RCIDLabel) {
			badNodes = append(badNodes, node.String())
		}
	}
	if len(badNodes) > 0 {
		return util.Errorf(
			"cannot replicate to nodes already manged by a controller: %s",
			strings.Join(badNodes, ", "),
		)
	}
	return nil
}

// Execute the replication.
// note: error management could use some improvement, errors coming out of
// updateOne need to be scoped to the node that they came from
func (r *replication) Enact() {
	defer close(r.replicationDoneCh)

	r.enactedCh = make(chan struct{})
	defer close(r.enactedCh)

	// Sort nodes from least healthy to most healthy to maximize overall
	// cluster health
	healthResults, err := r.health.Service(string(r.manifest.ID()))
	if err != nil {
		err = replicationError{
			err:     err,
			isFatal: true,
		}
		select {
		case r.errCh <- err:
		case <-r.quitCh:
		}
		return
	}

	// Sort nodes by health from worst to best to maximize overall
	// cluster health
	order := health.SortOrder{
		Nodes:  r.nodes,
		Health: healthResults,
	}
	sort.Sort(order)

	nodeQueue := make(chan types.NodeName)

	aggregateHealth := AggregateHealth(r.manifest.ID(), r.health, r.healthWatchDelay)
	// this loop multiplexes the node queue across some goroutines

	var updatePool sync.WaitGroup
	var completedCount int32
	for i := 0; i < r.active; i++ {
		updatePool.Add(1)
		go func() {
			// nodeQueue is managed below to throttle these goroutines
			defer updatePool.Done()
			for node := range nodeQueue {
				exitCh := make(chan struct{})
				timeoutCh := make(chan struct{})

				go func() {
					defer close(exitCh)
					err = r.updateOne(node, timeoutCh, aggregateHealth)
					if err == nil {
						r.logger.Infof("The host '%v' successfully replicated the pod '%v'", node, r.manifest.ID())
						atomic.AddInt32(&completedCount, 1)
						r.logger.Infof("Completed %d of %d", atomic.LoadInt32(&completedCount), len(r.nodes))
						return
					}

					switch err {
					case errTimeout:
						r.timedOutReplicationsMutex.Lock()
						r.timedOutReplications = append(r.timedOutReplications, node)
						r.timedOutReplicationsMutex.Unlock()
						r.logger.Errorf("The host '%v' timed out during replication for pod '%v'", node, r.manifest.ID())
					case errCancelled:
						r.logger.Errorf("The host '%v' was cancelled (probably due to an update) during replication for pod '%v'", node, r.manifest.ID())
					default:
						r.logger.Errorf("An unexpected error has occurred: %v", err)
					}
				}()

				if r.timeout == NoTimeout {
					select {
					case <-exitCh:
					case <-r.quitCh:
						return
					}
				} else {
					select {
					case <-exitCh:
					case <-time.After(r.timeout):
						close(timeoutCh)
					case <-r.quitCh:
						return
					}
				}

			}
		}()
	}

	// this goroutine populates the node queue with respect to the rate limiter
	go func() {
		defer close(nodeQueue)
		for _, node := range r.nodes {
			if r.rateLimiter != nil {
				select {
				case <-r.replicationCancelledCh:
					return
				case <-r.quitCh:
					return
				case <-r.rateLimiter.C:
				}
			}
			select {
			case <-r.replicationCancelledCh:
				return
			case <-r.quitCh:
				return
			case nodeQueue <- node:
			}
		}
	}()

	updatePool.Wait()
	r.logger.Info("Replication is over")
}

// Cancels all goroutines (e.g. replication and lock renewal)
func (r *replication) Cancel() {
	close(r.replicationCancelledCh)
}

func (r *replication) WaitForReplication() {
	<-r.quitCh
}

// handleReplicationEnd listens for various events that can cause the replication to end.
//
// These are:
// * The replication signals that it has finished executing.
// * The replication is canceled.
// * Errors in lock renewal (as signaled on the passed channel).
//   In this case, the error needs to be communicated up a level.
//   The passed channel may be nil, in which case this case is not checked,
//   but the others still are.
//
// When replication finishes for any of these reasons, this function is responsible for:
// * Stopping the replication (if it has not already)
// * Destroying its session (passed in to this function.
//   Passing nil is legal, in which case it is not destroyed)
func (r *replication) handleReplicationEnd(session consul.Session, renewalErrCh chan error) {
	defer func() {
		close(r.quitCh)
		close(r.errCh)
		if r.rateLimiter != nil {
			r.rateLimiter.Stop()
		}
		if session != nil {
			_ = session.Destroy()
		}
	}()

	select {
	case <-r.replicationDoneCh:
	case <-r.replicationCancelledCh:
		// If the replication is enacted, wait for it to exit
		<-r.enactedCh
	case err := <-renewalErrCh:
		// communicate the error to the caller.
		r.errCh <- replicationError{
			err:     err,
			isFatal: true,
		}
		return
	}
}

func (r *replication) shouldScheduleForNode(node types.NodeName, logger logging.Logger) bool {
	nodeReality, err := r.queryReality(node)
	if err != nil {
		logger.WithError(err).Errorln("Could not read Reality for this node. Will proceed to schedule onto it.")
		return true
	}
	if err == pods.NoCurrentManifest {
		logger.Infoln("Nothing installed on this node yet.")
		return true
	}

	if nodeReality != nil {
		nodeRealitySHA, err := nodeReality.SHA()
		if err != nil {
			logger.WithError(err).Errorln("Unable to compute manifest SHA for this node. Attempting to schedule anyway")
			return true
		}
		replicationRealitySHA, err := r.manifest.SHA()
		if err != nil {
			logger.WithError(err).Errorln("Unable to compute manifest SHA for this daemon set. Attempting to schedule anyway")
			return true
		}

		if nodeRealitySHA == replicationRealitySHA {
			logger.Info("Reality for this node matches this DS. No action required.")
			return false
		}
	}

	return true
}

func (r *replication) updateOne(
	node types.NodeName,
	timeoutCh <-chan struct{},
	aggregateHealth *podHealth,
) error {
	nodeLogger := r.logger.SubLogger(logrus.Fields{"node": node})

	if !r.shouldScheduleForNode(node, nodeLogger) {
		return nil
	}

	targetSHA, _ := r.manifest.SHA()
	nodeLogger.WithField("sha", targetSHA).Infoln("Updating node")
	_, err := r.store.SetPod(
		consul.INTENT_TREE,
		node,
		r.manifest,
	)

	exponentialBackoff := time.Duration(1 * time.Second)
	timer := time.NewTimer(exponentialBackoff)
	for err != nil {
		nodeLogger.WithError(err).Errorln("Could not write intent store")

		select {
		case r.errCh <- err:
		case <-r.quitCh:
			r.logger.Infoln("Caught quit signal during updateOne")
			return errQuit
		case <-timeoutCh:
			r.logger.Infoln("Caught timeout signal during updateOne")
			return errTimeout
		case <-r.replicationCancelledCh:
			r.logger.Infoln("Caught cancellation signal during updateOne")
			return errCancelled
		case <-timer.C:
			_, err = r.store.SetPod(
				consul.INTENT_TREE,
				node,
				r.manifest,
			)
			exponentialBackoff = exponentialBackoff * 2
			timer.Reset(exponentialBackoff)
		}
	}
	err = r.ensureInReality(node, timeoutCh, nodeLogger, targetSHA)
	if err != nil {
		return err
	}
	return r.ensureHealthy(node, timeoutCh, nodeLogger, aggregateHealth)
}

func (r *replication) queryReality(node types.NodeName) (manifest.Manifest, error) {
	for {
		select {
		case r.concurrentRealityRequests <- struct{}{}:
			man, _, err := r.store.Pod(consul.REALITY_TREE, node, r.manifest.ID())
			<-r.concurrentRealityRequests
			return man, err
		case <-time.After(5 * time.Second):
			r.logger.Infof("Waiting on concurrentRealityRequests for pod: %s/%s", node.String(), r.manifest.ID())
		case <-time.After(1 * time.Minute):
			err := util.Errorf("Timed out while waiting for reality query rate limit")
			r.logger.Error(err)
			return nil, err
		}
	}
}

func (r *replication) ensureInReality(
	node types.NodeName,
	timeoutCh <-chan struct{},
	nodeLogger logging.Logger,
	targetSHA string,
) error {
	for {
		select {
		case <-r.quitCh:
			r.logger.Infoln("Caught quit signal during ensureInReality")
			return errQuit
		case <-timeoutCh:
			r.logger.Infoln("Caught timeout signal during ensureInReality")
			return errTimeout
		case <-r.replicationCancelledCh:
			r.logger.Infoln("Caught cancellation signal during ensureInReality")
			return errCancelled
		case <-time.After(time.Duration(*ensureRealityPeriodMillis) * time.Millisecond):
			man, err := r.queryReality(node)
			if err == pods.NoCurrentManifest {
				// if the pod key doesn't exist yet, that's okay just wait longer
			} else if err != nil {
				nodeLogger.WithErrorAndFields(err, logrus.Fields{
					"node": node,
				}).Errorln("Could not read reality for pod manifest")
			} else {
				receivedSHA, _ := man.SHA()
				if receivedSHA == targetSHA {
					nodeLogger.NoFields().Infoln("Node is current")
					return nil
				} else {
					nodeLogger.WithFields(logrus.Fields{"current": receivedSHA, "target": targetSHA}).Infoln("Waiting for current")
				}
			}
		}
	}
}

func (r *replication) ensureHealthy(
	node types.NodeName,
	timeoutCh <-chan struct{},
	nodeLogger logging.Logger,
	aggregateHealth *podHealth,
) error {
	for {
		select {
		case <-r.quitCh:
			r.logger.Infoln("Caught quit signal during ensureHealthy")
			return errQuit
		case <-timeoutCh:
			r.logger.Infoln("Caught node timeout signal during ensureHealthy")
			return errTimeout
		case <-r.replicationCancelledCh:
			r.logger.Infoln("Caught cancellation signal during ensureHealthy")
			return errCancelled
		case <-time.After(time.Duration(*ensureHealthyPeriodMillis) * time.Millisecond):
			res, ok := aggregateHealth.GetHealth(node)
			if !ok {
				nodeLogger.WithFields(logrus.Fields{
					"node": node,
				}).Errorln("Could not get health, retrying")
				// Zero res should be treated like "critical"
			}
			id := res.ID
			status := res.Status
			// treat an empty threshold as "passing"
			threshold := health.Passing
			if r.threshold != "" {
				threshold = r.threshold
			}
			// is this status less than the threshold?
			if health.Compare(status, threshold) < 0 {
				nodeLogger.WithFields(logrus.Fields{"check": id, "health": status}).Infoln("Node is not healthy")
			} else {
				r.logger.WithField("node", node).Infoln("Node is current and healthy")
				return nil
			}
		}
	}
}
