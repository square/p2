package replication

import (
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
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

// A replication contains the information required to do a single replication (deploy).
type replication struct {
	active    int
	nodes     []types.NodeName
	store     kp.Store
	labeler   labels.Applicator
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
}

// Attempts to claim a lock on replicating this pod. Other pkg/replication
// operations for this pod ID will not be able to take place.
// if overrideLock is true, will destroy any session holding any of the keys we
// wish to lock
func (r *replication) lockHosts(overrideLock bool, lockMessage string) error {
	session, renewalErrCh, err := r.store.NewSession(lockMessage, nil)
	if err != nil {
		return err
	}

	lockPath := kp.ReplicationLockPath(r.manifest.ID())

	// We don't keep a reference to the consulutil.Unlocker, because we just destroy
	// the session at the end of the replication anyway
	_, err = r.lock(session, lockPath, overrideLock)
	if err != nil {
		_ = session.Destroy()
		return err
	}

	go r.handleRenewalErrors(session, renewalErrCh)

	return nil
}

// Attempts to claim a lock. If the overrideLock is set, any existing lock holder
// will be destroyed and one more attempt will be made to acquire the lock
func (r *replication) lock(session kp.Session, lockPath string, overrideLock bool) (consulutil.Unlocker, error) {
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
	done := make(chan nodeUpdated)

	aggregateHealth := AggregateHealth(r.manifest.ID(), r.health)
	// this loop multiplexes the node queue across some goroutines
	for i := 0; i < r.active; i++ {
		go func() {
			for node := range nodeQueue {
				exitCh := make(chan struct{})
				timeoutCh := make(chan struct{})

				if r.rateLimiter != nil {
					// Wait until we can read off the rate limit channel before
					// updating the node
					select {
					case <-r.rateLimiter.C:
					case <-r.quitCh:
						return
					}
				}
				go func() {
					defer close(exitCh)
					err = r.updateOne(node, done, timeoutCh, aggregateHealth)
					done <- nodeUpdated{
						node: node,
						err:  err,
					}
				}()

				if r.timeout == NoTimeout {
					select {
					case <-exitCh:
					case <-r.quitCh:
						return
					}
				} else {
					// Wait until either completion, timeout, or a quit
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

	// this goroutine populates the node queue
	go func() {
		defer close(nodeQueue)
		for _, node := range r.nodes {
			select {
			case nodeQueue <- node:
				// a worker will consume it
			case <-r.quitCh:
				return
			}
		}
	}()

	// the main blocking loop processes replies from workers
	defer r.logger.Info("Replication is over")
	for doneIndex := 0; doneIndex < len(r.nodes); {
		select {
		case status := <-done:
			doneIndex++
			if status.err == errTimeout {
				r.timedOutReplicationsMutex.Lock()
				r.timedOutReplications = append(r.timedOutReplications, status.node)
				r.timedOutReplicationsMutex.Unlock()
				r.logger.Errorf("The host '%v' timed out during replication for pod '%v'", status.node, r.manifest.ID())

			} else if status.err == errCancelled {
				r.logger.Errorf("The host '%v' was cancelled (probably due to an update) during replication for pod '%v'", status.node, r.manifest.ID())
			} else {
				r.logger.Errorf("An unexpected error has occurred: %v", status.err)
			}
			r.logger.Infof("The host '%v' successfully replicated the pod '%v'", status.node, r.manifest.ID())
			r.logger.Infof("%v nodes left", len(r.nodes)-doneIndex)
		case <-r.quitCh:
			return
		}
	}
}

// Cancels all goroutines (e.g. replication and lock renewal)
func (r *replication) Cancel() {
	close(r.replicationCancelledCh)
}

func (r *replication) WaitForReplication() {
	<-r.quitCh
}

// Listen for errors in lock renewal. If the lock can't be renewed, we need to
// 1) stop replication and 2) communicate the error up a level
// If replication finishes, destroy the lock
func (r *replication) handleRenewalErrors(session kp.Session, renewalErrCh chan error) {
	defer func() {
		close(r.quitCh)
		close(r.errCh)
		if r.rateLimiter != nil {
			r.rateLimiter.Stop()
		}
		_ = session.Destroy()
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

// note: logging should be delegated somehow
func (r *replication) updateOne(
	node types.NodeName,
	done chan<- nodeUpdated,
	timeoutCh <-chan struct{},
	aggregateHealth *podHealth,
) error {
	targetSHA, _ := r.manifest.SHA()
	nodeLogger := r.logger.SubLogger(logrus.Fields{"node": node})
	nodeLogger.WithField("sha", targetSHA).Infoln("Updating node")

	nodeReality, err := r.queryReality(node)
	if err != nil || nodeReality == nil {
		nodeLogger.WithError(err).Errorln("Could not read Reality for this node. Will proceed to schedule onto it.")
	}

	if nodeReality != nil {
		nodeRealitySHA, err := nodeReality.SHA()
		if err != nil {
			nodeLogger.WithError(err).Errorln("Unable to compute manifest SHA for this node. Attempting to schedule anyway")
		}
		replicationRealitySHA, err := r.manifest.SHA()
		if err != nil {
			nodeLogger.WithError(err).Errorln("Unable to compute manifest SHA for this daemon set. Attempting to schedule anyway")
		}

		if nodeRealitySHA == replicationRealitySHA {
			nodeLogger.Info("Reality for this node matches this DS. No action required.")
			return nil
		}
	}

	_, err = r.store.SetPod(
		kp.INTENT_TREE,
		node,
		r.manifest,
	)

	exponentialBackoff := time.Duration(1 * time.Second)
	timer := time.NewTimer(exponentialBackoff)
	for err != nil {
		nodeLogger.WithError(err).Errorln("Could not write intent store")
		r.errCh <- err

		select {
		case <-r.quitCh:
			return errQuit
		case <-timeoutCh:
			return errTimeout
		case <-r.replicationCancelledCh:
			return errCancelled
		case <-timer.C:
			_, err = r.store.SetPod(
				kp.INTENT_TREE,
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
	r.concurrentRealityRequests <- struct{}{}
	defer func() {
		<-r.concurrentRealityRequests
	}()
	man, _, err := r.store.Pod(kp.REALITY_TREE, node, r.manifest.ID())
	return man, err
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
			return errQuit
		case <-timeoutCh:
			return errTimeout
		case <-r.replicationCancelledCh:
			return errCancelled
		case <-time.After(5 * time.Second):
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
			return errQuit
		case <-timeoutCh:
			return errTimeout
		case <-r.replicationCancelledCh:
			return errCancelled
		case <-time.After(1 * time.Second):
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
