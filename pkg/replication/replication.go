package replication

import (
	"path"
	"sort"
	"strings"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
)

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
}

// A replication contains the information required to do a single replication (deploy).
type replication struct {
	active    int
	nodes     []string
	store     kp.Store
	labeler   labels.Applicator
	manifest  pods.Manifest
	health    checker.ConsulHealthChecker
	threshold health.HealthState // minimum state to treat as "healthy"
	logger    logging.Logger

	// communicates errors back to the caller, such as an error renewing
	// the deploy lock
	errCh chan<- error
	// signals replication cancellation by the caller
	replicationCancelledCh chan struct{}
	// signals any supplementary goroutines to exit once the
	// replication has completed successfully
	replicationDoneCh chan struct{}
	// Used to cancel replication due to a lock renewal failure or a
	// cancellation by the caller
	quitCh chan struct{}
}

// Attempts to claim a lock on replicating this pod. Other pkg/replication
// operations for this pod ID will not be able to take place.
// if overrideLock is true, will destroy any session holding any of the keys we
// wish to lock
func (r replication) lockHosts(overrideLock bool, lockMessage string) error {
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
func (r replication) lock(session kp.Session, lockPath string, overrideLock bool) (consulutil.Unlocker, error) {
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
func (r replication) checkForManaged() error {
	var badNodes []string
	for _, node := range r.nodes {
		podID := path.Join(node, string(r.manifest.ID()))
		labels, err := r.labeler.GetLabels(labels.POD, podID)
		if err != nil {
			return err
		}
		if labels.Labels.Has(rc.RCIDLabel) {
			badNodes = append(badNodes, node)
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
func (r replication) Enact() {
	defer close(r.replicationDoneCh)

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

	nodeQueue := make(chan string)
	done := make(chan string)
	innerQuit := make(chan struct{})

	// all child goroutines will be terminated on return
	defer close(innerQuit)

	// this loop multiplexes the node queue across some goroutines
	for i := 0; i < r.active; i++ {
		go func() {
			for node := range nodeQueue {
				r.updateOne(node, done, innerQuit)
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
	for doneIndex := 0; doneIndex < len(r.nodes); {
		select {
		case <-done:
			doneIndex++
		case <-r.quitCh:
			return
		}
	}
}

// Cancels all goroutines (e.g. replication and lock renewal)
func (r replication) Cancel() {
	close(r.replicationCancelledCh)
}

// Listen for errors in lock renewal. If the lock can't be renewed, we need to
// 1) stop replication and 2) communicate the error up a level
// If replication finishes, destroy the lock
func (r replication) handleRenewalErrors(session kp.Session, renewalErrCh chan error) {
	defer func() {
		close(r.quitCh)
		close(r.errCh)
		_ = session.Destroy()
	}()

	select {
	case <-r.replicationDoneCh:
	case <-r.replicationCancelledCh:
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
func (r replication) updateOne(node string, done chan<- string, quitCh <-chan struct{}) {
	targetSHA, _ := r.manifest.SHA()
	nodeLogger := r.logger.SubLogger(logrus.Fields{"node": node})
	nodeLogger.WithField("sha", targetSHA).Infoln("Updating node")

	_, err := r.store.SetPod(
		kp.INTENT_TREE,
		node,
		r.manifest,
	)
	for err != nil {
		nodeLogger.WithError(err).Errorln("Could not write intent store")
		r.errCh <- err
		time.Sleep(1 * time.Second)
		_, err = r.store.SetPod(
			kp.INTENT_TREE,
			node,
			r.manifest,
		)
	}

	realityResults := make(chan kp.ManifestResult)
	realityErr := make(chan error)
	realityQuit := make(chan struct{})
	defer close(realityQuit)
	go r.store.WatchPod(
		kp.REALITY_TREE,
		node,
		r.manifest.ID(),
		realityQuit,
		realityErr,
		realityResults,
	)
REALITY_LOOP:
	for {
		select {
		case <-quitCh:
			return
		case err := <-realityErr:
			nodeLogger.WithError(err).Errorln("Could not read reality store")
			select {
			case r.errCh <- err:
			case <-quitCh:
			}
		case mResult := <-realityResults:
			// if the pod key doesn't exist yet, that's okay just wait longer
			if mResult.Manifest != nil {
				receivedSHA, _ := mResult.Manifest.SHA()
				if receivedSHA == targetSHA {
					break REALITY_LOOP
				} else {
					nodeLogger.WithFields(logrus.Fields{"current": receivedSHA, "target": targetSHA}).Infoln("Waiting for current")
				}
			}
		}
	}
	nodeLogger.NoFields().Infoln("Node is current")

	healthResults := make(chan health.Result)
	healthErr := make(chan error)
	healthQuit := make(chan struct{})
	defer close(healthQuit)
	go r.health.WatchNodeService(
		node,
		string(r.manifest.ID()),
		healthResults,
		healthErr,
		healthQuit,
	)
HEALTH_LOOP:
	for {
		select {
		case <-quitCh:
			return
		case err := <-healthErr:
			nodeLogger.WithError(err).Errorln("Could not read health check")
			select {
			case r.errCh <- err:
			case <-quitCh:
			}
		case res := <-healthResults:
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
				break HEALTH_LOOP
			}
		}
	}
	r.logger.WithField("node", node).Infoln("Node is current and healthy")

	select {
	case done <- node:
	case <-quitCh:
	}
}
