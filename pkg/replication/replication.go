package replication

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/util"
)

type Replicator struct {
	Manifest  pods.Manifest // the manifest to replicate
	Logger    logging.Logger
	Nodes     []string
	Active    int // maximum number of nodes to update concurrently
	Store     kp.Store
	Health    health.ConsulHealthChecker
	Threshold health.HealthState // minimum state to treat as "healthy"
}

// Checks that the preparer is running on every host being deployed to.
func (r Replicator) CheckPreparers() error {
	for _, host := range r.Nodes {
		_, _, err := r.Store.Pod(kp.RealityPath(host, preparer.POD_ID))
		if err != nil {
			return util.Errorf("Host %q does not have a preparer", host)
		}
	}
	return nil
}

// Attempts to claim a lock on every host being deployed to.
// if overrideLock is true, will destroy any session holding any of the keys we
// wish to lock
func (r Replicator) LockHosts(lock kp.Lock, overrideLock bool) error {
	for _, host := range r.Nodes {
		lockPath := kp.LockPath(host, r.Manifest.ID())
		err := r.lock(lock, lockPath, overrideLock)

		if err != nil {
			return err
		}
	}
	return nil
}

// Attempts to claim a lock. If the overrideLock is set, any existing lock holder
// will be destroyed and one more attempt will be made to acquire the lock
func (r Replicator) lock(lock kp.Lock, lockPath string, overrideLock bool) error {
	err := lock.Lock(lockPath)

	if _, ok := err.(kp.AlreadyLockedError); ok {
		holder, id, err := r.Store.LockHolder(lockPath)
		if err != nil {
			return util.Errorf("Lock already held for %q, could not determine holder due to error: %s", lockPath, err)
		} else if holder == "" {
			// we failed to acquire this lock, but there is no outstanding
			// holder
			// this indicates that the previous holder had a LockDelay,
			// which prevents other parties from acquiring the lock for a
			// limited time
			return util.Errorf("Lock for %q is blocked due to delay by previous holder", lockPath)
		} else if overrideLock {
			err = r.Store.DestroyLockHolder(id)
			if err != nil {
				return util.Errorf("Unable to destroy the current lock holder (%s) for %q: %s", holder, lockPath, err)
			}

			// try acquiring the lock again, but this time don't destroy holders so we don't try forever
			return r.lock(lock, lockPath, false)

		} else {
			return util.Errorf("Lock for %q already held by lock %q", lockPath, holder)
		}
	}

	return err
}

// Execute the replication.
// note: error management could use some improvement, errors coming out of
// updateOne need to be scoped to the node that they came from
func (r Replicator) Enact(errCh chan<- error, quitCh <-chan struct{}) {
	nodeQueue := make(chan string)
	done := make(chan string)
	innerQuit := make(chan struct{})

	// all child goroutines will be terminated on return
	defer close(innerQuit)

	// this loop multiplexes the node queue across some goroutines
	for i := 0; i < r.Active; i++ {
		go func() {
			for node := range nodeQueue {
				r.updateOne(node, done, errCh, innerQuit)
			}
		}()
	}

	// this goroutine populates the node queue
	go func() {
		defer close(nodeQueue)
		for _, node := range r.Nodes {
			select {
			case nodeQueue <- node:
				// a worker will consume it
			case <-innerQuit:
				return
			}
		}
	}()

	// the main blocking loop processes replies from workers
	for doneIndex := 0; doneIndex < len(r.Nodes); {
		select {
		case <-done:
			doneIndex++
		case <-quitCh:
			return
		}
	}
}

// note: logging should be delegated somehow
func (r Replicator) updateOne(node string, done chan<- string, errCh chan<- error, quitCh <-chan struct{}) {
	targetSHA, _ := r.Manifest.SHA()
	nodeLogger := r.Logger.SubLogger(logrus.Fields{"node": node})
	nodeLogger.WithField("sha", targetSHA).Infoln("Updating node")

	_, err := r.Store.SetPod(kp.IntentPath(node, r.Manifest.ID()), r.Manifest)
	for err != nil {
		nodeLogger.WithField("err", err).Errorln("Could not write intent store")
		errCh <- err
		time.Sleep(1 * time.Second)
		_, err = r.Store.SetPod(kp.IntentPath(node, r.Manifest.ID()), r.Manifest)
	}

	realityResults := make(chan kp.ManifestResult)
	realityErr := make(chan error)
	realityQuit := make(chan struct{})
	defer close(realityQuit)
	go r.Store.WatchPods(kp.RealityPath(node, r.Manifest.ID()), realityQuit, realityErr, realityResults)
REALITY_LOOP:
	for {
		select {
		case <-quitCh:
			return
		case err := <-realityErr:
			nodeLogger.WithField("err", err).Errorln("Could not read reality store")
			errCh <- err
		case mResult := <-realityResults:
			receivedSHA, _ := mResult.Manifest.SHA()
			if receivedSHA == targetSHA {
				break REALITY_LOOP
			} else {
				nodeLogger.WithFields(logrus.Fields{"current": receivedSHA, "target": targetSHA}).Infoln("Waiting for current")
			}
		}
	}
	nodeLogger.NoFields().Infoln("Node is current")

	healthResults := make(chan [][]health.Result)
	healthErr := make(chan error)
	healthQuit := make(chan struct{})
	defer close(healthQuit)
	go r.Health.WatchNodeService(node, r.Manifest.ID(), healthResults, healthErr, healthQuit)
HEALTH_LOOP:
	for {
		select {
		case <-quitCh:
			return
		case err := <-healthErr:
			nodeLogger.WithField("err", err).Errorln("Could not read health check")
			errCh <- err
		case res := <-healthResults:
			id, status := health.GetMultisourceResult(res)
			// treat an empty threshold as "passing"
			threshold := health.Passing
			if r.Threshold != "" {
				threshold = r.Threshold
			}
			// is this status less than the threshold?
			if health.Compare(status, threshold) < 0 {
				nodeLogger.WithFields(logrus.Fields{"check": id, "health": status}).Infoln("Node is not healthy")
			} else {
				break HEALTH_LOOP
			}
		}
	}
	r.Logger.WithField("node", node).Infoln("Node is current and healthy")

	select {
	case done <- node:
	case <-quitCh:
	}
}
