package replication

import (
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/util"
)

type Replicator interface {
	InitializeReplication(overrideLock bool) (Replication, chan error, error)
}

// Replicator creates replications
type replicator struct {
	manifest  pods.Manifest // the manifest to replicate
	logger    logging.Logger
	nodes     []string
	active    int // maximum number of nodes to update concurrently
	store     kp.Store
	health    checker.ConsulHealthChecker
	threshold health.HealthState // minimum state to treat as "healthy"

	lockMessage string
}

func NewReplicator(
	manifest pods.Manifest,
	logger logging.Logger,
	nodes []string,
	active int,
	store kp.Store,
	health checker.ConsulHealthChecker,
	threshold health.HealthState,
	lockMessage string,
) (Replicator, error) {
	if active < 1 {
		return replicator{}, util.Errorf("Active must be >= 1, was %d", active)
	}
	return replicator{
		manifest:    manifest,
		logger:      logger,
		nodes:       nodes,
		active:      active,
		store:       store,
		health:      health,
		threshold:   threshold,
		lockMessage: lockMessage,
	}, nil
}

// Initializes a replication after performing some initial validation.
// Validation errors are returned immediately, and asynchronous errors are
// passed on the returned channel
func (r replicator) InitializeReplication(overrideLock bool) (Replication, chan error, error) {
	err := r.checkPreparers()
	if err != nil {
		return nil, nil, err
	}

	errCh := make(chan error)
	replication := &replication{
		active:    r.active,
		nodes:     r.nodes,
		store:     r.store,
		manifest:  r.manifest,
		health:    r.health,
		threshold: r.threshold,
		logger:    r.logger,
		errCh:     errCh,
		replicationCancelledCh: make(chan struct{}),
		replicationDoneCh:      make(chan struct{}),
		quitCh:                 make(chan struct{}),
	}

	err = replication.lockHosts(overrideLock, r.lockMessage)
	if err != nil {
		return nil, errCh, err
	}
	return replication, errCh, nil
}

// Checks that the preparer is running on every host being deployed to.
func (r replicator) checkPreparers() error {
	for _, host := range r.nodes {
		_, _, err := r.store.Pod(kp.RealityPath(host, string(preparer.POD_ID)))
		if err != nil {
			return util.Errorf("Could not verify %v state on %q: %v", preparer.POD_ID, host, err)
		}
	}
	return nil
}
