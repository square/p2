package replication

import (
	"errors"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

const (
	DefaultConcurrentReality = 3

	// Normal replications will have no timeout, but daemon sets will
	// because it is unlikely that all hosts are healthy at all times
	NoTimeout = time.Duration(-1)
)

var (
	errTimeout   = errors.New("Update timed out")
	errCancelled = errors.New("Replication cancelled")
	errQuit      = errors.New("Replication quit")
)

type Replicator interface {
	InitializeReplication(
		overrideLock bool,
		ignoreControllers bool,
		concurrentRealityNodes int,
	) (Replication, chan error, error)

	// Same as InitializeReplication but can skip the checkPreparers
	InitializeReplicationWithCheck(
		overrideLock bool,
		ignoreControllers bool,
		concurrentRealityRequests int,
		checkPreparers bool,
	) (Replication, chan error, error)
}

// Replicator creates replications
type replicator struct {
	manifest  manifest.Manifest // the manifest to replicate
	logger    logging.Logger
	nodes     []types.NodeName
	active    int // maximum number of nodes to update concurrently
	store     kp.Store
	labeler   labels.Applicator
	health    checker.ConsulHealthChecker
	threshold health.HealthState // minimum state to treat as "healthy"

	lockMessage string

	// Used to timeout daemon set replications
	timeout time.Duration
}

func NewReplicator(
	manifest manifest.Manifest,
	logger logging.Logger,
	nodes []types.NodeName,
	active int,
	store kp.Store,
	labeler labels.Applicator,
	health checker.ConsulHealthChecker,
	threshold health.HealthState,
	lockMessage string,
	timeout time.Duration,
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
		labeler:     labeler,
		health:      health,
		threshold:   threshold,
		lockMessage: lockMessage,
		timeout:     timeout,
	}, nil
}

// Initializes a replication after performing some initial validation.
// Validation errors are returned immediately, and asynchronous errors are
// passed on the returned channel
func (r replicator) InitializeReplication(
	overrideLock bool,
	ignoreControllers bool,
	concurrentRealityRequests int,
) (Replication, chan error, error) {
	return r.initializeReplicationWithCheck(
		overrideLock,
		ignoreControllers,
		concurrentRealityRequests,
		true,
	)
}

func (r replicator) InitializeReplicationWithCheck(
	overrideLock bool,
	ignoreControllers bool,
	concurrentRealityRequests int,
	checkPreparers bool,
) (Replication, chan error, error) {
	return r.initializeReplicationWithCheck(
		overrideLock,
		ignoreControllers,
		concurrentRealityRequests,
		checkPreparers,
	)
}

func (r replicator) initializeReplicationWithCheck(
	overrideLock bool,
	ignoreControllers bool,
	concurrentRealityRequests int,
	checkPreparers bool,
) (Replication, chan error, error) {
	var err error

	if checkPreparers {
		err = r.checkPreparers()
		if err != nil {
			return nil, nil, err
		}
	}
	if concurrentRealityRequests <= 0 {
		concurrentRealityRequests = DefaultConcurrentReality
	}

	errCh := make(chan error)
	replication := &replication{
		active:    r.active,
		nodes:     r.nodes,
		store:     r.store,
		labeler:   r.labeler,
		manifest:  r.manifest,
		health:    r.health,
		threshold: r.threshold,
		logger:    r.logger,
		errCh:     errCh,
		replicationCancelledCh: make(chan struct{}),
		replicationDoneCh:      make(chan struct{}),
		enactedCh:              make(chan struct{}),
		quitCh:                 make(chan struct{}),
		concurrentRealityRequests: make(chan struct{}, concurrentRealityRequests),
		timeout:                   r.timeout,
	}
	// To make a closed channel
	close(replication.enactedCh)

	err = replication.lockHosts(overrideLock, r.lockMessage)
	if err != nil {
		return nil, errCh, err
	}
	if !ignoreControllers {
		err = replication.checkForManaged()
		if err != nil {
			replication.Cancel()
			return nil, errCh, err
		}
	}
	return replication, errCh, nil
}

// Checks that the preparer is running on every host being deployed to.
func (r replicator) checkPreparers() error {
	for _, host := range r.nodes {
		_, _, err := r.store.Pod(kp.REALITY_TREE, host, preparer.POD_ID)
		if err != nil {
			return util.Errorf("Could not verify %v state on %q: %v", preparer.POD_ID, host, err)
		}
	}
	return nil
}
