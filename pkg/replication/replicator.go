package replication

import (
	"errors"
	"time"

	"github.com/square/p2/pkg/constants"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
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
		rateLimitInterval time.Duration,
	) (Replication, chan error, error)

	// InitializeDaemonSetReplication creates a Replication with parameters suitable for a daemon set.
	// Specifically:
	// * hosts are not locked
	// * replication controllers are ignored
	// * and preparers are not checked.
	InitializeDaemonSetReplication(
		concurrentRealityRequests int,
		rateLimitInterval time.Duration,
	) (Replication, chan error, error)
}

// Replicator creates replications
type replicator struct {
	manifest         manifest.Manifest // the manifest to replicate
	logger           logging.Logger
	nodes            []types.NodeName
	active           int // maximum number of nodes to update concurrently
	store            Store
	labeler          Labeler
	health           checker.ConsulHealthChecker
	threshold        health.HealthState // minimum state to treat as "healthy"
	healthWatchDelay time.Duration      // interval of time between initiating health watches

	lockMessage string

	// Used to timeout daemon set replications
	timeout time.Duration
}

func NewReplicator(
	manifest manifest.Manifest,
	logger logging.Logger,
	nodes []types.NodeName,
	active int,
	store Store,
	labeler Labeler,
	health checker.ConsulHealthChecker,
	threshold health.HealthState,
	lockMessage string,
	timeout time.Duration,
	healthWatchDelay time.Duration,
) (Replicator, error) {
	if active < 1 {
		return replicator{}, util.Errorf("Active must be >= 1, was %d", active)
	}
	if active > 50 {
		logger.Infof("Number of concurrent updates (%v) is greater than 50, reducing to 50", active)
		active = 50
	}
	return replicator{
		manifest:         manifest,
		logger:           logger,
		nodes:            nodes,
		active:           active,
		store:            store,
		labeler:          labeler,
		health:           health,
		threshold:        threshold,
		lockMessage:      lockMessage,
		timeout:          timeout,
		healthWatchDelay: healthWatchDelay,
	}, nil
}

// Initializes a replication after performing some initial validation.
// Validation errors are returned immediately, and asynchronous errors are
// passed on the returned channel
func (r replicator) InitializeReplication(
	overrideLock bool,
	ignoreControllers bool,
	concurrentRealityRequests int,
	rateLimitInterval time.Duration,
) (Replication, chan error, error) {
	return r.initializeReplicationWithCheck(
		overrideLock,
		ignoreControllers,
		concurrentRealityRequests,
		true,
		false,
		rateLimitInterval,
	)
}

func (r replicator) InitializeDaemonSetReplication(
	concurrentRealityRequests int,
	rateLimitInterval time.Duration,
) (Replication, chan error, error) {
	return r.initializeReplicationWithCheck(
		true, // override locks (irrelevant; they're being skipped)
		true, // ignore Replication Controllers
		concurrentRealityRequests,
		false, // Ignore missing preparers by writing intent/ anyway
		true,  // skip locking
		rateLimitInterval,
	)
}

func (r replicator) initializeReplicationWithCheck(
	overrideLock bool,
	ignoreControllers bool,
	concurrentRealityRequests int,
	checkPreparers bool,
	skipLocking bool,
	rateLimitInterval time.Duration,
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

	// NewTicker() requires the value passed to it to be positive.
	var ticker *time.Ticker
	if rateLimitInterval > 0 {
		ticker = time.NewTicker(rateLimitInterval)
	}

	errCh := make(chan error)
	replication := &replication{
		active:                 r.active,
		nodes:                  r.nodes,
		store:                  r.store,
		labeler:                r.labeler,
		manifest:               r.manifest,
		health:                 r.health,
		threshold:              r.threshold,
		logger:                 r.logger,
		rateLimiter:            ticker,
		errCh:                  errCh,
		healthWatchDelay:       r.healthWatchDelay,
		replicationCancelledCh: make(chan struct{}),
		replicationDoneCh:      make(chan struct{}),
		enactedCh:              make(chan struct{}),
		quitCh:                 make(chan struct{}),
		concurrentRealityRequests: make(chan struct{}, concurrentRealityRequests),
		timeout:                   r.timeout,
	}
	// To make a closed channel
	close(replication.enactedCh)

	var session consul.Session
	var renewalErrCh chan error
	if !skipLocking {
		session, renewalErrCh, err = replication.lockHosts(overrideLock, r.lockMessage)
		if err != nil {
			return nil, errCh, err
		}
	}
	// It is safe to call this with nils for session and renewalErrCh
	go replication.handleReplicationEnd(session, renewalErrCh)

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
		_, _, err := r.store.Pod(consul.REALITY_TREE, host, constants.PreparerPodID)
		if err != nil {
			return util.Errorf("Could not verify %v state on %q: %v", constants.PreparerPodID, host, err)
		}
	}
	return nil
}
