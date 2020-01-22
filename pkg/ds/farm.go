package ds

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/ds/fields"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	p2metrics "github.com/square/p2/pkg/metrics"
	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	DefaultStatusWritingInterval = 15 * time.Second
)

// DaemonSetLocker is necessary to allow coordination between multiple daemon
// set farm instances. Before processing a daemon set, each farm instance will
// attempt to acquire a distributed lock and will only proceed if the
// acquisition was successful. All subsequent consul operations performed as
// part of servicing the daemon set will ensure that the lock is held within a
// transaction.
//
// NOTE: this interface is separate from DaemonSetStore because it is specifically
// tied to consul semantics by requiring a consul session.
type DaemonSetLocker interface {
	LockForOwnershipTxn(
		lockCtx context.Context,
		dsID fields.ID,
		session consul.Session,
	) (consul.TxnUnlocker, error)
}

// Farm instatiates and deletes daemon sets as needed
type Farm struct {
	// constructor arguments
	store       store
	txner       transaction.Txner
	dsStore     DaemonSetStore
	dsLocker    DaemonSetLocker
	statusStore StatusStore
	scheduler   Scheduler
	labeler     Labeler
	watcher     LabelWatcher
	// session stream for the daemon sets locked by this farm
	sessions <-chan string
	// The time to wait between node updates for each replication
	rateLimitInterval     time.Duration
	dsRetryInterval       time.Duration
	statusWritingInterval time.Duration

	children map[fields.ID]*childDS
	childMu  sync.Mutex
	session  consul.Session

	logger  logging.Logger
	alerter alerting.Alerter

	healthChecker    *checker.HealthChecker
	healthWatchDelay time.Duration

	monitorHealth         bool
	cachedPodMatch        bool
	labelsAggregationRate time.Duration

	config DSFarmConfig
}

type childDS struct {
	ds        DaemonSet
	cancel    context.CancelFunc
	updatedCh chan ds_fields.DaemonSet
	deletedCh chan<- ds_fields.DaemonSet
	errCh     <-chan error
	unlocker  consul.TxnUnlocker
}

// TODO: move other config options in here to reduce the number of arguments to NewFarm()
type DSFarmConfig struct {
	// PodBlacklist represents pod IDs that the farm should skip over. In other
	// words, if the daemon set's pod ID matches one of these the farm will
	// refuse to service it
	PodBlacklist []types.PodID `yaml:"pod_blacklist" json:"pod_blacklist"`

	// PodWhitelist contains the set of pod IDs that the farm should exclusively
	// service. If there is at least one entry in here, all daemon sets with pod
	// IDs other than the ones included in the whitelist will be ignored by this
	// farm
	PodWhitelist []types.PodID `yaml:"pod_whitelist" json:"pod_whitelist"`

	StatusWritingInterval time.Duration
}

func NewFarm(
	store store,
	txner transaction.Txner,
	dsStore DaemonSetStore,
	dsLocker DaemonSetLocker,
	statusStore StatusStore,
	labeler Labeler,
	watcher LabelWatcher,
	sessions <-chan string,
	logger logging.Logger,
	alerter alerting.Alerter,
	healthChecker *checker.HealthChecker,
	rateLimitInterval time.Duration,
	monitorHealth bool,
	cachedPodMatch bool,
	healthWatchDelay time.Duration,
	dsRetryInterval time.Duration,
	farmConfig DSFarmConfig,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	statusWritingInterval := farmConfig.StatusWritingInterval
	if statusWritingInterval == 0 {
		statusWritingInterval = DefaultStatusWritingInterval
	}

	return &Farm{
		store:                 store,
		txner:                 txner,
		dsStore:               dsStore,
		dsLocker:              dsLocker,
		statusStore:           statusStore,
		scheduler:             scheduler.NewApplicatorScheduler(labeler),
		labeler:               labeler,
		watcher:               watcher,
		sessions:              sessions,
		children:              make(map[fields.ID]*childDS),
		logger:                logger,
		alerter:               alerter,
		healthChecker:         healthChecker,
		healthWatchDelay:      healthWatchDelay,
		rateLimitInterval:     rateLimitInterval,
		monitorHealth:         monitorHealth,
		cachedPodMatch:        cachedPodMatch,
		dsRetryInterval:       dsRetryInterval,
		statusWritingInterval: statusWritingInterval,
		config:                farmConfig,
	}
}

func (dsf *Farm) Start(quitCh <-chan struct{}) {
	consulutil.WithSession(quitCh, dsf.sessions, func(sessionQuit <-chan struct{}, sessionID string) {
		dsf.logger.WithField("session", sessionID).Infoln("Acquired new session for ds farm")
		dsf.session = dsf.store.NewUnmanagedSession(sessionID, "")

		// TODO: make consulutil.WithSession use a context instead of
		// cancellation channel. Here we're just translating from that
		// channel to a context cancellation
		farmCtx, cancel := context.WithCancel(context.TODO())
		go func() {
			<-sessionQuit
			cancel()
		}()

		go dsf.cleanupDaemonSetPods(farmCtx)
		dsf.mainLoop(farmCtx)
	})
}

const cleanupInterval = 60 * time.Second

// This function removes all pods with a DSIDLabel where the daemon set id does
// not exist in the store at every interval specified because it is possible
// that the farm will unexpectedly crash or someone deletes or modifies a node
func (dsf *Farm) cleanupDaemonSetPods(ctx context.Context) {
	timer := time.NewTimer(time.Duration(0))

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		timer.Reset(cleanupInterval)

		allDaemonSets, err := dsf.dsStore.List()
		if err != nil {
			dsf.logger.Errorf("Unable to get daemon sets from intent tree in daemon set farm: %v", err)
			continue
		}

		countHistogram := metrics.GetOrRegisterHistogram("ds_count", p2metrics.Registry, metrics.NewExpDecaySample(1028, 0.015))
		countHistogram.Update(int64(len(allDaemonSets)))

		dsIDMap := make(map[fields.ID]ds_fields.DaemonSet)
		for _, dsFields := range allDaemonSets {
			dsIDMap[dsFields.ID] = dsFields
		}

		dsIDLabelSelector := klabels.Everything().
			Add(DSIDLabel, klabels.ExistsOperator, []string{})

		var allPods []labels.Labeled
		if dsf.cachedPodMatch {
			allPods, err = dsf.labeler.GetCachedMatches(dsIDLabelSelector, labels.POD, dsf.labelsAggregationRate)
		} else {
			allPods, err = dsf.labeler.GetMatches(dsIDLabelSelector, labels.POD)
		}
		if err != nil {
			dsf.logger.Errorf("Unable to get matches for daemon sets in pod store: %v", err)
			continue
		}

		for _, podLabels := range allPods {
			// Only check if it is a pod scheduled by a daemon set
			dsID := podLabels.Labels.Get(DSIDLabel)

			// Check if the daemon set exists, if it doesn't unschedule the pod
			if _, ok := dsIDMap[fields.ID(dsID)]; ok {
				continue
			}

			nodeName, podID, err := labels.NodeAndPodIDFromPodLabel(podLabels)
			if err != nil {
				dsf.logger.NoFields().Error(err)
				continue
			}

			// TODO: Since this mirrors the unschedule function in daemon_set.go,
			// We should find a nice way to couple them together
			dsf.logger.NoFields().Infof("Unscheduling '%v' in node '%v' with dangling daemon set uuid '%v'", podID, nodeName, dsID)

			ctx, cancel := transaction.New(context.Background())
			err = dsf.store.DeletePodTxn(ctx, consul.INTENT_TREE, nodeName, podID)
			if err != nil {
				dsf.logger.NoFields().Errorf("Unable to delete pod id '%v' in node '%v', from intent tree: %v", podID, nodeName, err)
				cancel()
				continue
			}

			id := labels.MakePodLabelKey(nodeName, podID)
			err = dsf.labeler.RemoveLabelTxn(ctx, labels.POD, id, DSIDLabel)
			if err != nil {
				dsf.logger.NoFields().Errorf("Error removing ds pod id label '%v': %v", id, err)
				cancel()
				continue
			}

			err = transaction.MustCommit(ctx, dsf.txner)
			if err != nil {
				dsf.logger.Errorf("could not remove %s from %s: %s", podID, nodeName, err)
				cancel()
				continue
			}
		}
	}
}

func (dsf *Farm) mainLoop(ctx context.Context) {
	// TODO: use a context here instead of cancellation channel
	subQuit := make(chan struct{})
	defer close(subQuit)
	dsWatch := dsf.dsStore.Watch(subQuit)

	defer func() {
		dsf.session = nil
	}()
	defer dsf.closeAllChildren()
	defer dsf.logger.NoFields().Infoln("Session expired, releasing daemon sets")

	var changes dsstore.WatchedDaemonSets
	var ok bool

	for {
		select {
		case <-ctx.Done():
			return
		case changes, ok = <-dsWatch:
			if !ok {
				return
			}
			dsf.handleDSChanges(ctx, changes)
		}
	}
}

func (dsf *Farm) closeAllChildren() {
	dsf.childMu.Lock()
	for dsID := range dsf.children {
		dsf.closeChild(dsID)
	}
	dsf.childMu.Unlock()
}

// closeChild must be called with childMu lock held
func (dsf *Farm) closeChild(dsID fields.ID) {
	if child, ok := dsf.children[dsID]; ok {
		dsf.logger.WithField("ds", dsID).Infoln("Releasing daemon set")
		child.cancel()

		// drain the updatedCh (it's buffered)
		for range child.updatedCh {
		}
		close(child.updatedCh)
		close(child.deletedCh)

		// if our lock is active, attempt to gracefully release it on this daemon set
		if dsf.session != nil {
			unlocker := dsf.children[dsID].unlocker
			if unlocker != nil {
				ctx, cancel := transaction.New(context.Background())
				defer cancel()

				err := transaction.MustCommit(ctx, dsf.txner)
				if err != nil {
					dsf.logger.WithField("ds", dsID).Warnln("Could not release daemon set lock")
				} else {
					dsf.logger.WithField("ds", dsID).Info("Daemon set lock successfully released")
				}
			}
		}
		delete(dsf.children, dsID)
	}
}

func (dsf *Farm) handleDSChanges(ctx context.Context, changes dsstore.WatchedDaemonSets) {
	if changes.Err != nil {
		dsf.logger.Infof("An error has occurred while watching daemon sets: %v", changes.Err)
		return
	}

	dsf.childMu.Lock()
	defer dsf.childMu.Unlock()
	if len(changes.Created) > 0 {
		for _, dsFields := range changes.Created {
			dsf.lockAndSpawn(ctx, *dsFields, Created)
		}
	}

	if len(changes.Updated) > 0 {
		for _, dsFields := range changes.Updated {
			dsf.lockAndSpawn(ctx, *dsFields, Updated)
		}
	}

	if len(changes.Same) > 0 {
		for _, dsFields := range changes.Same {
			dsf.lockAndSpawn(ctx, *dsFields, Same)
		}
	}

	if len(changes.Deleted) > 0 {
		for _, dsFields := range changes.Deleted {
			// We have to spawn the daemon set, so that it can act on its deletion.
			// Otherwise, child is nil when we reach the below select.
			// TODO: do we really need this?
			// comment above is old, possibly
			// before we stopped daemon set
			// deletion behavior were all pods
			// become deleted
			dsf.lockAndSpawn(ctx, *dsFields, Deleted)
			dsf.logger.Infof("%v", *dsFields)

			child, ok := dsf.children[dsFields.ID]
			if !ok {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case err := <-child.errCh:
				if err != nil {
					dsf.logger.Errorf("Error occurred when deleting spawned daemon set '%v': %v", dsFields, err)
				}
				dsf.closeChild(dsFields.ID)
			case child.deletedCh <- *dsFields:
				dsf.closeChild(dsFields.ID)
			}

			dsf.logger.Infof("Released deleted daemon set %s", dsFields.ID)
		}
	}
}

func dsIDs(dss []*ds_fields.DaemonSet) []ds_fields.ID {
	var ids []ds_fields.ID
	for _, ds := range dss {
		if ds != nil {
			ids = append(ids, ds.ID)
		}
	}
	return ids
}

func (dsf *Farm) makeDSLogger(dsFields ds_fields.DaemonSet) logging.Logger {
	return dsf.logger.SubLogger(logrus.Fields{
		"ds":  dsFields.ID,
		"pod": dsFields.Manifest.ID(),
	})
}
func (dsf *Farm) handleSessionExpiry(dsFields ds_fields.DaemonSet, dsLogger logging.Logger, err error) {
	dsLogger.WithFields(logrus.Fields{
		"ID":           dsFields.ID,
		"Name":         dsFields.Name,
		"NodeSelector": dsFields.NodeSelector.String(),
		"PodID":        dsFields.PodID,
	}).WithError(err).Errorln("Got error while locking daemon set in ds farm - session may be expired")
}

func (dsf *Farm) releaseLock(unlocker consul.TxnUnlocker) {
	ctx, cancel := transaction.New(context.Background())
	defer cancel()

	err := transaction.MustCommit(ctx, dsf.txner)
	if err != nil {
		dsf.logger.Errorf("Error releasing lock on ds farm: %v", err)
	}
}

// Naive implementation of a guard, this checks if any of the scheduled nodes
// are used by two daemon sets, does not pre-emptively catch overlaps by selector
// because of how kubernetes selectors work
//
// Also naively checks the selectors to see if there are any selector overlap
// if two label selectors are labels.Everything()
//
// Returns [ daemon set contended, contention exists, error ]
func DSContends(dsFields ds_fields.DaemonSet, scheduler Scheduler, dsStore DaemonSetStore) (ds_fields.DaemonSet, bool, error) {
	// This daemon set does not contend if it is disabled
	if dsFields.Disabled {
		return ds_fields.DaemonSet{}, false, nil
	}
	// Get all eligible nodes for this daemon set by looking at the labes.NODE tree
	eligibleNodes, err := scheduler.EligibleNodes(dsFields.Manifest, dsFields.NodeSelector)
	if err != nil {
		return ds_fields.DaemonSet{}, false, util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}

	allDaemonSets, err := dsStore.List()
	if err != nil {
		return ds_fields.DaemonSet{}, false, util.Errorf("listing daemon sets failed so could not check for contention: %s", err)
	}

	// If this daemon set has a node selector set to Everything, check the labels
	// of other daemon sets
	for _, otherDS := range allDaemonSets {
		everythingSelector := klabels.Everything().String()
		if !otherDS.Disabled && otherDS.PodID == dsFields.PodID && otherDS.ID != dsFields.ID {
			// Naively check if both selectors are the Everything selector plus
			// something else or if both selectors are the same
			//
			// This will still think that the following two selectors contend:
			// { az = zone_one, az != zone_one } and {} (the everything selector)
			// even though the first selector doesn't select anything
			//
			// If either the otherDS or the current daemon set has the everything selector
			// then they contend
			if dsFields.NodeSelector.String() == everythingSelector ||
				otherDS.NodeSelector.String() == everythingSelector {
				return otherDS, true, nil
			}

			// If both daemon sets have the same selector, then they contend
			//
			// This will still think that the following two selectors contend:
			// { az = zone_one, az != zone_one } and { az = zone_one, az != zone_one }
			// even though they don't select anything
			if dsFields.NodeSelector.String() == otherDS.NodeSelector.String() {
				return otherDS, true, nil
			}

			// Check the otherDS's eligibleNodes, then intersect it to see if there
			// are any overlaps
			//
			// NOTE: This is naive, it does not account for new nodes, so any alerts
			// we get will be caused by adding new nodes by both human, machine error,
			// are starting up a daemon set farm where contention already exists
			scheduledNodes, err := scheduler.EligibleNodes(otherDS.Manifest, otherDS.NodeSelector)
			if err != nil {
				return ds_fields.DaemonSet{}, false, util.Errorf("Error getting scheduled nodes: %v", err)
			}
			intersectedNodes := types.NewNodeSet(eligibleNodes...).Intersection(types.NewNodeSet(scheduledNodes...))
			if intersectedNodes.Len() > 0 {
				return otherDS, true, nil
			}
		}
	}

	return ds_fields.DaemonSet{}, false, nil
}

func (dsf *Farm) raiseContentionAlert(oldDS ds_fields.DaemonSet, newDS ds_fields.DaemonSet) {
	if alertErr := dsf.alerter.Alert(alerting.AlertInfo{
		Description: fmt.Sprintf("New ds '%v', contends with '%v'", oldDS.ID, newDS.ID),
		IncidentKey: "preemptive_ds_contention",
		Details: struct {
			OldID           ds_fields.ID          `json:"old_id"`
			OldName         ds_fields.ClusterName `json:"old_cluster_name"`
			OldNodeSelector string                `json:"old_node_selector"`
			OldPodID        types.PodID           `json:"old_pod_id"`
			OldDisabled     bool                  `json:"old_disabled"`
			OldCurrentNodes []string              `json:"old_current_nodes"`

			NewID           ds_fields.ID          `json:"new_id"`
			NewName         ds_fields.ClusterName `json:"new_cluster_name"`
			NewNodeSelector string                `json:"new_node_selector"`
			NewdPodID       types.PodID           `json:"new_pod_id"`
			NewDisabled     bool                  `json:"new_disabled"`
		}{
			OldID:           oldDS.ID,
			OldName:         oldDS.Name,
			OldNodeSelector: oldDS.NodeSelector.String(),
			OldPodID:        oldDS.PodID,
			OldDisabled:     oldDS.Disabled,

			NewID:           newDS.ID,
			NewName:         newDS.Name,
			NewNodeSelector: newDS.NodeSelector.String(),
			NewdPodID:       newDS.PodID,
			NewDisabled:     newDS.Disabled,
		},
	}, alerting.LowUrgency); alertErr != nil {
		dsf.logger.WithError(alertErr).Errorln("Unable to deliver alert!")
	}
}

// Creates a functioning daemon set that will watch and write to the pod tree
func (dsf *Farm) spawnDaemonSet(
	ctx context.Context,
	unlocker consul.TxnUnlocker,
	dsFields ds_fields.DaemonSet,
	dsLogger logging.Logger,
) *childDS {
	ds := New(
		dsFields,
		dsf.dsStore,
		dsf.store,
		dsf.txner,
		dsf.labeler,
		dsf.watcher,
		dsf.labelsAggregationRate,
		dsLogger,
		dsf.healthChecker,
		dsf.rateLimitInterval,
		dsf.cachedPodMatch,
		dsf.healthWatchDelay,
		dsf.dsRetryInterval,
		unlocker,
		dsf.statusStore,
		dsf.statusWritingInterval,
	)

	// updatedCh is buffered by 1 to protect the control loop by slow (or
	// dead) readers
	updatedCh := make(chan ds_fields.DaemonSet, 1)
	deletedCh := make(chan ds_fields.DaemonSet)
	ctx, cancel := context.WithCancel(ctx)

	desiresCh := ds.WatchDesires(ctx, updatedCh, deletedCh)

	if dsf.monitorHealth {
		go func() {
			// daemon sets are deployed to many servers and deploys are very slow. This means that
			// 1) the data size for each health response is huge which can suck up bandwidth
			// 2) the metrics aren't going to change very frequently, so a low sample rate is acceptable
			aggregateHealth := replication.AggregateHealth(ds.PodID(), *dsf.healthChecker, dsf.healthWatchDelay)
			ticks := time.NewTicker(dsf.healthWatchDelay)

			defer aggregateHealth.Stop()
			defer ticks.Stop()

			for {
				select {
				case <-ctx.Done():
					for _, suffix := range []string{"healthy", "critical", "unknown", "warning"} {
						for _, name := range ds.MetricNames(suffix) {
							p2metrics.Registry.Unregister(name)
						}
					}
					return
				case <-ticks.C:
					eligible, err := ds.EligibleNodes()
					if err != nil {
						dsLogger.WithError(err).Warnf("Error finding eligible nodes; can't count healthy nodes")
						continue
					}

					for _, metric := range []struct {
						suffix string
						val    int
					}{
						{"healthy", aggregateHealth.NumHealthyOf(eligible)},
						{"critical", aggregateHealth.NumUnhealthyOf(eligible)},
						{"unknown", aggregateHealth.NumUnknownHealthOf(eligible)},
						{"warning", aggregateHealth.NumWarningHealthOf(eligible)},
					} {
						for _, name := range ds.MetricNames(metric.suffix) {
							gauge := metrics.GetOrRegisterGauge(name, p2metrics.Registry)
							gauge.Update(int64(metric.val))
						}
					}
				}
			}
		}()
	}

	go func() {
		// This loop will check the error channel of the child and if
		// the child's error channel closes, close the child.
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-desiresCh:
				if err != nil {
					dsf.logger.Errorf("An error has occurred in spawned ds '%v':, %v", ds.ID(), err)
				}
				if !ok {
					// child error channel closed
					dsf.logger.Errorf("Child ds '%v' error channel closed, removing ds now", ds.ID())
					dsf.childMu.Lock()
					dsf.closeChild(ds.ID())
					dsf.childMu.Unlock()
					return
				}
			}
		}
	}()
	return &childDS{
		ds:        ds,
		cancel:    cancel,
		updatedCh: updatedCh,
		deletedCh: deletedCh,
		errCh:     desiresCh,
		unlocker:  unlocker,
	}
}

type dsState string

const (
	Created dsState = "created"
	Same    dsState = "same"
	Updated dsState = "updated"
	Deleted dsState = "deleted"
)

// lockAndSpawn does the following
// 1) checks if the daemon set is "owned" by this farm, and if not tries to
// acquire the lock and spawn a worker
// 2) if the daemon set is owned after step 1), it will check for contention
// with another DS, and if there is contention it will send an alert and
// disable the daemon set
// 3) If applicable, ensures the worker for this daemon set has the latest copy of the daemon
// set
func (dsf *Farm) lockAndSpawn(ctx context.Context, dsFields ds_fields.DaemonSet, dsState dsState) bool {
	var err error
	dsLogger := dsf.makeDSLogger(dsFields)

	if !dsf.shouldWorkOn(dsFields.Manifest.ID()) {
		return false
	}

	var unlocker consul.TxnUnlocker

	// If it is not in our map, then try to acquire the lock
	child, ok := dsf.children[dsFields.ID]
	if !ok {
		lockCtx, lockCancel := transaction.New(ctx)
		defer lockCancel()
		unlocker, err = dsf.dsLocker.LockForOwnershipTxn(lockCtx, dsFields.ID, dsf.session)
		if err != nil {
			dsf.logger.WithError(err).Errorln("could not build daemon set locking transaction")
			return false
		}

		ok, resp, err := transaction.Commit(lockCtx, dsf.txner)
		if err != nil {
			dsf.logger.WithError(err).Errorf("Could not acquire lock on daemon set '%v'", dsFields.ID)
			// The session probably either expired or there was probably a network
			// error, so the rest will probably fail
			dsf.handleSessionExpiry(dsFields, dsLogger, err)
			return false
		}
		if !ok {
			// Lock was either already acquired by another farm or it was Acquired
			// by this farm
			dsf.logger.Infof("Lock on daemon set '%v' was already acquired by another ds farm: %s", dsFields.ID, transaction.TxnErrorsToString(resp.Errors))
			if err != nil {
				dsf.logger.Infof("Additional ds farm lock errors: %v", err)
			}
			return false
		}
		dsf.logger.Infof("Lock on daemon set '%v' acquired", dsFields.ID)
	}

	// We only need to check for contention if it's new or changed. This is
	// an expensive operation to run, particularly when 99% of the time a
	// daemon set passed to this function hasn't changed
	if dsState == Updated || dsState == Created {
		// If the daemon set contends with another daemon set, disable it
		dsContended, isContended, err := DSContends(dsFields, dsf.scheduler, dsf.dsStore)
		if err != nil {
			dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
			dsf.releaseLock(unlocker)
			return false
		}

		if isContended {
			dsf.raiseContentionAlert(dsContended, dsFields)
			dsf.logger.Errorf("Created daemon set '%s' contends with %s", dsFields.ID, dsContended.ID)
			dsFields, err = dsf.dsStore.Disable(dsFields.ID)
			if err != nil {
				dsf.logger.Errorf("Error occurred when trying to disable daemon set: %v", err)
				dsf.releaseLock(unlocker)
				return false
			}
		}
	}

	// If we already are running the daemon set, just pass the update. Otherwise spawn one
	if ok {
		// try to drain the buffered value off the updatedCh if there is one (which
		// indicates the worker goroutine was slow to read it or is dead)
		select {
		case <-child.updatedCh:
			dsf.logger.Warnln("daemon set worker missed an update, sending a newer one")
		default:
		}

		child.updatedCh <- dsFields
	} else {
		dsf.children[dsFields.ID] = dsf.spawnDaemonSet(
			ctx,
			unlocker,
			dsFields,
			dsLogger,
		)
	}

	return true
}

// shouldWorkOn determines if the given pod ID should have its daemon sets
// serviced based on the configured blacklist and whitelist in the farm's
// config.
// If the white list has at least one pod ID in it, it will return true if and
// only if the given pod ID is in the white list. Otherwise, it will return
// true as long as the pod ID isn't in the blacklist.
func (dsf *Farm) shouldWorkOn(podID types.PodID) bool {
	if len(dsf.config.PodWhitelist) > 0 {
		for _, whiteListed := range dsf.config.PodWhitelist {
			if podID == whiteListed {
				return true
			}
		}

		return false
	}

	for _, blackListed := range dsf.config.PodBlacklist {
		if podID == blackListed {
			return false
		}
	}

	return true
}
