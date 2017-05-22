package ds

import (
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rcrowley/go-metrics"

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
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"
)

// DaemonSetLocker is necessary to allow coordination between multiple daemon
// set farm instances. Before processing a daemon set, each farm instance will
// attempt to acquire a distributed lock and will only proceed if the
// acquisition was successful.
//
// NOTE: this interface is separate from DaemonSetStore because it is specifically
// tied to consul semantics by requiring a consul session.
type DaemonSetLocker interface {
	LockForOwnership(dsID fields.ID, session consul.Session) (consulutil.Unlocker, error)
}

// Farm instatiates and deletes daemon sets as needed
type Farm struct {
	// constructor arguments
	store     store
	dsStore   DaemonSetStore
	dsLocker  DaemonSetLocker
	scheduler scheduler.Scheduler
	labeler   Labeler
	watcher   LabelWatcher
	// session stream for the daemon sets locked by this farm
	sessions <-chan string
	// The time to wait between node updates for each replication
	rateLimitInterval time.Duration
	dsRetryInterval   time.Duration

	children map[fields.ID]*childDS
	childMu  sync.Mutex
	session  consul.Session

	logger  logging.Logger
	alerter alerting.Alerter

	healthChecker    *checker.ConsulHealthChecker
	healthWatchDelay time.Duration

	monitorHealth  bool
	cachedPodMatch bool
}

type childDS struct {
	ds        DaemonSet
	quitCh    chan<- struct{}
	updatedCh chan<- *ds_fields.DaemonSet
	deletedCh chan<- *ds_fields.DaemonSet
	errCh     <-chan error
	unlocker  consulutil.Unlocker
}

func NewFarm(
	store store,
	dsStore DaemonSetStore,
	dsLocker DaemonSetLocker,
	labeler Labeler,
	watcher LabelWatcher,
	sessions <-chan string,
	logger logging.Logger,
	alerter alerting.Alerter,
	healthChecker *checker.ConsulHealthChecker,
	rateLimitInterval time.Duration,
	monitorHealth bool,
	cachedPodMatch bool,
	healthWatchDelay time.Duration,
	dsRetryInterval time.Duration,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &Farm{
		store:             store,
		dsStore:           dsStore,
		dsLocker:          dsLocker,
		scheduler:         scheduler.NewApplicatorScheduler(labeler),
		labeler:           labeler,
		watcher:           watcher,
		sessions:          sessions,
		children:          make(map[fields.ID]*childDS),
		logger:            logger,
		alerter:           alerter,
		healthChecker:     healthChecker,
		healthWatchDelay:  healthWatchDelay,
		rateLimitInterval: rateLimitInterval,
		monitorHealth:     monitorHealth,
		cachedPodMatch:    cachedPodMatch,
		dsRetryInterval:   dsRetryInterval,
	}
}

func (dsf *Farm) Start(quitCh <-chan struct{}) {
	consulutil.WithSession(quitCh, dsf.sessions, func(sessionQuit <-chan struct{}, sessionID string) {
		dsf.logger.WithField("session", sessionID).Infoln("Acquired new session for ds farm")
		dsf.session = dsf.store.NewUnmanagedSession(sessionID, "")
		go dsf.cleanupDaemonSetPods(sessionQuit)
		dsf.mainLoop(sessionQuit)
	})
}

const cleanupInterval = 60 * time.Second

// This function removes all pods with a DSIDLabel where the daemon set id does
// not exist in the store at every interval specified because it is possible
// that the farm will unexpectedly crash or someone deletes or modifies a node
func (dsf *Farm) cleanupDaemonSetPods(quitCh <-chan struct{}) {
	timer := time.NewTimer(time.Duration(0))

	for {
		select {
		case <-quitCh:
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

		allPods, err := dsf.labeler.GetMatches(dsIDLabelSelector, labels.POD, dsf.cachedPodMatch)
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

			_, err = dsf.store.DeletePod(consul.INTENT_TREE, nodeName, podID)
			if err != nil {
				dsf.logger.NoFields().Errorf("Unable to delete pod id '%v' in node '%v', from intent tree: %v", podID, nodeName, err)
				continue
			}

			id := labels.MakePodLabelKey(nodeName, podID)
			err = dsf.labeler.RemoveLabel(labels.POD, id, DSIDLabel)
			if err != nil {
				dsf.logger.NoFields().Errorf("Error removing ds pod id label '%v': %v", id, err)
			}
		}
	}
}

func (dsf *Farm) mainLoop(quitCh <-chan struct{}) {
	subQuit := make(chan struct{})
	defer close(subQuit)
	dsWatch := dsf.dsStore.Watch(subQuit)

	defer func() {
		dsf.session = nil
	}()
	defer dsf.closeAllChildren()
	defer dsf.logger.NoFields().Infoln("Session expired, releasing daemon sets")

	var changes dsstore.WatchedDaemonSets
	var err error
	var ok bool

	for {
		select {
		case <-quitCh:
			return
		case changes, ok = <-dsWatch:
			if !ok {
				return
			}
		}

		// This loop will check all the error channels of the children owned by this
		// farm, if any child outputs an error, close the child.
		// The quitCh here will also return if the caller to mainLoop closes it
		dsf.childMu.Lock()
		for dsID, child := range dsf.children {
			select {
			case <-quitCh:
				dsf.childMu.Unlock()
				return
			case err, ok = <-child.errCh:
				if err != nil {
					dsf.logger.Errorf("An error has occurred in spawned ds '%v':, %v", child.ds.ID(), err)
				}
				if !ok {
					// child error channel closed
					dsf.logger.Errorf("Child ds '%v' error channel closed, removing ds now", child.ds.ID())
					dsf.closeChild(dsID)
				}
				continue
			default:
			}
		}
		dsf.childMu.Unlock()
		dsf.handleDSChanges(changes, quitCh)
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
		close(child.quitCh)
		close(child.updatedCh)
		close(child.deletedCh)

		// if our lock is active, attempt to gracefully release it on this daemon set
		if dsf.session != nil {
			unlocker := dsf.children[dsID].unlocker
			if unlocker != nil {
				err := unlocker.Unlock()
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

func (dsf *Farm) handleDSChanges(changes dsstore.WatchedDaemonSets, quitCh <-chan struct{}) {
	if changes.Err != nil {
		dsf.logger.Infof("An error has occurred while watching daemon sets: %v", changes.Err)
		return
	}

	dsf.childMu.Lock()
	defer dsf.childMu.Unlock()
	if len(changes.Created) > 0 {
		dsf.logger.Infof("The following %d daemon sets have been created: %s", len(changes.Created), dsIDs(changes.Created))
		for _, dsFields := range changes.Created {
			var dsUnlocker consulutil.Unlocker
			var err error
			dsLogger := dsf.makeDSLogger(*dsFields)

			// If it is not in our map, then try to acquire the lock
			_, ok := dsf.children[dsFields.ID]
			if !ok {
				dsUnlocker, err = dsf.dsLocker.LockForOwnership(dsFields.ID, dsf.session)
				if _, ok := err.(consulutil.AlreadyLockedError); ok {
					// Lock was either already acquired by another farm or it was Acquired
					// by this farm
					dsf.logger.Infof("Lock on daemon set '%v' was already acquired by another ds farm", dsFields.ID)
					if err != nil {
						dsf.logger.Infof("Additional ds farm lock errors: %v", err)
					}
					dsf.releaseLock(dsUnlocker)
					continue
				} else if err != nil {
					// The session probably either expired or there was probably a network
					// error, so the rest will probably fail
					dsf.handleSessionExpiry(*dsFields, dsLogger, err)
					dsf.releaseLock(dsUnlocker)
					return
				}
				dsf.logger.Infof("Lock on daemon set '%v' acquired", dsFields.ID)
			}

			// If the daemon set contends with another daemon set, disable it
			dsContended, isContended, err := DSContends(dsFields, dsf.scheduler, dsf.dsStore)
			if err != nil {
				dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
				dsf.releaseLock(dsUnlocker)
				continue
			}

			if isContended {
				dsf.raiseContentionAlert(dsContended, *dsFields)
				dsf.logger.Errorf("Created daemon set '%s' contends with %s", dsFields.ID, dsContended.ID)
				newDS, err := dsf.dsStore.Disable(dsFields.ID)
				if err != nil {
					dsf.logger.Errorf("Error occurred when trying to disable daemon set: %v", err)
					dsf.releaseLock(dsUnlocker)
					continue
				}
				dsf.children[newDS.ID] = dsf.spawnDaemonSet(&newDS, dsUnlocker, dsLogger)
			} else {
				dsf.children[dsFields.ID] = dsf.spawnDaemonSet(dsFields, dsUnlocker, dsLogger)
			}
		}
	}

	if len(changes.Updated) > 0 {
		dsf.logger.Infof("The following %d daemon sets have been updated: %s", len(changes.Updated), dsIDs(changes.Updated))
		for _, dsFields := range changes.Updated {
			dsLogger := dsf.makeDSLogger(*dsFields)

			ds, ok := dsf.children[dsFields.ID]
			if !ok {
				dsUnlocker, err := dsf.dsLocker.LockForOwnership(dsFields.ID, dsf.session)
				if _, ok := err.(consulutil.AlreadyLockedError); ok {
					dsf.logger.Infof("Lock on daemon set '%v' was already acquired by another farm", dsFields.ID)
					if err != nil {
						dsf.logger.Infof("Additional ds farm lock errors: %v", err)
					}
					continue

				} else if err != nil {
					dsf.handleSessionExpiry(*dsFields, dsLogger, err)
					return
				}
				dsf.logger.Infof("Lock on daemon set '%v' acquired", dsFields.ID)

				dsf.children[dsFields.ID] = dsf.spawnDaemonSet(dsFields, dsUnlocker, dsLogger)
				ds = dsf.children[dsFields.ID]
			}

			// If the daemon set contends with another daemon set, disable it
			dsContended, isContended, err := DSContends(dsFields, dsf.scheduler, dsf.dsStore)
			if err != nil {
				dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
				continue
			}

			if isContended {
				dsf.raiseContentionAlert(dsContended, *dsFields)
				dsf.logger.Errorf("Updated daemon set '%s' contends with %s", dsFields.ID, dsContended.ID)
				newDS, err := dsf.dsStore.Disable(dsFields.ID)
				if err != nil {
					dsf.logger.Errorf("Error occurred when trying to disable daemon set: %v", err)
					continue
				}
				ds.updatedCh <- &newDS
			} else {
				ds.updatedCh <- dsFields
			}
		}
	}

	if len(changes.Same) > 0 {
		dsf.logger.Infof("The following %d daemon sets are the same: %s", len(changes.Same), dsIDs(changes.Same))
		for _, dsFields := range changes.Same {
			if _, ok := dsf.children[dsFields.ID]; ok {
				// For unchanged daemon sets, if it's already locked by us, we don't need to do anything.
				continue
			}

			dsLogger := dsf.makeDSLogger(*dsFields)
			dsUnlocker, err := dsf.dsLocker.LockForOwnership(dsFields.ID, dsf.session)
			if _, ok := err.(consulutil.AlreadyLockedError); ok {
				dsf.logger.Infof("Lock on daemon set '%v' was already acquired by another farm", dsFields.ID)
				if err != nil {
					dsf.logger.Infof("Additional ds farm lock errors: %v", err)
				}
				continue
			} else if err != nil {
				dsf.handleSessionExpiry(*dsFields, dsLogger, err)
				return
			}
			dsf.logger.Infof("Lock on daemon set '%v' acquired", dsFields.ID)

			dsf.children[dsFields.ID] = dsf.spawnDaemonSet(dsFields, dsUnlocker, dsLogger)
		}
	}

	if len(changes.Deleted) > 0 {
		dsf.logger.Infof("The following %d daemon sets have been deleted: %s", len(changes.Deleted), dsIDs(changes.Deleted))
		for _, dsFields := range changes.Deleted {
			dsf.logger.Infof("%v", *dsFields)

			var ok bool
			var child *childDS
			dsLogger := dsf.makeDSLogger(*dsFields)

			if child, ok = dsf.children[dsFields.ID]; !ok {
				dsUnlocker, err := dsf.dsLocker.LockForOwnership(dsFields.ID, dsf.session)
				if _, ok := err.(consulutil.AlreadyLockedError); ok {
					dsf.logger.Infof("Lock on daemon set '%v' was already acquired by another farm", dsFields.ID)
					if err != nil {
						dsf.logger.Infof("Additional ds farm lock errors: %v", err)
					}
					continue

				} else if err != nil {
					dsf.handleSessionExpiry(*dsFields, dsLogger, err)
					return
				} else {
					// We have to spawn the daemon set, so that it can act on its deletion.
					// Otherwise, child is nil when we reach the below select.
					child = dsf.spawnDaemonSet(dsFields, dsUnlocker, dsLogger)
					dsf.children[dsFields.ID] = child
				}
				dsf.logger.Infof("Lock on daemon set '%v' acquired", dsFields.ID)
			}

			select {
			case <-quitCh:
				return
			case err := <-child.errCh:
				if err != nil {
					dsf.logger.Errorf("Error occurred when deleting spawned daemon set '%v': %v", dsFields, err)
				}
				dsf.closeChild(dsFields.ID)
			case child.deletedCh <- dsFields:
				dsf.closeChild(dsFields.ID)
			}
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

func (dsf *Farm) releaseLock(unlocker consulutil.Unlocker) {
	if unlocker == nil {
		return
	}
	err := unlocker.Unlock()
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
func DSContends(dsFields *ds_fields.DaemonSet, scheduler scheduler.Scheduler, dsStore DaemonSetStore) (ds_fields.DaemonSet, bool, error) {
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
	}); alertErr != nil {
		dsf.logger.WithError(alertErr).Errorln("Unable to deliver alert!")
	}
}

// Creates a functioning daemon set that will watch and write to the pod tree
func (dsf *Farm) spawnDaemonSet(
	dsFields *ds_fields.DaemonSet,
	dsUnlocker consulutil.Unlocker,
	dsLogger logging.Logger,
) *childDS {
	ds := New(
		*dsFields,
		dsf.dsStore,
		dsf.store,
		dsf.labeler,
		dsf.watcher,
		dsLogger,
		dsf.healthChecker,
		dsf.rateLimitInterval,
		dsf.cachedPodMatch,
		dsf.healthWatchDelay,
		dsf.dsRetryInterval,
	)

	quitSpawnCh := make(chan struct{})
	updatedCh := make(chan *ds_fields.DaemonSet)
	deletedCh := make(chan *ds_fields.DaemonSet)

	desiresCh := ds.WatchDesires(quitSpawnCh, updatedCh, deletedCh)

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
				case <-quitSpawnCh:
					return
				case <-ticks.C:
					eligible, err := ds.EligibleNodes()
					if err != nil {
						dsLogger.WithError(err).Warnf("Error finding eligible nodes; can't count healthy nodes")
						continue
					}
					numHealthy := aggregateHealth.NumHealthyOf(eligible)
					for _, name := range ds.MetricNames("healthy") {
						gauge := metrics.GetOrRegisterGauge(name, p2metrics.Registry)
						gauge.Update(int64(numHealthy))
					}
				}
			}
		}()
	}

	return &childDS{
		ds:        ds,
		quitCh:    quitSpawnCh,
		updatedCh: updatedCh,
		deletedCh: deletedCh,
		errCh:     desiresCh,
		unlocker:  dsUnlocker,
	}
}
