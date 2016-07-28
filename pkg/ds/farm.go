package ds

import (
	"fmt"
	"sync"
	"time"

	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/ds/fields"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/types"
	klabels "k8s.io/kubernetes/pkg/labels"
)

// Farm instatiates and deletes daemon sets as needed
type Farm struct {
	// constructor arguments
	kpStore    kp.Store
	dsStore    dsstore.Store
	scheduler  scheduler.Scheduler
	applicator labels.Applicator
	// session stream for the daemon sets locked by this farm
	sessions <-chan string

	children map[fields.ID]*childDS
	childMu  sync.Mutex
	session  kp.Session

	logger  logging.Logger
	alerter alerting.Alerter
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
	kpStore kp.Store,
	dsStore dsstore.Store,
	applicator labels.Applicator,
	sessions <-chan string,
	logger logging.Logger,
	alerter alerting.Alerter,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &Farm{
		kpStore:    kpStore,
		dsStore:    dsStore,
		scheduler:  scheduler.NewApplicatorScheduler(applicator),
		applicator: applicator,
		sessions:   sessions,
		children:   make(map[fields.ID]*childDS),
		logger:     logger,
		alerter:    alerter,
	}
}

func (dsf *Farm) Start(quitCh <-chan struct{}) {
	consulutil.WithSession(quitCh, dsf.sessions, func(sessionQuit <-chan struct{}, sessionID string) {
		dsf.logger.WithField("session", sessionID).Infoln("Acquired new session for ds farm")
		dsf.session = dsf.kpStore.NewUnmanagedSession(sessionID, "")
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

		dsIDMap := make(map[fields.ID]ds_fields.DaemonSet)
		for _, dsFields := range allDaemonSets {
			dsIDMap[dsFields.ID] = dsFields
		}

		dsIDLabelSelector := klabels.Everything().
			Add(DSIDLabel, klabels.ExistsOperator, []string{})

		allPods, err := dsf.applicator.GetMatches(dsIDLabelSelector, labels.POD)
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

			_, err = dsf.kpStore.DeletePod(kp.INTENT_TREE, nodeName, podID)
			if err != nil {
				dsf.logger.NoFields().Errorf("Unable to delete pod id '%v' in node '%v', from intent tree: %v", podID, nodeName, err)
				continue
			}

			id := labels.MakePodLabelKey(nodeName, podID)
			err = dsf.applicator.RemoveLabel(labels.POD, id, DSIDLabel)
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

	defer dsf.logger.NoFields().Infoln("Session expired, releasing daemon sets")
	defer func() {
		dsf.session = nil
	}()
	defer dsf.closeAllChildren()

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

		// This loop will check all the error channels of the children spawn by this
		// farm, if any child outputs an error, close the child.
		// The quitCh here will also return if the caller to mainLoop closes it
		for dsID, child := range dsf.children {
			select {
			case <-quitCh:
				return
			case err, ok = <-child.errCh:
				if !ok {
					// child error channel closed
					dsf.closeChild(dsID)
					continue
				}
				dsf.logger.Errorf("An error has occurred in spawned ds '%v':, %v", child.ds, err)
				continue
			default:
			}
		}
		dsf.handleDSChanges(changes, quitCh)
	}
}

func (dsf *Farm) closeAllChildren() {
	for dsID := range dsf.children {
		dsf.closeChild(dsID)
	}
}

func (dsf *Farm) closeChild(dsID fields.ID) {
	if child, ok := dsf.children[dsID]; ok {
		dsf.logger.WithField("ds", dsID).Infoln("Releasing daemon set")
		close(child.quitCh)
		close(child.updatedCh)
		close(child.deletedCh)

		// if our lock is active, attempt to gracefully release it on this daemon set
		if dsf.session != nil {
			unlocker := dsf.children[dsID].unlocker
			err := unlocker.Unlock()
			if err != nil {
				dsf.logger.WithField("ds", dsID).Warnln("Could not release daemon set lock")
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

	if len(changes.Created) > 0 {
		dsf.logger.Infof("The following daemon sets have been created:")
		for _, dsFields := range changes.Created {
			dsf.logger.Infof("%v", *dsFields)

			dsLogger := dsf.makeDSLogger(*dsFields)
			dsUnlocker, err := dsf.dsStore.LockForOwnership(dsFields.ID, dsf.session)

			if _, ok := err.(consulutil.AlreadyLockedError); ok {
				// Lock was either already acquired by another farm or it was Acquired
				// by this farm
				if _, ok := dsf.children[dsFields.ID]; !ok {
					dsLogger.Debugln("Lock on daemon set was already acquired by another farm")
					continue
				}
			} else if err != nil {
				// The session probably either expired or there was probably a network
				// error, so the rest will probably fail
				dsf.makeSessionExpiryAlert(*dsFields, dsLogger, err)
				return
			}

			// If the daemon set contends with another daemon set, disable it
			dsIDContended, isContended, err := dsf.dsContends(dsFields)
			if err != nil {
				dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
				continue
			}

			if isContended {
				dsf.logger.Errorf("Created daemon set '%s' contends with %s", dsFields.ID, dsIDContended)
				newDS, err := dsf.dsStore.Disable(dsFields.ID)
				if err != nil {
					dsf.logger.Errorf("Error occurred when trying to disable daemon set: %v", err)
					continue
				}
				dsf.children[newDS.ID] = dsf.spawnDaemonSet(&newDS, dsUnlocker, dsLogger)
			} else {
				dsf.children[dsFields.ID] = dsf.spawnDaemonSet(dsFields, dsUnlocker, dsLogger)
			}
		}
	}

	if len(changes.Updated) > 0 {
		dsf.logger.Infof("The following daemon sets have been updated:")
		for _, dsFields := range changes.Updated {
			dsf.logger.Infof("%v", *dsFields)

			dsLogger := dsf.makeDSLogger(*dsFields)
			_, err := dsf.dsStore.LockForOwnership(dsFields.ID, dsf.session)

			if _, ok := err.(consulutil.AlreadyLockedError); ok {
				if _, ok := dsf.children[dsFields.ID]; !ok {
					dsLogger.Debugln("Lock on daemon set was already acquired by another farm")
					continue
				}
			} else if err != nil {
				dsf.makeSessionExpiryAlert(*dsFields, dsLogger, err)
				return
			}

			// If the daemon set contends with another daemon set, disable it
			if _, ok := dsf.children[dsFields.ID]; ok {
				dsIDContended, isContended, err := dsf.dsContends(dsFields)
				if err != nil {
					dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
					continue
				}

				if isContended {
					dsf.logger.Errorf("Updated daemon set '%s' contends with %s", dsFields.ID, dsIDContended)
					newDS, err := dsf.dsStore.Disable(dsFields.ID)
					if err != nil {
						dsf.logger.Errorf("Error occurred when trying to delete daemon set: %v", err)
						continue
					}
					dsf.children[newDS.ID].updatedCh <- &newDS
				} else {
					dsf.children[dsFields.ID].updatedCh <- dsFields
				}
			}
		}
	}

	if len(changes.Deleted) > 0 {
		dsf.logger.Infof("The following daemon sets have been deleted:")
		for _, dsFields := range changes.Deleted {
			dsf.logger.Infof("%v", *dsFields)

			dsLogger := dsf.makeDSLogger(*dsFields)
			_, err := dsf.dsStore.LockForOwnership(dsFields.ID, dsf.session)

			if _, ok := err.(consulutil.AlreadyLockedError); ok {
				if _, ok := dsf.children[dsFields.ID]; !ok {
					dsLogger.Debugln("Lock on daemon set was already acquired by another farm")
					continue
				}
			} else if err != nil {
				dsf.makeSessionExpiryAlert(*dsFields, dsLogger, err)
				return
			}

			if child, ok := dsf.children[dsFields.ID]; ok {
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
}

func (dsf *Farm) makeDSLogger(dsFields ds_fields.DaemonSet) logging.Logger {
	return dsf.logger.SubLogger(logrus.Fields{
		"ds":  dsFields.ID,
		"pod": dsFields.Manifest.ID(),
	})
}

func (dsf *Farm) makeSessionExpiryAlert(dsFields ds_fields.DaemonSet, dsLogger logging.Logger, err error) {
	dsLogger.WithError(err).Errorln("Got error while locking daemon set in farm - session may be expired")

	if alertErr := dsf.alerter.Alert(alerting.AlertInfo{
		Description: "Got error while locking daemon set - session may be expired",
		IncidentKey: "ds_session_expired",
		Details: struct {
			ID           ds_fields.ID          `json:"id"`
			Name         ds_fields.ClusterName `json:"cluster_name"`
			NodeSelector string                `json:"node_selector"`
			PodID        types.PodID           `json:"pod_id"`
		}{
			ID:           dsFields.ID,
			Name:         dsFields.Name,
			NodeSelector: dsFields.NodeSelector.String(),
			PodID:        dsFields.PodID,
		},
	}); alertErr != nil {
		dsf.logger.WithError(alertErr).Errorln("Unable to deliver alert!")
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
func (dsf *Farm) dsContends(dsFields *ds_fields.DaemonSet) (ds_fields.ID, bool, error) {
	// This daemon set does not contend if it is disabled
	if dsFields.Disabled {
		return "", false, nil
	}
	// Get all eligible nodes for this daemon set by looking at the labes.NODE tree
	eligibleNodes, err := dsf.scheduler.EligibleNodes(dsFields.Manifest, dsFields.NodeSelector)
	if err != nil {
		return "", false, util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}

	// If this daemon set has a node selector set to Everything, check the labels
	// of other daemon sets
	for _, child := range dsf.children {
		everythingSelector := klabels.Everything().String()
		if !child.ds.IsDisabled() && child.ds.PodID() == dsFields.PodID && child.ds.ID() != dsFields.ID {
			// Naively check if both selectors are the Everything selector plus
			// something else or if both selectors are the same
			//
			// This will still think that the following two selectors contend:
			// { az = zone_one, az != zone_one } and {} (the everything selector)
			// even though the first selector doesn't select anything
			//
			// If either the child or the current daemon set has the everything selector
			// then they contend
			if dsFields.NodeSelector.String() == everythingSelector ||
				child.ds.GetNodeSelector().String() == everythingSelector {
				dsf.raiseContentionAlert(child.ds, *dsFields)
				return child.ds.ID(), true, nil
			}

			// If both daemon sets have the same selector, then they contend
			//
			// This will still think that the following two selectors contend:
			// { az = zone_one, az != zone_one } and { az = zone_one, az != zone_one }
			// even though they don't select anything
			if dsFields.NodeSelector.String() == child.ds.GetNodeSelector().String() {
				dsf.raiseContentionAlert(child.ds, *dsFields)
				return child.ds.ID(), true, nil
			}

			// Check the child's eligibleNodes, then intersect it to see if there
			// are any overlaps
			//
			// NOTE: This is naive, it does not account for new nodes, so any alerts
			// we get will be caused by adding new nodes by both human, machine error,
			// are starting up a daemon set farm where contention already exists
			scheduledNodes, err := child.ds.EligibleNodes()
			if err != nil {
				return "", false, util.Errorf("Error getting scheduled nodes: %v", err)
			}
			intersectedNodes := types.NewNodeSet(eligibleNodes...).Intersection(types.NewNodeSet(scheduledNodes...))
			if intersectedNodes.Len() > 0 {
				dsf.raiseContentionAlert(child.ds, *dsFields)
				return child.ds.ID(), true, nil
			}
		}
	}

	return "", false, nil
}

func (dsf *Farm) raiseContentionAlert(oldDS DaemonSet, newDS ds_fields.DaemonSet) {
	var oldCurrentNodes []string

	podLocations, err := oldDS.CurrentPods()
	if err != nil {
		oldCurrentNodes = append(oldCurrentNodes, fmt.Sprintf("Error! Unable to get current pods: %v", err))

	} else {
		for _, node := range podLocations.Nodes() {
			oldCurrentNodes = append(oldCurrentNodes, node.String())
		}
	}

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
			OldID:           oldDS.ID(),
			OldName:         oldDS.ClusterName(),
			OldNodeSelector: oldDS.GetNodeSelector().String(),
			OldPodID:        oldDS.PodID(),
			OldDisabled:     oldDS.IsDisabled(),
			OldCurrentNodes: oldCurrentNodes,

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
		dsf.kpStore,
		dsf.applicator,
		dsLogger,
	)

	quitSpawnCh := make(chan struct{})
	updatedCh := make(chan *ds_fields.DaemonSet)
	deletedCh := make(chan *ds_fields.DaemonSet)

	desiresCh := ds.WatchDesires(quitSpawnCh, updatedCh, deletedCh)

	return &childDS{
		ds:        ds,
		quitCh:    quitSpawnCh,
		updatedCh: updatedCh,
		deletedCh: deletedCh,
		errCh:     desiresCh,
		unlocker:  dsUnlocker,
	}
}
