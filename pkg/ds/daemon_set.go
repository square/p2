package ds

import (
	"time"

	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/types"
)

const (
	// This label is applied to pods owned by an RC.
	DSIDLabel = "daemon_set_id"
)

type DaemonSet interface {
	ID() fields.ID

	WatchDesires(
		quitCh <-chan struct{},
		updatedCh <-chan fields.DaemonSet,
		deletedCh <-chan struct{},
		nodesChangedCh <-chan struct{},
	) <-chan error

	// CurrentPods() returns all nodes that are scheduled by this daemon set
	CurrentPods() (PodLocations, error)
}

type PodLocation struct {
	Node  types.NodeName
	PodID types.PodID
}
type PodLocations []PodLocation

// Nodes returns a list of just the locations' nodes.
func (l PodLocations) Nodes() []types.NodeName {
	nodes := make([]types.NodeName, len(l))
	for i, pod := range l {
		nodes[i] = pod.Node
	}
	return nodes
}

// These methods are the same as the methods of the same name in kp.Store.
// This interface makes writing unit tests easier as daemon sets do not
// use any functions not listed here
type kpStore interface {
	SetPod(
		podPrefix kp.PodPrefix,
		nodeName types.NodeName,
		manifest pods.Manifest,
	) (time.Duration, error)

	DeletePod(podPrefix kp.PodPrefix,
		nodeName types.NodeName,
		manifestID types.PodID,
	) (time.Duration, error)
}

type daemonSet struct {
	fields.DaemonSet

	logger     logging.Logger
	kpStore    kpStore
	scheduler  scheduler.Scheduler
	dsStore    dsstore.Store
	applicator labels.Applicator
}

func New(
	fields fields.DaemonSet,
	dsStore dsstore.Store,
	kpStore kpStore,
	applicator labels.Applicator,
	logger logging.Logger,
) DaemonSet {
	return &daemonSet{
		DaemonSet: fields,

		dsStore:    dsStore,
		kpStore:    kpStore,
		logger:     logger,
		applicator: applicator,
		scheduler:  scheduler.NewApplicatorScheduler(applicator),
	}
}

func (ds *daemonSet) ID() fields.ID {
	return ds.DaemonSet.ID
}

// WatchDesires watches for changes to its daemon set, then schedule/unschedule
// pods to to the nodes that it is responsible for
//
// Whatever calls WatchDesires is responsible for sending signals for whether
// the daemon set updated or deleted
//
// When this is first called, it assumes that the daemon set is created
//
// The caller is responsible for sending signals when something has been changed
func (ds *daemonSet) WatchDesires(
	quitCh <-chan struct{},
	updatedCh <-chan fields.DaemonSet,
	deletedCh <-chan struct{},
	nodesChangedCh <-chan struct{},
) <-chan error {
	errCh := make(chan error)

	// Do something whenever something is changed
	go func() {
		var err error
		defer close(errCh)

		// Try to schedule pods when this begins watching
		if !ds.Disabled {
			ds.logger.NoFields().Infof("Received new daemon set: %v", *ds)
			err = ds.addPods()
			if err != nil {
				err = util.Errorf("Unable to add pods to intent tree: %v", err)
			}
		}

		for {
			if err != nil {
				select {
				case errCh <- err:
				case <-quitCh:
				}
			}

			select {
			case newDS := <-updatedCh:
				ds.logger.NoFields().Infof("Received daemon set update signal: %v", newDS)
				if ds.ID() != newDS.ID {
					err = util.Errorf("Expected uuid to be the same, expected '%v', got '%v'", ds.ID(), newDS.ID)
					continue
				}
				ds.DaemonSet = newDS

				if ds.Disabled {
					continue
				}
				err := ds.removePods()
				if err != nil {
					err = util.Errorf("Unable to remove pods from intent tree: %v", err)
					continue
				}
				err = ds.addPods()
				if err != nil {
					err = util.Errorf("Unable to add pods to intent tree: %v", err)
					continue
				}

			case <-deletedCh:
				ds.logger.NoFields().Infof("Received daemon set delete signal")
				err = ds.clearPods()
				if err != nil {
					err = util.Errorf("Unable to clear pods from intent tree: %v", err)
					select {
					case errCh <- err:
					case <-quitCh:
					}
				}
				return

			case <-nodesChangedCh:
				ds.logger.NoFields().Infof("Received node update signal")
				if ds.Disabled {
					continue
				}
				err := ds.removePods()
				if err != nil {
					err = util.Errorf("Unable to remove pods from intent tree: %v", err)
					continue
				}
				err = ds.addPods()
				if err != nil {
					err = util.Errorf("Unable to add pods to intent tree: %v", err)
					continue
				}

			case <-quitCh:
				return
			}
		}
	}()

	return errCh
}

// addPods schedules pods for all unscheduled nodes selected by ds.nodeSelector
func (ds *daemonSet) addPods() error {
	podLocations, err := ds.CurrentPods()
	if err != nil {
		return util.Errorf("Error retrieving pod locations from daemon set: %v", err)
	}
	currentNodes := podLocations.Nodes()

	eligible, err := ds.scheduler.EligibleNodes(ds.Manifest, ds.NodeSelector)
	if err != nil {
		return util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}

	// Get the difference in nodes that we need to schedule on and then sort them
	// for deterministic ordering
	toScheduleSorted := types.NewNodeSet(eligible...).Difference(types.NewNodeSet(currentNodes...)).ListNodes()
	ds.logger.NoFields().Infof("Need to schedule %d nodes", len(toScheduleSorted))

	for _, node := range toScheduleSorted {
		err := ds.schedule(node)
		if err != nil {
			return util.Errorf("Error scheduling node: %v", err)
		}
	}
	return nil
}

// removePods unschedules pods for all scheduled nodes not selected
// by ds.nodeSelector
func (ds *daemonSet) removePods() error {
	podLocations, err := ds.CurrentPods()
	if err != nil {
		return util.Errorf("Error retrieving pod locations from daemon set: %v", err)
	}
	currentNodes := podLocations.Nodes()

	eligible, err := ds.scheduler.EligibleNodes(ds.Manifest, ds.NodeSelector)
	if err != nil {
		return util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}

	// Get the difference in nodes that we need to unschedule on and then sort them
	// for deterministic ordering
	toUnscheduleSorted := types.NewNodeSet(currentNodes...).Difference(types.NewNodeSet(eligible...)).ListNodes()
	ds.logger.NoFields().Infof("Need to unschedule %d nodes", len(toUnscheduleSorted))

	for _, node := range toUnscheduleSorted {
		err := ds.unschedule(node)
		if err != nil {
			return util.Errorf("Error unscheduling node: %v", err)
		}
	}
	return nil
}

// clearPods unschedules pods for all the nodes that have been scheduled by
// this daemon set by using CurrentPods()
// This should only be used when a daemon set is deleted
func (ds *daemonSet) clearPods() error {
	podLocations, err := ds.CurrentPods()
	if err != nil {
		return util.Errorf("Error retrieving pod locations from daemon set: %v", err)
	}
	currentNodes := podLocations.Nodes()

	// Get the difference in nodes that we need to unschedule on and then sort them
	// for deterministic ordering
	toUnscheduleSorted := types.NewNodeSet(currentNodes...).ListNodes()
	ds.logger.NoFields().Infof("Need to unschedule %d nodes", len(toUnscheduleSorted))

	for _, node := range toUnscheduleSorted {
		err := ds.unschedule(node)
		if err != nil {
			return util.Errorf("Error scheduling node: %v", err)
		}
	}
	return nil
}

func (ds *daemonSet) schedule(node types.NodeName) error {
	ds.logger.NoFields().Infof("Scheduling on %s", node)

	// Will apply the following label on the key <labels.POD>/<node>/<ds.Manifest.ID()>:
	// 	{ DSIDLabel : ds.ID() }
	// eg node/127.0.0.1[daemon_set_id] := test_ds_id
	// This is for indicating that this pod path belongs to this daemon set
	id := MakePodLabelKey(node, ds.Manifest.ID())
	err := ds.applicator.SetLabel(labels.POD, id, DSIDLabel, ds.ID().String())
	if err != nil {
		return util.Errorf("Error setting label: %v", err)
	}

	// Will set the following the value, ds.Manifest, to the following key:
	// <kp.INTENT_TREE>/<node>/<ds.Manifest.ID()>
	// eg intent/127.0.0.1/test = test_pod
	_, err = ds.kpStore.SetPod(kp.INTENT_TREE, node, ds.Manifest)
	if err != nil {
		return util.Errorf("Error adding pod to intent tree: %v", err)
	}
	return nil
}

func (ds *daemonSet) unschedule(node types.NodeName) error {
	ds.logger.NoFields().Infof("Unscheduling from %s", node)

	// Will remove the following key:
	// <kp.INTENT_TREE>/<node>/<ds.Manifest.ID()>
	_, err := ds.kpStore.DeletePod(kp.INTENT_TREE, node, ds.Manifest.ID())
	if err != nil {
		return util.Errorf("Unable to delete pod from intent tree: %v", err)
	}

	// Will remove the following label on the key <labels.POD>/<node>/<ds.Manifest.ID()>: DSIDLabel
	// This is for indicating that this pod path no longer belongs to this daemon set
	id := MakePodLabelKey(node, ds.Manifest.ID())
	err = ds.applicator.RemoveLabel(labels.POD, id, DSIDLabel)
	if err != nil {
		return util.Errorf("Error removing label: %v", err)
	}

	return nil
}

func (ds *daemonSet) CurrentPods() (PodLocations, error) {
	// Changing DaemonSet.ID is not permitted, so as long as there is no uuid
	// collision, this will always get the current pod path that this daemon set
	// had scheduled on
	selector := klabels.Everything().Add(DSIDLabel, klabels.EqualsOperator, []string{ds.ID().String()})

	podMatches, err := ds.applicator.GetMatches(selector, labels.POD)
	if err != nil {
		return nil, util.Errorf("Unable to get matches on pod tree: %v", err)
	}

	result := make(PodLocations, len(podMatches))
	for i, podMatch := range podMatches {
		// ID will be something like <node>/<PodID>
		node, podID, err := labels.NodeAndPodIDFromPodLabel(podMatch)
		if err != nil {
			return nil, err
		}
		result[i].Node = node
		result[i].PodID = podID
	}

	return result, nil
}

func MakePodLabelKey(node types.NodeName, podID types.PodID) string {
	return node.String() + "/" + podID.String()
}
