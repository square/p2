package ds

import (
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/types"
)

const (
	// This label is applied to pods owned by an RC.
	DSIDLabel = "daemon_set_id"
)

type DaemonSet interface {
	ID() fields.ID

	IsDisabled() bool

	// Returns the daemon set's pod id
	PodID() types.PodID

	ClusterName() fields.ClusterName

	GetNodeSelector() klabels.Selector

	// Returns a list of all nodes that are selected by this daemon set's selector
	EligibleNodes() ([]types.NodeName, error)

	// WatchDesires watches for changes to its daemon set, then schedule/unschedule
	// pods to to the nodes that it is responsible for
	//
	// Whatever calls WatchDesires is responsible for sending signals for whether
	// the daemon set updated or deleted
	//
	// When this is first called, it assumes that the daemon set is created
	//
	// The caller is responsible for sending signals when something has been changed
	WatchDesires(
		quitCh <-chan struct{},
		updatedCh <-chan *fields.DaemonSet,
		deletedCh <-chan *fields.DaemonSet,
	) <-chan error

	// CurrentPods() returns all nodes that are scheduled by this daemon set
	CurrentPods() (types.PodLocations, error)

	// Schedules pods by using replication, this will automatically
	// cancel any current replications and re-enact
	PublishToReplication() error
}

type daemonSet struct {
	fields.DaemonSet

	contention    dsContention
	logger        logging.Logger
	kpStore       kp.Store
	scheduler     scheduler.Scheduler
	dsStore       dsstore.Store
	applicator    labels.Applicator
	healthChecker *checker.ConsulHealthChecker

	// This is the current replication enact go routine that is running
	currentReplication replication.Replication

	// This locks are used to make sure only one replication is going on at the same time
	replicationLock sync.Mutex
}

type dsContention struct {
	contendedWith fields.ID
	isContended   bool
}

func New(
	fields fields.DaemonSet,
	dsStore dsstore.Store,
	kpStore kp.Store,
	applicator labels.Applicator,
	logger logging.Logger,
	healthChecker *checker.ConsulHealthChecker,
) DaemonSet {
	return &daemonSet{
		DaemonSet: fields,

		dsStore:            dsStore,
		kpStore:            kpStore,
		logger:             logger,
		applicator:         applicator,
		scheduler:          scheduler.NewApplicatorScheduler(applicator),
		healthChecker:      healthChecker,
		currentReplication: nil,
	}
}

func (ds *daemonSet) ID() fields.ID {
	return ds.DaemonSet.ID
}

func (ds *daemonSet) IsDisabled() bool {
	return ds.DaemonSet.Disabled
}

func (ds *daemonSet) PodID() types.PodID {
	return ds.DaemonSet.PodID
}

func (ds *daemonSet) ClusterName() fields.ClusterName {
	return ds.DaemonSet.Name
}

func (ds *daemonSet) GetNodeSelector() klabels.Selector {
	return ds.DaemonSet.NodeSelector
}

func (ds *daemonSet) EligibleNodes() ([]types.NodeName, error) {
	return ds.scheduler.EligibleNodes(ds.Manifest, ds.NodeSelector)
}

func (ds *daemonSet) WatchDesires(
	quitCh <-chan struct{},
	updatedCh <-chan *fields.DaemonSet,
	deletedCh <-chan *fields.DaemonSet,
) <-chan error {
	errCh := make(chan error)
	nodesChangedCh := ds.applicator.WatchMatchDiff(ds.NodeSelector, labels.NODE, quitCh)

	// Do something whenever something is changed
	go func() {
		var err error
		defer close(errCh)
		defer ds.CancelReplication()

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
					return
				}
			}

			select {
			case newDS, ok := <-updatedCh:
				if !ok {
					// channel closed
					return
				}
				ds.logger.NoFields().Infof("Received daemon set update signal: %v", newDS)
				if newDS == nil {
					ds.logger.Errorf("Unexpected nil daemon set during update")
					return
				}
				if ds.ID() != newDS.ID {
					err = util.Errorf("Expected uuid to be the same, expected '%v', got '%v'", ds.ID(), newDS.ID)
					continue
				}
				ds.DaemonSet = *newDS

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

			case deleteDS, ok := <-deletedCh:
				if !ok {
					// channel closed
					return
				}
				ds.logger.NoFields().Infof("Received daemon set delete signal: %v", deleteDS)
				if deleteDS == nil {
					ds.logger.Errorf("Unexpected nil daemon set during delete")
					return
				}
				if ds.ID() != deleteDS.ID {
					err = util.Errorf("Expected uuid to be the same, expected '%v', got '%v'", ds.ID(), deleteDS.ID)
					continue
				}

				err = ds.clearPods()
				if err != nil {
					err = util.Errorf("Unable to clear pods from intent tree: %v", err)
					select {
					case errCh <- err:
					case <-quitCh:
					}
				}
				return

			case labeledChanges, ok := <-nodesChangedCh:
				if !ok {
					// channel closed
					return
				}
				if ds.Disabled {
					continue
				}
				err = ds.handleNodeChanges(labeledChanges)
				if err != nil {
					continue
				}

			case <-quitCh:
				return
			}
		}
	}()

	return errCh
}

// Watch for changes to nodes and sends update and delete signals
func (ds *daemonSet) handleNodeChanges(changes *labels.LabeledChanges) error {
	if len(changes.Updated) > 0 {
		ds.logger.NoFields().Infof("Received node change signal")
		err := ds.removePods()
		if err != nil {
			return util.Errorf("Unable to remove pods from intent tree: %v", err)
		}
		err = ds.addPods()
		if err != nil {
			return util.Errorf("Unable to add pods to intent tree: %v", err)
		}
		return nil
	}

	if len(changes.Created) > 0 {
		ds.logger.NoFields().Infof("Received node create signal")
		err := ds.addPods()
		if err != nil {
			return util.Errorf("Unable to add pods to intent tree: %v", err)
		}
	}

	if len(changes.Deleted) > 0 {
		ds.logger.NoFields().Infof("Received node delete signal")
		err := ds.removePods()
		if err != nil {
			return util.Errorf("Unable to remove pods from intent tree: %v", err)
		}
	}

	return nil
}

// addPods schedules pods for all unscheduled nodes selected by ds.nodeSelector
func (ds *daemonSet) addPods() error {
	podLocations, err := ds.CurrentPods()
	if err != nil {
		return util.Errorf("Error retrieving pod locations from daemon set: %v", err)
	}
	currentNodes := podLocations.Nodes()

	eligible, err := ds.EligibleNodes()
	if err != nil {
		return util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}
	// TODO: Grab a lock here for the pod_id before adding something to check
	// contention and then disable

	// Get the difference in nodes that we need to schedule on and then sort them
	// for deterministic ordering
	toScheduleSorted := types.NewNodeSet(eligible...).Difference(types.NewNodeSet(currentNodes...)).ListNodes()
	ds.logger.NoFields().Infof("Need to schedule %d nodes", len(toScheduleSorted))

	for _, node := range toScheduleSorted {
		err := ds.labelPod(node)
		if err != nil {
			return util.Errorf("Error labeling node: %v", err)
		}
	}

	if len(toScheduleSorted) > 0 {
		return ds.PublishToReplication()
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

	eligible, err := ds.EligibleNodes()
	if err != nil {
		return util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}

	// Get the difference in nodes that we need to unschedule on and then sort them
	// for deterministic ordering
	toUnscheduleSorted := types.NewNodeSet(currentNodes...).Difference(types.NewNodeSet(eligible...)).ListNodes()
	ds.logger.NoFields().Infof("Need to unschedule %d nodes", len(toUnscheduleSorted))

	ds.CancelReplication()

	for _, node := range toUnscheduleSorted {
		err := ds.unschedule(node)
		if err != nil {
			return util.Errorf("Error unscheduling node: %v", err)
		}
	}

	if len(podLocations)-len(toUnscheduleSorted) > 0 {
		return ds.PublishToReplication()
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

	ds.CancelReplication()

	for _, node := range toUnscheduleSorted {
		err := ds.unschedule(node)
		if err != nil {
			return util.Errorf("Error unscheduling node: %v", err)
		}
	}

	return nil
}

func (ds *daemonSet) labelPod(node types.NodeName) error {
	ds.logger.NoFields().Infof("Labelling '%v' in node '%v' with daemon set uuid '%v'", ds.Manifest.ID(), node, ds.ID())

	// Will apply the following label on the key <labels.POD>/<node>/<ds.Manifest.ID()>:
	// 	{ DSIDLabel : ds.ID() }
	// eg node/127.0.0.1/test_pod[daemon_set_id] := test_ds_id
	// This is for indicating that this pod path belongs to this daemon set
	id := labels.MakePodLabelKey(node, ds.Manifest.ID())
	err := ds.applicator.SetLabel(labels.POD, id, DSIDLabel, ds.ID().String())
	if err != nil {
		return util.Errorf("Error setting label: %v", err)
	}
	return nil
}

func (ds *daemonSet) unschedule(node types.NodeName) error {
	ds.logger.NoFields().Infof("Unscheduling '%v' in node '%v' with daemon set uuid '%v'", ds.Manifest.ID(), node, ds.ID())

	// Will remove the following key:
	// <kp.INTENT_TREE>/<node>/<ds.Manifest.ID()>
	_, err := ds.kpStore.DeletePod(kp.INTENT_TREE, node, ds.Manifest.ID())
	if err != nil {
		return util.Errorf("Unable to delete pod id '%v' in node '%v', from intent tree: %v", ds.Manifest.ID(), node, err)
	}

	// Will remove the following label on the key <labels.POD>/<node>/<ds.Manifest.ID()>: DSIDLabel
	// This is for indicating that this pod path no longer belongs to this daemon set
	id := labels.MakePodLabelKey(node, ds.Manifest.ID())
	err = ds.applicator.RemoveLabel(labels.POD, id, DSIDLabel)
	if err != nil {
		return util.Errorf("Error removing label: %v", err)
	}

	return nil
}

func (ds *daemonSet) PublishToReplication() error {
	// TODO: We need to specifically turn this on to work
	if ds.healthChecker == nil {
		ds.logger.Info("Healthchecker is nil")
		return nil
	}

	podLocations, err := ds.CurrentPods()
	if err != nil {
		return util.Errorf("Error retrieving pod locations from daemon set: %v", err)
	}
	nodes := podLocations.Nodes()

	ds.logger.Infof("Preparing to publish the following nodes: %v", nodes)

	thisHost, err := os.Hostname()
	if err != nil {
		ds.logger.Errorf("Could not retrieve hostname: %s", err)
		thisHost = ""
	}
	thisUser, err := user.Current()
	if err != nil {
		ds.logger.Errorf("Could not retrieve user: %s", err)
		thisUser = &user.User{}
	}
	lockMessage := fmt.Sprintf("%q from %q at %q", thisUser.Username, thisHost, time.Now())
	repl, err := replication.NewReplicator(
		ds.DaemonSet.Manifest,
		ds.logger,
		nodes,
		len(nodes)-ds.DaemonSet.MinHealth,
		ds.kpStore,
		ds.applicator,
		*ds.healthChecker,
		health.HealthState(health.Passing),
		lockMessage,
	)
	if err != nil {
		ds.logger.Errorf("Could not initialize replicator: %s", err)
	}

	ds.logger.Info("New replicator was made")

	// Do not override locks, ignore controllers, and don't check for preparers
	replication, errCh, err := repl.InitializeReplicationWithCheck(
		false,
		true,
		replication.DefaultConcurrentReality,
		false,
	)
	if err != nil {
		ds.logger.Errorf("Unable to initialize replication: %s", err)
		return err
	}

	ds.logger.Info("Replication initialized")

	// auto-drain this channel
	go func() {
		for err := range errCh {
			ds.logger.Errorf("Error occurred in replication: '%v'", err)
		}
	}()

	ds.replicationLock.Lock()
	if ds.currentReplication != nil {
		ds.currentReplication.Cancel()
	}
	// Set a new replication
	ds.currentReplication = replication

	go replication.Enact()
	ds.replicationLock.Unlock()

	ds.logger.Info("Replication enacted")

	return nil
}

func (ds *daemonSet) CancelReplication() {
	ds.replicationLock.Lock()
	defer ds.replicationLock.Unlock()

	if ds.currentReplication != nil {
		ds.currentReplication.Cancel()
		ds.currentReplication = nil
	}
}

func (ds *daemonSet) CurrentPods() (types.PodLocations, error) {
	// Changing DaemonSet.ID is not permitted, so as long as there is no uuid
	// collision, this will always get the current pod path that this daemon set
	// had scheduled on
	selector := klabels.Everything().Add(DSIDLabel, klabels.EqualsOperator, []string{ds.ID().String()})

	podMatches, err := ds.applicator.GetMatches(selector, labels.POD)
	if err != nil {
		return nil, util.Errorf("Unable to get matches on pod tree: %v", err)
	}

	result := make(types.PodLocations, len(podMatches))
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
