package ds

import (
	"fmt"
	"os"
	"os/user"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/health"
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

	"github.com/rcrowley/go-metrics"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	// This label is applied to pods owned by a DS.
	DSIDLabel = "daemon_set_id"
)

var (
	retryInterval = time.Duration(5 * time.Minute)
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

	MetricNames(suffix string) []string

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

type Labeler interface {
	SetLabel(labelType labels.Type, id, name, value string) error
	RemoveLabel(labelType labels.Type, id, name string) error
	GetMatches(selector klabels.Selector, labelType labels.Type, cachedMatch bool) ([]labels.Labeled, error)
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
}

type LabelWatcher interface {
	WatchMatchDiff(
		selector klabels.Selector,
		labelType labels.Type,
		quitCh <-chan struct{},
	) <-chan *labels.LabeledChanges
}

type store interface {
	DeletePod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (time.Duration, error)
	NewUnmanagedSession(session, name string) consul.Session

	// For passing to the replication package:
	replication.Store
}

type DaemonSetStore interface {
	List() ([]fields.DaemonSet, error)
	Watch(quitCh <-chan struct{}) <-chan dsstore.WatchedDaemonSets
	LockForOwnership(dsID fields.ID, session consul.Session) (consulutil.Unlocker, error)
	Disable(id fields.ID) (fields.DaemonSet, error)
}

type daemonSet struct {
	fields.DaemonSet

	contention       dsContention
	logger           logging.Logger
	store            store
	scheduler        scheduler.Scheduler
	dsStore          DaemonSetStore
	applicator       Labeler
	watcher          LabelWatcher
	healthChecker    *checker.ConsulHealthChecker
	healthWatchDelay time.Duration

	// This is the current replication enact go routine that is running
	currentReplication replication.Replication

	// Indicates how long to wait between updating each node during a replication
	rateLimitInterval time.Duration

	// allow stale reads of matching pods.  We allow stale matches for daemon
	// set queries because the consequent operations are idempotent.
	cachedPodMatch bool
}

type dsContention struct {
	contendedWith fields.ID
	isContended   bool
}

func New(
	fields fields.DaemonSet,
	dsStore DaemonSetStore,
	store store,
	applicator Labeler,
	watcher LabelWatcher,
	logger logging.Logger,
	healthChecker *checker.ConsulHealthChecker,
	rateLimitInterval time.Duration,
	cachedPodMatch bool,
	healthWatchDelay time.Duration,
) DaemonSet {
	return &daemonSet{
		DaemonSet: fields,

		dsStore:            dsStore,
		store:              store,
		logger:             logger,
		applicator:         applicator,
		watcher:            watcher,
		scheduler:          scheduler.NewApplicatorScheduler(applicator),
		healthChecker:      healthChecker,
		healthWatchDelay:   healthWatchDelay,
		currentReplication: nil,
		rateLimitInterval:  rateLimitInterval,
		cachedPodMatch:     cachedPodMatch,
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

func (ds *daemonSet) MetricNames(suffix string) []string {
	prefix := "daemonset"
	middles := []string{
		ds.PodID().String() + "." + ds.ClusterName().String(),
		ds.ID().String(),
	}
	var ret []string
	for _, middle := range middles {
		ret = append(ret, fmt.Sprintf("%s.%s.%s", prefix, middle, suffix))
	}
	return ret
}

func (ds *daemonSet) WatchDesires(
	quitCh <-chan struct{},
	updatedCh <-chan *fields.DaemonSet,
	deletedCh <-chan *fields.DaemonSet,
) <-chan error {
	errCh := make(chan error)
	nodesChangedCh := ds.watcher.WatchMatchDiff(ds.NodeSelector, labels.NODE, quitCh)
	// Do something whenever something is changed
	go func() {
		var err error
		defer close(errCh)
		defer ds.cancelReplication()

		// Make a timer and stop it so the receieve from channel does not occur
		// until a reset happens
		timer := time.NewTimer(time.Duration(0))
		timer.Stop()

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
				ds.logger.Errorf("An error has occurred in the daemon set, retrying if no changes are made in %d. %v", retryInterval, err)
				select {
				case errCh <- err:
					// Retry the replication in the RetryInterval's duration
					timer.Reset(retryInterval)
					// This is required in case the user disables the daemon set
					// so that the timer would be stopped after
					err = nil
				case <-quitCh:
					return
				}
			} else {
				// If err == nil, stop the timer because there is no need to retry
				timer.Stop()
			}

			// Precondition: err == nil
			// err should be assigned a value in this select statement unless
			// it returns or the daemon set is disabled
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

				if reportErr := ds.reportEligible(); reportErr != nil {
					// An error in sending the metrics shouldn't stop us from doing updates.
					// Report it, and move on.
					ds.logger.WithError(reportErr).Warnf("Error reporting number of eligible nodes")
				}

				if ds.Disabled {
					ds.cancelReplication()
					continue
				}
				err = ds.removePods()
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
					return
				}
				// Deleting a daemon sets has no effect
				ds.logger.WithFields(logrus.Fields{"id": deleteDS, "node_selector": ds.NodeSelector.String()}).Infof("Daemon Set Deletion is disabled and has no effect. You may want to clean this up manually.")
				return

			case labeledChanges, ok := <-nodesChangedCh:
				if !ok {
					// channel closed
					return
				}
				if reportErr := ds.reportEligible(); reportErr != nil {
					// An error in sending the metrics shouldn't stop us from doing updates.
					// Report it, and move on.
					ds.logger.WithError(reportErr).Warnf("Error reporting number of eligible nodes")
				}
				if ds.Disabled {
					continue
				}
				err = ds.handleNodeChanges(labeledChanges)
				if err != nil {
					continue
				}

			case <-timer.C:
				// Account for any operations that could have failed and retry the replication
				err = ds.removePods()
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

func (ds *daemonSet) reportEligible() error {
	eligible, err := ds.EligibleNodes()
	if err != nil {
		return util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}
	for _, name := range ds.MetricNames("total") {
		gauge := metrics.GetOrRegisterGauge(name, p2metrics.Registry)
		gauge.Update(int64(len(eligible)))
	}
	return nil
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
	ds.logger.NoFields().Infof("Need to label %d nodes", len(toScheduleSorted))

	for _, node := range toScheduleSorted {
		err := ds.labelPod(node)
		if err != nil {
			return util.Errorf("Error labeling node: %v", err)
		}
	}

	if len(currentNodes) > 0 || len(toScheduleSorted) > 0 {
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

	if len(eligible) == 0 {
		return util.Errorf("No nodes eligible; daemon set refuses to unschedule everything.")
	}

	// Get the difference in nodes that we need to unschedule on and then sort them
	// for deterministic ordering
	toUnscheduleSorted := types.NewNodeSet(currentNodes...).Difference(types.NewNodeSet(eligible...)).ListNodes()
	ds.logger.NoFields().Infof("Need to unschedule %d nodes, remaining on %d nodes", len(toUnscheduleSorted), len(eligible))

	ds.cancelReplication()

	for _, node := range toUnscheduleSorted {
		err := ds.unschedule(node)
		if err != nil {
			return util.Errorf("Error unscheduling node: %v", err)
		}
	}

	ds.logger.Infof("Need to schedule %v nodes", len(currentNodes))
	if len(currentNodes)-len(toUnscheduleSorted) > 0 {
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
	ds.logger.NoFields().Infof("Deleted: Need to unschedule %d nodes", len(toUnscheduleSorted))

	ds.cancelReplication()

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
	// <consul.INTENT_TREE>/<node>/<ds.Manifest.ID()>
	_, err := ds.store.DeletePod(consul.INTENT_TREE, node, ds.Manifest.ID())
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
	// We must cancel the replication because if we try to call
	// InitializeReplicationWithCheck, we will get an error
	ds.cancelReplication()

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
		ds.store,
		ds.applicator,
		*ds.healthChecker,
		health.HealthState(health.Passing),
		lockMessage,
		ds.Timeout,
		ds.healthWatchDelay,
	)
	if err != nil {
		ds.logger.Errorf("Could not initialize replicator: %s", err)
		return err
	}

	ds.logger.Info("New replicator was made")

	// Replication locks are designed to make sure that two replications to
	// the same nodes cannot occur at the same time. The granularity is
	// pod-wide as an optimization for consul performance (only need to
	// lock a single key) with limited downside when human operators are
	// executing deploys, because the likelihood of a lock collision is
	// low. With daemon sets, locking is not necessary because the node
	// sets should not overlap when they are managed properly. Even when
	// there is a node overlap between two daemon sets, a simple mutual
	// exclusion lock around replication will not prevent the pod manifest
	// on an overlapped node from thrashing. Therefore, it makes sense for
	// daemon sets to ignore this locking mechanism and always try to
	// converge nodes to the specified manifest
	currentReplication, errCh, err := repl.InitializeDaemonSetReplication(
		replication.DefaultConcurrentReality,
		ds.rateLimitInterval,
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

	// Set a new replication
	ds.currentReplication = currentReplication

	go currentReplication.Enact()

	ds.logger.Info("Replication enacted")

	return nil
}

// It is also okay to call this multiple times because it keeps track of when
// it has been cancelled by checking whether ds.currentReplication == nil
func (ds *daemonSet) cancelReplication() {
	if ds.currentReplication != nil {
		ds.currentReplication.Cancel()
		ds.currentReplication.WaitForReplication()
		ds.logger.Info("Replication cancelled")
		ds.currentReplication = nil
	}
}

func (ds *daemonSet) CurrentPods() (types.PodLocations, error) {
	// Changing DaemonSet.ID is not permitted, so as long as there is no uuid
	// collision, this will always get the current pod path that this daemon set
	// had scheduled on
	selector := klabels.Everything().Add(DSIDLabel, klabels.EqualsOperator, []string{ds.ID().String()})

	podMatches, err := ds.applicator.GetMatches(selector, labels.POD, ds.cachedPodMatch)
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
