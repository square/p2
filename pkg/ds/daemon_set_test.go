package ds

import (
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/consultest"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/store/consul/dsstore/dsstoretest"
	"github.com/square/p2/pkg/types"

	. "github.com/anthonybishopric/gotcha"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	fake_checker "github.com/square/p2/pkg/health/checker/test"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	testDSRetryInterval = time.Duration(1000 * time.Millisecond)
)

func waitForNodes(
	t *testing.T,
	ds DaemonSet,
	desired int,
	desiresErrCh <-chan error,
	dsChangesErrCh <-chan error,
) int {
	timeout := time.After(10 * time.Second)
	podLocations, err := ds.CurrentPods()
	Assert(t).IsNil(err, "expected no error getting pod locations")
	timedOut := false

	// This for loop runs until you either time out or len(podLocations) == desired
	// then return the length of whatever ds.CurrentNodes() is
	for len(podLocations) != desired && !timedOut {
		select {
		case <-time.Tick(100 * time.Millisecond):
			// Also check for errors
			var err error
			podLocations, err = ds.CurrentPods()
			Assert(t).IsNil(err, "expected no error getting pod locations nodes")

			select {
			case err = <-desiresErrCh:
				Assert(t).IsNil(err, "expected no error watches desires")
			case err = <-dsChangesErrCh:
				Assert(t).IsNil(err, "expected no error watching for daemon set changes")
			default:
			}

		case <-timeout:
			timedOut = true
		}
	}
	return len(podLocations)
}

// Watches for changes to daemon sets and sends update and delete signals
// since these are unit tests and have little daemon sets, we will watch
// the entire tree for each daemon set for now
func watchDSChanges(
	ds *daemonSet,
	dsStore dsstore.Store,
	quitCh <-chan struct{},
	updatedCh chan<- *ds_fields.DaemonSet,
	deletedCh chan<- *ds_fields.DaemonSet,
) <-chan error {
	errCh := make(chan error)
	changesCh := dsStore.Watch(quitCh)

	go func() {
		defer close(errCh)

		for {
			var watched dsstore.WatchedDaemonSets

			// Get some changes
			select {
			case watched = <-changesCh:
			case <-quitCh:
				return
			}

			if watched.Err != nil {
				errCh <- util.Errorf("Error occured when watching daemon set changes: %v", watched.Err)
			}

			// Signal daemon set when changes have been made,
			// creations are handled when WatchDesires is called, so ignore them here
			for _, changedDS := range watched.Updated {
				if ds.ID() == changedDS.ID {
					ds.logger.NoFields().Infof("Watched daemon set was updated: %v", *changedDS)
					updatedCh <- changedDS
				}
			}
			for _, changedDS := range watched.Deleted {
				if ds.ID() == changedDS.ID {
					ds.logger.NoFields().Infof("Watched daemon set was deleted: %v", *changedDS)
					deletedCh <- changedDS
				}
			}
		}
	}()
	return errCh
}

// TestSchedule checks consecutive scheduling and unscheduling for:
//	- creation of a daemon set
// 	- different node selectors
//	- changes to nodes allocations
// 	- mutations to a daemon set
//	- deleting a daemon set
func TestSchedule(t *testing.T) {
	retryInterval = testFarmRetryInterval

	//
	// Setup fixture and schedule a pod
	//
	dsStore := dsstoretest.NewFake()

	podID := types.PodID("testPod")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add("nodeQuality", klabels.EqualsOperator, []string{"good"})
	timeout := replication.NoTimeout

	dsData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID, timeout)
	Assert(t).IsNil(err, "expected no error creating request")

	consulStore := consultest.NewFakePodStore(make(map[consultest.FakePodStoreKey]manifest.Manifest), make(map[string]consul.WatchResult))
	applicator := labels.NewFakeApplicator()

	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	var allNodes []types.NodeName
	allNodes = append(allNodes, "node1", "node2", "nodeOk")
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		allNodes = append(allNodes, types.NodeName(nodeName))
	}
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("bad_node%v", i)
		allNodes = append(allNodes, types.NodeName(nodeName))
	}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	ds := New(
		dsData,
		dsStore,
		consulStore,
		applicator,
		applicator,
		logging.DefaultLogger,
		&happyHealthChecker,
		0,
		false,
	).(*daemonSet)

	labeled := labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), 0, "expected no pods to have been labeled")

	err = waitForPodsInIntent(consulStore, 0)
	Assert(t).IsNil(err, "Unexpected number of pods labeled")

	err = applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	//
	// Adds a watch that will automatically send a signal when a change was made
	// to the daemon set
	//
	quitCh := make(chan struct{})
	updatedCh := make(chan *ds_fields.DaemonSet)
	deletedCh := make(chan *ds_fields.DaemonSet)
	defer close(quitCh)
	defer close(updatedCh)
	defer close(deletedCh)
	desiresErrCh := ds.WatchDesires(quitCh, updatedCh, deletedCh)
	dsChangesErrCh := watchDSChanges(ds, dsStore, quitCh, updatedCh, deletedCh)

	//
	// Verify that the pod has been labeled
	//
	numNodes := waitForNodes(t, ds, 1, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule")

	labeled = labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), 1, "expected a node to have been labeled")
	Assert(t).AreEqual(labeled[0].ID, "node2/testPod", "expected node labeled with the daemon set's id")

	// Verify that the labeled pod is correct
	err = waitForSpecificPod(consulStore, "node2", types.PodID("testPod"))
	Assert(t).IsNil(err, "Unexpected pod labeled")

	//
	// Add 10 good nodes and 10 bad nodes then verify
	//
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		err := applicator.SetLabel(labels.NODE, nodeName, "nodeQuality", "good")
		Assert(t).IsNil(err, "expected no error labeling node")
	}

	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("bad_node%v", i)
		err := applicator.SetLabel(labels.NODE, nodeName, "nodeQuality", "bad")
		Assert(t).IsNil(err, "expected no error labeling node")
	}

	// The node watch should automatically notice a change
	numNodes = waitForNodes(t, ds, 11, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, 11, "took too long to schedule")

	labeled = labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), 11, "expected a lot of nodes to have been labeled")

	//
	// Add a node with the labels nodeQuality=good and cherry=pick
	//
	err = applicator.SetLabel(labels.NODE, "nodeOk", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling nodeOk")
	err = applicator.SetLabel(labels.NODE, "nodeOk", "cherry", "pick")
	Assert(t).IsNil(err, "expected no error labeling nodeOk")
	fmt.Println(applicator.GetLabels(labels.NODE, "nodeOk"))

	numNodes = waitForNodes(t, ds, 12, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, 12, "took too long to schedule")

	// Schedule only a node that is both nodeQuality=good and cherry=pick
	mutator := func(dsToChange ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToChange.NodeSelector = klabels.Everything().
			Add("nodeQuality", klabels.EqualsOperator, []string{"good"}).
			Add("cherry", klabels.EqualsOperator, []string{"pick"})
		return dsToChange, nil
	}
	_, err = dsStore.MutateDS(ds.ID(), mutator)
	Assert(t).IsNil(err, "Unxpected error trying to mutate daemon set")

	numNodes = waitForNodes(t, ds, 1, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule")

	// Verify that the labeled pod is correct
	err = waitForSpecificPod(consulStore, "nodeOk", types.PodID("testPod"))
	Assert(t).IsNil(err, "Unexpected pod labeled")

	//
	// Disabling the daemon set and making a change should not do anything
	//
	mutator = func(dsToChange ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToChange.Disabled = true
		dsToChange.NodeSelector = klabels.Everything().
			Add("nodeQuality", klabels.EqualsOperator, []string{"good"})
		return dsToChange, nil
	}
	_, err = dsStore.MutateDS(ds.ID(), mutator)
	Assert(t).IsNil(err, "Unxpected error trying to mutate daemon set")

	numNodes = waitForNodes(t, ds, 1, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, 1, "took too long to unschedule")

	//
	// Now re-enable it and try to schedule everything
	//
	mutator = func(dsToChange ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToChange.NodeSelector = klabels.Everything()
		dsToChange.Disabled = false
		return dsToChange, nil
	}
	_, err = dsStore.MutateDS(ds.ID(), mutator)
	Assert(t).IsNil(err, "Unxpected error trying to mutate daemon set")

	// 11 good nodes 11 bad nodes, and 1 good cherry picked node = 23 nodes
	expectedNodes := 23
	numNodes = waitForNodes(t, ds, expectedNodes, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, expectedNodes, "took too long to schedule")

	//
	// Deleting the daemon set should unschedule all of its nodes
	//
	beforeDeletePods, _, err := scheduledPods(consulStore)
	Assert(t).IsNil(err, "Unable to list intent/")
	ds.logger.NoFields().Info("Deleting daemon set...")
	err = dsStore.Delete(ds.ID())
	if err != nil {
		t.Fatalf("Unable to delete daemon set: %v", err)
	}
	// Behaivour change: Deleting a daemon set will no longer unschedule its pods (for now)
	numNodes = waitForNodes(t, ds, 12, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, expectedNodes, "Unexpected number of nodes labeled")

	labeled = labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), expectedNodes, "Expected no nodes to be unlabeled")

	err = waitForPodsInIntent(consulStore, len(beforeDeletePods))
	Assert(t).IsNil(err, "Unexpected number of pods labeled")
}

func TestPublishToReplication(t *testing.T) {
	retryInterval = testFarmRetryInterval

	//
	// Setup fixture and schedule a pod
	//
	dsStore := dsstoretest.NewFake()

	podID := types.PodID("testPod")
	minHealth := 1
	clusterName := ds_fields.ClusterName("some_name")

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add("nodeQuality", klabels.EqualsOperator, []string{"good"})
	timeout := replication.NoTimeout

	dsData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID, timeout)
	Assert(t).IsNil(err, "expected no error creating request")

	consulStore := consultest.NewFakePodStore(make(map[consultest.FakePodStoreKey]manifest.Manifest), make(map[string]consul.WatchResult))
	applicator := labels.NewFakeApplicator()

	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	var allNodes []types.NodeName
	allNodes = append(allNodes, "node1", "node2")
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		allNodes = append(allNodes, types.NodeName(nodeName))
	}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	ds := New(
		dsData,
		dsStore,
		consulStore,
		applicator,
		applicator,
		logging.DefaultLogger,
		&happyHealthChecker,
		0,
		false,
	).(*daemonSet)

	labeled := labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), 0, "expected no pods to have been labeled")
	err = waitForPodsInIntent(consulStore, 0)
	Assert(t).IsNil(err, "Unexpected number of pods labeled")

	err = applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node1")

	//
	// Adds a watch that will automatically send a signal when a change was made
	// to the daemon set
	//
	quitCh := make(chan struct{})
	updatedCh := make(chan *ds_fields.DaemonSet)
	deletedCh := make(chan *ds_fields.DaemonSet)
	defer close(quitCh)
	defer close(updatedCh)
	defer close(deletedCh)
	desiresErrCh := ds.WatchDesires(quitCh, updatedCh, deletedCh)
	dsChangesErrCh := watchDSChanges(ds, dsStore, quitCh, updatedCh, deletedCh)

	//
	// Verify that 2 pods have been labeled
	//
	numNodes := waitForNodes(t, ds, 2, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, 2, "took too long to schedule")

	labeled = labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), 2, "expected a node to have been labeled")

	// Mutate the daemon set so that no nodes are eligible.
	// The mutation shouldn't produce an error, but the daemon set should when attempting to meet.
	mutator := func(dsToChange ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToChange.NodeSelector = klabels.Everything().
			Add("nodeQuality", klabels.EqualsOperator, []string{"bad"})
		return dsToChange, nil
	}
	_, err = dsStore.MutateDS(ds.ID(), mutator)
	Assert(t).IsNil(err, "Unxpected error trying to mutate daemon set")

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("Unexpectedly no error when no nodes are eligible")
	case err := <-desiresErrCh:
		Assert(t).IsNotNil(err, "Unexpectedly nil error when no nodes are eligible")
	}
	numNodes = waitForNodes(t, ds, 2, desiresErrCh, dsChangesErrCh)
	Assert(t).AreEqual(numNodes, 2, "unexpectedly unlabeled")
}

// Polls for the store to have the same number of pods as the argument
func waitForPodsInIntent(consulStore *consultest.FakePodStore, numPodsExpected int) error {
	condition := func() error {
		manifestResults, _, err := consulStore.AllPods(consul.INTENT_TREE)
		if err != nil {
			return util.Errorf("Unable to get all pods from pod store: %v", err)
		}
		if len(manifestResults) != numPodsExpected {
			fmt.Printf("%d : %v\n\n", len(manifestResults), manifestResults)
			return util.Errorf(
				"Expected no manifests to be scheduled, got '%v' manifests, expected '%v'",
				len(manifestResults),
				numPodsExpected,
			)
		}
		return nil
	}
	return waitForCondition(condition)
}

// Polls for the store to have a pod with the same pod id and node name
func waitForSpecificPod(consulStore *consultest.FakePodStore, nodeName types.NodeName, podID types.PodID) error {
	condition := func() error {
		manifestResults, _, err := consulStore.AllPods(consul.INTENT_TREE)
		if err != nil {
			return util.Errorf("Unable to get all pods from pod store: %v", err)
		}
		if len(manifestResults) == 0 {
			return util.Errorf("expected a manifest in the intent tree")
		}
		if manifestResults[0].PodLocation.Node != nodeName {
			return util.Errorf("expected manifest labeled on the right node")
		}
		if manifestResults[0].PodLocation.PodID != podID {
			return util.Errorf("expected manifest labeled with correct pod ID")
		}
		if manifestResults[0].Manifest.ID() != podID {
			return util.Errorf("expected manifest with correct ID")
		}
		return nil
	}
	return waitForCondition(condition)
}

func labeledPods(t *testing.T, ds *daemonSet) []labels.Labeled {
	selector := klabels.Everything().Add(DSIDLabel, klabels.EqualsOperator, []string{ds.ID().String()})
	labeled, err := ds.applicator.GetMatches(selector, labels.POD, false)
	Assert(t).IsNil(err, "expected no error matching pods")
	return labeled
}

func scheduledPods(consulStore *consultest.FakePodStore) ([]consul.ManifestResult, time.Duration, error) {
	return consulStore.AllPods(consul.INTENT_TREE)
}
