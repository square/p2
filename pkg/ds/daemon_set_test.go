// +build !race

package ds

import (
	"context"
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
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/daemonsetstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"

	. "github.com/anthonybishopric/gotcha"
	"github.com/pborman/uuid"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	fake_checker "github.com/square/p2/pkg/health/checker/test"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	testDSRetryInterval   = time.Duration(1000 * time.Millisecond)
	replicationTimeout    = replication.NoTimeout
	testFarmRetryInterval = time.Duration(1000 * time.Millisecond)
)

// Polls for a condition to happen. This should only be run with the -timeout
// flag to go test set to avoid tests hanging forever if something goes wrong
func waitForCondition(condition func() error) error {
	for err := condition(); err != nil; err = condition() {
		fmt.Printf("condition failed, will retry: %s\n", err)
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func waitForNodes(
	ds DaemonSet,
	desired int,
	desiresErrCh <-chan error,
	dsChangesErrCh <-chan error,
) error {
	timeout := time.After(10 * time.Second)
	podLocations, err := ds.CurrentPods()
	if err != nil {
		return util.Errorf("error getting pod locations: %s", err)
	}

	timedOut := false

	// This for loop runs until you either time out or len(podLocations) == desired
	// then return the length of whatever ds.CurrentNodes() is
	for len(podLocations) != desired && !timedOut {
		select {
		case <-time.After(100 * time.Millisecond):
			podLocations, err = ds.CurrentPods()
			if err != nil {
				return util.Errorf("error getting pod locations: %s", err)
			}

			select {
			case err = <-desiresErrCh:
				if err != nil {
					return util.Errorf("error watching desires: %s", err)
				}
			case err = <-dsChangesErrCh:
				if err != nil {
					return util.Errorf("error watching for daemon set changes: %s", err)
				}
			default:
			}

		case <-timeout:
			timedOut = true
		}
	}
	if len(podLocations) != desired {
		return util.Errorf("expected %d nodes but got %d after timeout", desired, len(podLocations))
	}

	return nil
}

// Watches for changes to daemon sets and sends update and delete signals
// since these are unit tests and have little daemon sets, we will watch
// the entire tree for each daemon set for now
func watchDSChanges(
	ctx context.Context,
	ds *daemonSet,
	dsStore DaemonSetStore,
	updatedCh chan<- ds_fields.DaemonSet,
	deletedCh chan<- ds_fields.DaemonSet,
) <-chan error {
	errCh := make(chan error)
	quitCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(quitCh)
	}()
	changesCh := dsStore.Watch(quitCh)

	go func() {
		defer close(errCh)

		for {
			var watched dsstore.WatchedDaemonSets

			// Get some changes
			select {
			case watched = <-changesCh:
			case <-ctx.Done():
				return
			}

			if watched.Err != nil {
				errCh <- util.Errorf("Error occured when watching daemon set changes: %v", watched.Err)
			}

			dsID := ds.ID()
			// Signal daemon set when changes have been made,
			// creations are handled when WatchDesires is called, so ignore them here
			for _, changedDS := range watched.Updated {
				if dsID == changedDS.ID {
					ds.logger.NoFields().Infof("Watched daemon set was updated: %v", changedDS)
					updatedCh <- *changedDS
				}
			}
			for _, changedDS := range watched.Deleted {
				if dsID == changedDS.ID {
					ds.logger.NoFields().Infof("Watched daemon set was deleted: %v", changedDS)
					deletedCh <- *changedDS
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
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	//
	// Setup fixture and schedule a pod
	//
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)

	podID := types.PodID("testPod")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add("nodeQuality", klabels.EqualsOperator, []string{"good"})
	timeout := replication.NoTimeout

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	dsData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, nodeSelector, podID, timeout)
	Assert(t).IsNil(err, "expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")

	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)

	// seed the applicator so the "no labels" failsafe isn't triggered
	err = applicator.SetLabel(labels.POD, "some_untouched_pod", "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

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

	rawStatusStore := statusstore.NewConsul(fixture.Client)
	statusStore := daemonsetstatus.NewConsul(rawStatusStore, "test_schedule")
	ds := New(
		dsData,
		dsStore,
		consulStore,
		fixture.Client.KV(),
		applicator,
		applicator,
		1*time.Nanosecond,
		logging.DefaultLogger,
		&happyHealthChecker,
		0,
		false,
		0,
		testFarmRetryInterval,
		nullUnlocker{},
		statusStore,
		DefaultStatusWritingInterval,
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
	updatedCh := make(chan ds_fields.DaemonSet)
	deletedCh := make(chan ds_fields.DaemonSet)
	desiresErrCh := ds.WatchDesires(ctx, updatedCh, deletedCh)
	dsChangesErrCh := watchDSChanges(ctx, ds, dsStore, updatedCh, deletedCh)

	// We need to be careful when shutting down in order to avoid data
	// races, since updatedCh and deletedCh are shared between two
	// functions (WatchDesires() and watchDSChanges()). We need to make
	// sure that we don't close updatedCh or deletedCh until their
	// respective output channels have closed which indicates that they
	// won't try to send any more values
	defer func(cancel context.CancelFunc) {
		cancel()
		desiresErrChClosed := false
		dsChangesErrChClosed := false
		for !dsChangesErrChClosed || !desiresErrChClosed {
			select {
			case _, ok := <-dsChangesErrCh:
				if !ok {
					dsChangesErrChClosed = true
				}
			case _, ok := <-desiresErrCh:
				if !ok {
					desiresErrChClosed = true
				}
			case <-deletedCh:
			case <-updatedCh:
			case <-time.After(1 * time.Second):
				t.Fatal("watchDSChanges or WatchDesires did not exit promptly after closing quitCh")
			}
		}
		close(updatedCh)
		close(deletedCh)
	}(cancel)

	//
	// Verify that the pod has been labeled
	//
	err = waitForNodes(ds, 1, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "took too long to schedule")

	err = waitForPodsInIntent(consulStore, 1)
	if err != nil {
		t.Fatal(err)
	}
	labeled = labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), 1, "expected a node to have been labeled")
	Assert(t).AreEqual(labeled[0].ID, "node2/testPod", "expected node labeled with the daemon set's id")

	// Verify that the labeled pod is correct
	err = waitForSpecificPod(consulStore, "node2", types.PodID("testPod"))
	Assert(t).IsNil(err, "Unexpected pod labeled")

	//
	// Add 10 good nodes and 10 bad nodes then verify
	//

	txnCtx, cancel := transaction.New(context.Background())
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		err := applicator.SetLabelTxn(txnCtx, labels.NODE, nodeName, "nodeQuality", "good")
		Assert(t).IsNil(err, "expected no error labeling node")
	}

	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("bad_node%v", i)
		err := applicator.SetLabelTxn(txnCtx, labels.NODE, nodeName, "nodeQuality", "bad")
		Assert(t).IsNil(err, "expected no error labeling node")
	}

	err = transaction.MustCommit(txnCtx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// The node watch should automatically notice a change
	err = waitForNodes(ds, 11, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "took too long to schedule")

	labeled = labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), 11, "expected a lot of nodes to have been labeled")

	err = waitForPodsInIntent(consulStore, 11)
	if err != nil {
		t.Fatal(err)
	}

	//
	// Add a node with the labels nodeQuality=good and cherry=pick
	//
	err = applicator.SetLabel(labels.NODE, "nodeOk", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling nodeOk")
	err = applicator.SetLabel(labels.NODE, "nodeOk", "cherry", "pick")
	Assert(t).IsNil(err, "expected no error labeling nodeOk")

	err = waitForNodes(ds, 12, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "took too long to schedule")

	// Schedule only a node that is both nodeQuality=good and cherry=pick
	mutator := func(dsToChange ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToChange.NodeSelector = klabels.Everything().
			Add("nodeQuality", klabels.EqualsOperator, []string{"good"}).
			Add("cherry", klabels.EqualsOperator, []string{"pick"})
		return dsToChange, nil
	}
	_, err = dsStore.MutateDS(ds.ID(), mutator)
	Assert(t).IsNil(err, "Unxpected error trying to mutate daemon set")

	err = waitForNodes(ds, 1, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "took too long to schedule")

	err = waitForPodsInIntent(consulStore, 1)
	if err != nil {
		t.Fatal(err)
	}

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

	err = waitForNodes(ds, 1, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "took too long to unschedule")

	err = waitForPodsInIntent(consulStore, 1)
	if err != nil {
		t.Fatal(err)
	}

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
	err = waitForNodes(ds, expectedNodes, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "took too long to schedule")

	err = waitForPodsInIntent(consulStore, expectedNodes)
	if err != nil {
		t.Fatal(err)
	}

	//
	// Deleting the daemon set should not unschedule any nodes
	//
	ds.logger.NoFields().Info("Deleting daemon set...")
	err = dsStore.Delete(ds.ID())
	if err != nil {
		t.Fatalf("Unable to delete daemon set: %v", err)
	}
	// behavior change: Deleting a daemon set will no longer unschedule its pods (for now)
	err = waitForNodes(ds, 23, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "Unexpected number of nodes labeled")

	labeled = labeledPods(t, ds)
	Assert(t).AreEqual(len(labeled), expectedNodes, "Expected no nodes to be unlabeled")

	err = waitForPodsInIntent(consulStore, expectedNodes)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPublishToReplication(t *testing.T) {
	//
	// Setup fixture and schedule a pod
	//
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)

	podID := types.PodID("testPod")
	minHealth := 1
	clusterName := ds_fields.ClusterName("some_name")

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add("nodeQuality", klabels.EqualsOperator, []string{"good"})
	timeout := replication.NoTimeout

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	dsData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, nodeSelector, podID, timeout)
	Assert(t).IsNil(err, "expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")

	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)

	// seed applicator with unrelated labels so we don't trigger "no labels" failsave
	err = applicator.SetLabel(labels.POD, "some_unrelated_pod", "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

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

	rawStatusStore := statusstore.NewConsul(fixture.Client)
	statusStore := daemonsetstatus.NewConsul(rawStatusStore, "test_publish_to_replication")
	ds := New(
		dsData,
		dsStore,
		consulStore,
		fixture.Client.KV(),
		applicator,
		applicator,
		1*time.Nanosecond,
		logging.DefaultLogger,
		&happyHealthChecker,
		0,
		false,
		0,
		testFarmRetryInterval,
		nullUnlocker{},
		statusStore,
		DefaultStatusWritingInterval,
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
	updatedCh := make(chan ds_fields.DaemonSet)
	deletedCh := make(chan ds_fields.DaemonSet)
	desiresErrCh := ds.WatchDesires(ctx, updatedCh, deletedCh)
	dsChangesErrCh := watchDSChanges(ctx, ds, dsStore, updatedCh, deletedCh)

	// We need to be careful when shutting down in order to avoid data
	// races, since updatedCh and deletedCh are shared between two
	// functions (WatchDesires() and watchDSChanges()). We need to make
	// sure that we don't close updatedCh or deletedCh until their
	// respective output channels have closed which indicates that they
	// won't try to send any more values
	defer func() {
		cancel()
		desiresErrChClosed := false
		dsChangesErrChClosed := false
		for !dsChangesErrChClosed || !desiresErrChClosed {
			select {
			case _, ok := <-dsChangesErrCh:
				if !ok {
					dsChangesErrChClosed = true
				}
			case _, ok := <-desiresErrCh:
				if !ok {
					desiresErrChClosed = true
				}
			case <-deletedCh:
			case <-updatedCh:
			case <-time.After(1 * time.Second):
				t.Fatal("watchDSChanges or WatchDesires did not exit promptly after canceling context")
			}
		}
		close(updatedCh)
		close(deletedCh)
	}()

	//
	// Verify that 2 pods have been labeled
	//
	err = waitForNodes(ds, 2, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "took too long to schedule")

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
	err = waitForNodes(ds, 2, desiresErrCh, dsChangesErrCh)
	Assert(t).IsNil(err, "unexpectedly unlabeled")
}

// nullUnlocker satisfies consul.TxnUnlocker to avoid npe in tests but it doesn't actually do anything
type nullUnlocker struct {
}

func (nullUnlocker) UnlockTxn(ctx context.Context) error {
	return nil
}

func (nullUnlocker) CheckLockedTxn(ctx context.Context) error {
	return nil
}

func fakeReplication(completedCount int32, inProgress bool) nullReplication {
	return nullReplication{
		completedCount: completedCount,
		inProgress:     inProgress,
	}
}

type nullReplication struct {
	completedCount int32
	inProgress     bool
}

func (nullReplication) Enact() {
	panic("Enact() not implemented on nullReplication")
}

func (nullReplication) Cancel() {
	return
}

func (nullReplication) WaitForReplication() {
	return
}

func (n nullReplication) CompletedCount() int32 {
	return n.completedCount
}

func (n nullReplication) InProgress() bool {
	return n.inProgress
}

func (n nullReplication) SetManifest(manifest.Manifest) {
	panic("SetManifest() not implemented on nullReplication")
}

func TestWriteNewestStatus(t *testing.T) {
	type writeStatusTestCase struct {
		lastStatus         daemonsetstatus.Status
		expectedStatus     daemonsetstatus.Status
		nodeCompletedCount int32
		inProgress         bool
	}

	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	rawStatusStore := statusstore.NewConsul(fixture.Client)

	statusStore := daemonsetstatus.NewConsul(rawStatusStore, "test_write_newest_status")

	manifest := testManifest("some_pod")
	manifestSHA, err := manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []writeStatusTestCase{
		// no previous status
		{
			expectedStatus: daemonsetstatus.Status{
				ManifestSHA:           manifestSHA,
				NodesDeployed:         38,
				ReplicationInProgress: true,
			},
			nodeCompletedCount: 38,
			inProgress:         true,
		},
		// same manifest sha, more nodes, so we expect the node count to be updated
		{
			lastStatus: daemonsetstatus.Status{
				ManifestSHA:   manifestSHA,
				NodesDeployed: 14,
			},
			expectedStatus: daemonsetstatus.Status{
				ManifestSHA:           manifestSHA,
				NodesDeployed:         38,
				ReplicationInProgress: true,
			},
			nodeCompletedCount: 38,
			inProgress:         true,
		},
		// same manifest sha, fewer nodes, so we don't expect the node count to be updated
		{
			lastStatus: daemonsetstatus.Status{
				ManifestSHA:   manifestSHA,
				NodesDeployed: 82,
			},
			expectedStatus: daemonsetstatus.Status{
				ManifestSHA:           manifestSHA,
				NodesDeployed:         82,
				ReplicationInProgress: true,
			},
			nodeCompletedCount: 38,
			inProgress:         true,
		},
		// different manifest sha, fewer nodes, so we expect the node count to be updated
		{
			lastStatus: daemonsetstatus.Status{
				ManifestSHA:   "some_other_sha",
				NodesDeployed: 82,
			},
			expectedStatus: daemonsetstatus.Status{
				ManifestSHA:           manifestSHA,
				NodesDeployed:         38,
				ReplicationInProgress: true,
			},
			nodeCompletedCount: 38,
			inProgress:         true,
		},
		// different manifest sha but no replication in progress, so node count should be reset
		{
			lastStatus: daemonsetstatus.Status{
				ManifestSHA:   "some_other_sha",
				NodesDeployed: 82,
			},
			expectedStatus: daemonsetstatus.Status{
				ManifestSHA:           manifestSHA,
				NodesDeployed:         0,
				ReplicationInProgress: false,
			},
			nodeCompletedCount: -1,
			inProgress:         false,
		},
		// same manifest sha, more nodes, no replication in progress
		{
			lastStatus: daemonsetstatus.Status{
				ManifestSHA:   manifestSHA,
				NodesDeployed: 14,
			},
			expectedStatus: daemonsetstatus.Status{
				ManifestSHA:           manifestSHA,
				NodesDeployed:         38,
				ReplicationInProgress: false,
			},
			nodeCompletedCount: 38,
			inProgress:         false,
		},
	}

	ds := daemonSet{
		DaemonSet: ds_fields.DaemonSet{
			Manifest: manifest,
		},
		unlocker:    nullUnlocker{},
		statusStore: statusStore,
		logger:      logging.TestLogger(),
		txner:       fixture.Client.KV(),
	}

	for i, testCase := range testCases {
		ds.DaemonSet.ID = ds_fields.ID(uuid.New())

		ds.setDSReplication(nil)
		if testCase.nodeCompletedCount >= 0 {
			ds.setDSReplication(&dsReplication{
				replication: fakeReplication(testCase.nodeCompletedCount, testCase.inProgress),
			})
		}
		if testCase.lastStatus.ManifestSHA != "" {
			writeCtx, writeCancel := transaction.New(context.Background())
			defer writeCancel()

			err := statusStore.CASTxn(writeCtx, ds.ID(), 0, testCase.lastStatus)
			if err != nil {
				t.Fatal(err)
			}

			err = transaction.MustCommit(writeCtx, fixture.Client.KV())
			if err != nil {
				t.Fatal(err)
			}
		}

		ds.writeNewestStatus(context.Background(), testCase.lastStatus)

		newStatus, _, err := ds.statusStore.Get(ds.ID())
		if err != nil {
			t.Fatal(err)
		}

		if newStatus != testCase.expectedStatus {
			t.Errorf("test case %d: expected %+v got %+v", i, testCase.expectedStatus, newStatus)
		}
	}
}

type testStore interface {
	AllPods(podPrefix consul.PodPrefix) ([]consul.ManifestResult, time.Duration, error)
}

// Polls for the store to have the same number of pods as the argument
func waitForPodsInIntent(consulStore store, numPodsExpected int) error {
	condition := func() error {
		manifestResults, _, err := consulStore.(testStore).AllPods(consul.INTENT_TREE)
		if err != nil {
			return util.Errorf("Unable to get all pods from pod store: %v", err)
		}
		if len(manifestResults) != numPodsExpected {
			return util.Errorf(
				"Expected %d manifests to be scheduled, got %d",
				numPodsExpected,
				len(manifestResults),
			)
		}
		return nil
	}
	return waitForCondition(condition)
}

// Polls for the store to have a pod with the same pod id and node name
func waitForSpecificPod(consulStore store, nodeName types.NodeName, podID types.PodID) error {
	condition := func() error {
		manifestResults, _, err := consulStore.(testStore).AllPods(consul.INTENT_TREE)
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
	labeled, err := ds.applicator.GetMatches(selector, labels.POD)
	Assert(t).IsNil(err, "expected no error matching pods")
	return labeled
}

func scheduledPods(consulStore *consultest.FakePodStore) ([]consul.ManifestResult, time.Duration, error) {
	return consulStore.AllPods(consul.INTENT_TREE)
}

func testManifest(podID types.PodID) manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID(podID)
	return builder.GetManifest()
}
