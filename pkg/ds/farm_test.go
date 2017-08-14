// +build !race

package ds

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consultest"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"

	. "github.com/anthonybishopric/gotcha"
	"github.com/hashicorp/consul/api"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	fake_checker "github.com/square/p2/pkg/health/checker/test"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	klabels "k8s.io/kubernetes/pkg/labels"
)

// Tests dsContends for changes to both daemon sets and nodes
func TestContendNodes(t *testing.T) {
	//
	// Instantiate farm
	//
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)
	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := createAndSeedApplicator(fixture.Client, t)
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "contendNodes",
	})
	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	var allNodes []types.NodeName
	allNodes = append(allNodes, "node1")
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	dsf := &Farm{
		dsStore:               dsStore,
		dsLocker:              dsStore,
		store:                 consulStore,
		txner:                 fixture.Client.KV(),
		scheduler:             scheduler.NewApplicatorScheduler(applicator),
		labeler:               applicator,
		watcher:               applicator,
		labelsAggregationRate: 1 * time.Nanosecond,
		children:              make(map[ds_fields.ID]*childDS),
		session:               consultest.NewSession(),
		logger:                logger,
		alerter:               alerting.NewNop(),
		healthChecker:         &happyHealthChecker,
		dsRetryInterval:       testFarmRetryInterval,
	}
	quitCh := make(chan struct{})
	defer close(quitCh)
	go func() {
		go dsf.cleanupDaemonSetPods(quitCh)
		dsf.mainLoop(quitCh)
	}()

	//
	// Check for contention between two daemon sets among their nodes
	//
	// Make a daemon set
	podID := types.PodID("testPod")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az1"})
	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	dsData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, dsData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Make a node and verify that it was scheduled
	err = applicator.SetLabel(labels.NODE, "node1", pc_fields.AvailabilityZoneLabel, "az1")
	if err != nil {
		t.Fatal(err)
	}

	labeled, err := waitForPodLabel(applicator, true, "node1/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID := labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make another daemon set with a contending AvailabilityZoneLabel and verify
	// that it gets disabled and that the node label does not change
	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	anotherDSData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
	Assert(t).AreNotEqual(dsData.ID.String(), anotherDSData.ID.String(), "Precondition failed")
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, anotherDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	labeled, err = waitForPodLabel(applicator, true, "node1/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	anotherDSID := labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), anotherDSID, "Expected pod label not to be overwritten")

	// Expect the new daemon set to be disabled both in the farm and in the dsStore
	err = waitForDisabled(dsf, dsStore, anotherDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling daemon set!")

	//
	// Make a third daemon set and update its node selector to force a contend,
	// then verify that it has been disabled and the node hasn't been overwritten
	//
	anotherSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"undefined"})
	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	badDS, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, anotherSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, badDS.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	mutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.NodeSelector = nodeSelector
		return dsToUpdate, nil
	}

	badDS, err = dsStore.MutateDS(badDS.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	err = waitForMutateSelector(dsf, badDS)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	labeled, err = waitForPodLabel(applicator, true, "node1/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	anotherDSID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), anotherDSID, "Expected pod label not to be overwritten")

	err = waitForDisabled(dsf, dsStore, anotherDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling daemon set!")
}

// Tests dsContends for NodeSelectors
func TestContendSelectors(t *testing.T) {

	//
	// Instantiate farm
	//
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)
	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := createAndSeedApplicator(fixture.Client, t)
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "contendSelectors",
	})
	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	var allNodes []types.NodeName
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	dsf := &Farm{
		dsStore:               dsStore,
		dsLocker:              dsStore,
		store:                 consulStore,
		txner:                 fixture.Client.KV(),
		scheduler:             scheduler.NewApplicatorScheduler(applicator),
		labeler:               applicator,
		watcher:               applicator,
		labelsAggregationRate: 1 * time.Nanosecond,
		children:              make(map[ds_fields.ID]*childDS),
		session:               consultest.NewSession(),
		logger:                logger,
		alerter:               alerting.NewNop(),
		healthChecker:         &happyHealthChecker,
		dsRetryInterval:       testFarmRetryInterval,
	}
	quitCh := make(chan struct{})
	defer close(quitCh)
	go func() {
		go dsf.cleanupDaemonSetPods(quitCh)
		dsf.mainLoop(quitCh)
	}()

	//
	// Make two daemon sets with a everything selector and verify that they trivially
	// contend and that only the second daemon set gets disabled
	//
	// Make a daemon set
	podID := types.PodID("testPod")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	everythingSelector := klabels.Everything()
	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	firstDSData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, everythingSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, firstDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	secondDSData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, everythingSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, secondDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Verify that only the second daemon set is disabled
	err = waitForDisabled(dsf, dsStore, firstDSData.ID, false)
	Assert(t).IsNil(err, "First daemon set should not be disabled")

	err = waitForDisabled(dsf, dsStore, secondDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling second daemon set")

	// Add another daemon set with different selector and verify it gets disabled
	someSelector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"nowhere"})
	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	thirdDSData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, someSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, thirdDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	err = waitForDisabled(dsf, dsStore, thirdDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling third daemon set")

	//
	// Disable first daemon set, then enable second and third daemon sets in that order
	// and then there should be a contend on the third daemon set
	//
	disableMutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = true
		return dsToUpdate, nil
	}

	enableMutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = false
		return dsToUpdate, nil
	}

	// Disable first ds and verify it is disabled
	_, err = dsStore.MutateDS(firstDSData.ID, disableMutator)
	Assert(t).IsNil(err, "Expected no error getting daemon set")

	err = waitForDisabled(dsf, dsStore, firstDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling first daemon set")

	// Enable second ds and verify it is enabled
	_, err = dsStore.MutateDS(secondDSData.ID, enableMutator)
	Assert(t).IsNil(err, "Expected no error getting daemon set")

	err = waitForDisabled(dsf, dsStore, secondDSData.ID, false)
	Assert(t).IsNil(err, "Error enabling second daemon set")

	// Enabled third ds and verify it disabled because it contends with second ds
	_, err = dsStore.MutateDS(thirdDSData.ID, enableMutator)
	Assert(t).IsNil(err, "Expected no error getting daemon set")

	err = waitForDisabled(dsf, dsStore, thirdDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling third daemon set")

	//
	// Test equivalent selectors, fifth ds should contend with fourth
	//
	anotherPodID := types.PodID("anotherPodID")

	anotherManifestBuilder := manifest.NewBuilder()
	anotherManifestBuilder.SetID(anotherPodID)
	anotherPodManifest := anotherManifestBuilder.GetManifest()

	equalSelector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})

	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	fourthDSData, err := dsStore.Create(ctx, anotherPodManifest, minHealth, clusterName, equalSelector, anotherPodID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, fourthDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Verify the fourth daemon set is enabled
	err = waitForDisabled(dsf, dsStore, fourthDSData.ID, false)
	Assert(t).IsNil(err, "Error enabling fourth daemon set")

	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	fifthDSData, err := dsStore.Create(ctx, anotherPodManifest, minHealth, clusterName, equalSelector, anotherPodID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, fifthDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Verify that the fifth daemon set contends and gets disabled
	err = waitForDisabled(dsf, dsStore, fifthDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling fifth daemon set")
}

func TestFarmSchedule(t *testing.T) {
	//
	// Instantiate farm
	//
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)
	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := createAndSeedApplicator(fixture.Client, t)
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "farmSchedule",
	})
	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	var allNodes []types.NodeName
	allNodes = append(allNodes, "node1", "node2", "node3", "node4")
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		allNodes = append(allNodes, types.NodeName(nodeName))
	}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	dsf := &Farm{
		dsStore:               dsStore,
		dsLocker:              dsStore,
		store:                 consulStore,
		txner:                 fixture.Client.KV(),
		scheduler:             scheduler.NewApplicatorScheduler(applicator),
		labeler:               applicator,
		watcher:               applicator,
		labelsAggregationRate: 1 * time.Nanosecond,
		children:              make(map[ds_fields.ID]*childDS),
		session:               consultest.NewSession(),
		logger:                logger,
		alerter:               alerting.NewNop(),
		healthChecker:         &happyHealthChecker,
		dsRetryInterval:       testFarmRetryInterval,
	}
	quitCh := make(chan struct{})
	defer close(quitCh)
	go func() {
		go dsf.cleanupDaemonSetPods(quitCh)
		dsf.mainLoop(quitCh)
	}()

	// Make two daemon sets with difference node selectors
	// First daemon set
	podID := types.PodID("testPod")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az1"})
	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	dsData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, dsData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Second daemon set
	anotherNodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az2"})
	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	anotherDSData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, anotherNodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")
	err = waitForCreate(dsf, anotherDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Make a node and verify that it was scheduled by the first daemon set
	applicator.SetLabel(labels.NODE, "node1", pc_fields.AvailabilityZoneLabel, "az1")

	labeled, err := waitForPodLabel(applicator, true, "node1/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID := labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make a second node and verify that it was scheduled by the second daemon set
	applicator.SetLabel(labels.NODE, "node2", pc_fields.AvailabilityZoneLabel, "az2")

	labeled, err = waitForPodLabel(applicator, true, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make unschedulable nodes and verify they don't get scheduled by anything
	applicator.SetLabel(labels.NODE, "node3", pc_fields.AvailabilityZoneLabel, "az3")
	applicator.SetLabel(labels.NODE, "node4", pc_fields.AvailabilityZoneLabel, "undefined")

	labeled, err = waitForPodLabel(applicator, false, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")
	labeled, err = waitForPodLabel(applicator, false, "node4/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")

	// Now add 10 new nodes and verify that they are scheduled by the first daemon set
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		err := applicator.SetLabel(labels.NODE, nodeName, pc_fields.AvailabilityZoneLabel, "az1")
		Assert(t).IsNil(err, "expected no error labeling node")
	}
	for i := 0; i < 10; i++ {
		podPath := fmt.Sprintf("good_node%v/testPod", i)
		labeled, err = waitForPodLabel(applicator, true, podPath)
		Assert(t).IsNil(err, "Expected pod to have a dsID label")
		dsID := labeled.Labels.Get(DSIDLabel)
		Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled")
	}

	//
	// Update a daemon set's node selector and expect one node to be scheduled, one to be unscheduled
	//
	mutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az3"})
		dsToUpdate.NodeSelector = someSelector
		return dsToUpdate, nil
	}
	anotherDSData, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	err = waitForMutateSelector(dsf, anotherDSData)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	// Verify node2 is unscheduled
	labeled, err = waitForPodLabel(applicator, false, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")
	// Verify node3 is scheduled
	labeled, err = waitForPodLabel(applicator, true, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	//
	// Now update the node selector to schedule node2 again and verify
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.NodeSelector = anotherNodeSelector
		return dsToUpdate, nil
	}
	anotherDSData, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	err = waitForMutateSelector(dsf, anotherDSData)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	// Verify node2 is scheduled
	labeled, err = waitForPodLabel(applicator, true, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	//
	// Disabling a daemon set should not schedule or unschedule any nodes
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = true

		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az3"})
		dsToUpdate.NodeSelector = someSelector
		return dsToUpdate, nil
	}
	anotherDSData, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	err = waitForMutateSelector(dsf, anotherDSData)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	// Verify it is disabled
	err = waitForDisabled(dsf, dsStore, anotherDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling daemon set")

	// Verify node2 is scheduled
	labeled, err = waitForPodLabel(applicator, true, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")

	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	// Verify node3 is unscheduled
	labeled, err = waitForPodLabel(applicator, false, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")

	//
	// Enable a daemon set should make the dameon set resume its regular activities
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = false
		return dsToUpdate, nil
	}
	anotherDSData, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")

	// Verify it is enabled
	err = waitForDisabled(dsf, dsStore, anotherDSData.ID, false)
	Assert(t).IsNil(err, "Error enabling daemon set")

	// Verify node2 is unscheduled
	labeled, err = waitForPodLabel(applicator, false, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")
	// Verify node3 is scheduled
	labeled, err = waitForPodLabel(applicator, true, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	// If no nodes are eligible, DS should take no action.
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})
		dsToUpdate.NodeSelector = someSelector
		return dsToUpdate, nil
	}
	_, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	err = waitForMutateSelector(dsf, anotherDSData)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	// Verify node3 is still scheduled
	labeled, err = waitForPodLabel(applicator, true, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	err = dsStore.Delete(anotherDSData.ID)
	Assert(t).IsNil(err, "Expected no error deleting daemon set")
	err = waitForDelete(dsf, anotherDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be deleted in farm")

	// Verify node3 is unscheduled
	// behavior change: Daemon Set deletions do not delete their pods (for now)
	labeled, err = waitForPodLabel(applicator, true, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
}

func TestCleanupPods(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)
	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := createAndSeedApplicator(fixture.Client, t)

	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	// Make some dangling pod labels and instantiate a farm and expect it clean it up
	podID := types.PodID("testPod")
	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	var allNodes []types.NodeName
	allNodes = append(allNodes)
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("node%v", i)
		allNodes = append(allNodes, types.NodeName(nodeName))
	}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("node%v", i)
		id := labels.MakePodLabelKey(types.NodeName(nodeName), podID)
		err := applicator.SetLabel(labels.POD, id, DSIDLabel, "impossible_id")
		Assert(t).IsNil(err, "Expected no error labeling node")

		_, err = consulStore.SetPod(consul.INTENT_TREE, types.NodeName(nodeName), podManifest)
		Assert(t).IsNil(err, "Expected no error added pod to intent tree")
	}

	// Assert that precondition is true
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("node%v", i)
		id := labels.MakePodLabelKey(types.NodeName(nodeName), podID)
		labeled, err := applicator.GetLabels(labels.POD, id)
		Assert(t).IsNil(err, "Expected no error getting labels")
		Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Precondition failed: Pod must have a dsID label")

		_, _, err = consulStore.Pod(consul.INTENT_TREE, types.NodeName(nodeName), podID)
		Assert(t).IsNil(err, "Expected no error getting pod from intent store")
		Assert(t).AreNotEqual(err, pods.NoCurrentManifest, "Precondition failed: Pod was not in intent store")
	}

	// Instantiate farm
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "cleanupPods",
	})
	dsf := &Farm{
		dsStore:               dsStore,
		store:                 consulStore,
		txner:                 fixture.Client.KV(),
		scheduler:             scheduler.NewApplicatorScheduler(applicator),
		labeler:               applicator,
		watcher:               applicator,
		labelsAggregationRate: 1 * time.Nanosecond,
		children:              make(map[ds_fields.ID]*childDS),
		session:               consultest.NewSession(),
		logger:                logger,
		alerter:               alerting.NewNop(),
		healthChecker:         &happyHealthChecker,
		dsRetryInterval:       testFarmRetryInterval,
	}
	quitCh := make(chan struct{})
	defer close(quitCh)
	go func() {
		go dsf.cleanupDaemonSetPods(quitCh)
		dsf.mainLoop(quitCh)
	}()

	// Make there are no nodes left
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("node%v", i)
		id := labels.MakePodLabelKey(types.NodeName(nodeName), podID)
		_, err := waitForPodLabel(applicator, false, id)
		Assert(t).IsNil(err, "Expected pod not to have a dsID label")

		condition := func() error {
			_, _, err = consulStore.Pod(consul.INTENT_TREE, types.NodeName(nodeName), podID)
			if err != pods.NoCurrentManifest {
				return util.Errorf("Expected pod to be deleted in intent store")
			}
			return nil
		}
		err = waitForCondition(condition)
		Assert(t).IsNil(err, "Error cleaning up pods")
	}
}

func TestMultipleFarms(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)
	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := createAndSeedApplicator(fixture.Client, t)

	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	session1, renewalErrCh1, err := consulStore.NewSession("firstFarm", nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err, ok := <-renewalErrCh1
		if ok {
			t.Error(err)
		}
	}()

	session2, renewalErrCh2, err := consulStore.NewSession("secondFarm", nil)
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err, ok := <-renewalErrCh2
		if ok {
			t.Error(err)
		}
	}()

	firstLogger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "firstMultiple",
	})

	var allNodes []types.NodeName
	allNodes = append(allNodes, "node1", "node2", "node3", "node4")
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		allNodes = append(allNodes, types.NodeName(nodeName))
	}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	//
	// Instantiate first farm
	//
	firstFarm := &Farm{
		dsStore:               dsStore,
		dsLocker:              dsStore,
		store:                 consulStore,
		txner:                 fixture.Client.KV(),
		scheduler:             scheduler.NewApplicatorScheduler(applicator),
		labeler:               applicator,
		watcher:               applicator,
		labelsAggregationRate: 1 * time.Nanosecond,
		children:              make(map[ds_fields.ID]*childDS),
		session:               session1,
		logger:                firstLogger,
		alerter:               alerting.NewNop(),
		healthChecker:         &happyHealthChecker,
		dsRetryInterval:       testFarmRetryInterval,
	}
	firstQuitCh := make(chan struct{})
	defer close(firstQuitCh)
	go func() {
		go firstFarm.cleanupDaemonSetPods(firstQuitCh)
		firstFarm.mainLoop(firstQuitCh)
	}()

	//
	// Instantiate second farm
	//
	secondLogger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "secondMultiple",
	})
	secondFarm := &Farm{
		dsStore:               dsStore,
		dsLocker:              dsStore,
		store:                 consulStore,
		txner:                 fixture.Client.KV(),
		scheduler:             scheduler.NewApplicatorScheduler(applicator),
		labeler:               applicator,
		watcher:               applicator,
		labelsAggregationRate: 1 * time.Nanosecond,
		children:              make(map[ds_fields.ID]*childDS),
		session:               session2,
		logger:                secondLogger,
		alerter:               alerting.NewNop(),
		healthChecker:         &happyHealthChecker,
	}
	secondQuitCh := make(chan struct{})
	defer close(secondQuitCh)
	go func() {
		go secondFarm.cleanupDaemonSetPods(secondQuitCh)
		secondFarm.mainLoop(secondQuitCh)
	}()

	// Make two daemon sets with different node selectors
	// First daemon set
	podID := types.PodID("testPod")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az1"})
	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	dsData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")

	// Second daemon set
	anotherNodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az2"})
	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	anotherDSData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, anotherNodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	Assert(t).IsNil(err, "Expected no error committing transaction")

	// Make a node and verify that it was scheduled by the first daemon set
	applicator.SetLabel(labels.NODE, "node1", pc_fields.AvailabilityZoneLabel, "az1")

	labeled, err := waitForPodLabel(applicator, true, "node1/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID := labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make a second node and verify that it was scheduled by the second daemon set
	applicator.SetLabel(labels.NODE, "node2", pc_fields.AvailabilityZoneLabel, "az2")

	labeled, err = waitForPodLabel(applicator, true, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make unschedulable nodes and verify they don't get scheduled by anything
	applicator.SetLabel(labels.NODE, "node3", pc_fields.AvailabilityZoneLabel, "az3")
	applicator.SetLabel(labels.NODE, "node4", pc_fields.AvailabilityZoneLabel, "undefined")

	labeled, err = waitForPodLabel(applicator, false, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")
	labeled, err = waitForPodLabel(applicator, false, "node4/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")

	// Now add 10 new nodes and verify that they are scheduled by the first daemon set
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		err := applicator.SetLabel(labels.NODE, nodeName, pc_fields.AvailabilityZoneLabel, "az1")
		Assert(t).IsNil(err, "expected no error labeling node")
	}
	for i := 0; i < 10; i++ {
		podPath := fmt.Sprintf("good_node%v/testPod", i)
		labeled, err = waitForPodLabel(applicator, true, podPath)
		Assert(t).IsNil(err, "Expected pod to have a dsID label")
		dsID := labeled.Labels.Get(DSIDLabel)
		Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled")
	}

	//
	// Update a daemon set's node selector and expect one node to be scheduled, one to be unscheduled
	//
	mutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az3"})
		dsToUpdate.NodeSelector = someSelector
		return dsToUpdate, nil
	}
	anotherDSData, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")

	err = waitForMutateSelectorFarms(firstFarm, secondFarm, anotherDSData)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	// Verify node2 is unscheduled
	labeled, err = waitForPodLabel(applicator, false, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")
	// Verify node3 is scheduled
	labeled, err = waitForPodLabel(applicator, true, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	//
	// Now update the node selector to schedule node2 again and verify
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az2"})
		dsToUpdate.NodeSelector = someSelector
		return dsToUpdate, nil
	}
	anotherDSData, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	err = waitForMutateSelectorFarms(firstFarm, secondFarm, anotherDSData)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	// Verify node2 is scheduled
	labeled, err = waitForPodLabel(applicator, true, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")
	// Verify node3 is unscheduled
	labeled, err = waitForPodLabel(applicator, false, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")

	//
	// Disabling a daemon set should not schedule or unschedule any nodes
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = true

		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az3"})
		dsToUpdate.NodeSelector = someSelector
		return dsToUpdate, nil
	}
	anotherDSData, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	err = waitForMutateSelectorFarms(firstFarm, secondFarm, anotherDSData)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	// Verify it is disabled
	condition := func() error {
		anotherDSData, _, err = dsStore.Get(anotherDSData.ID)
		if err != nil {
			return util.Errorf("Expected no error getting daemon set")
		}
		if !anotherDSData.Disabled {
			return util.Errorf("Expected daemon set to be disabled")
		}

		firstFarm.childMu.Lock()
		defer firstFarm.childMu.Unlock()
		secondFarm.childMu.Lock()
		defer secondFarm.childMu.Unlock()
		if _, ok := firstFarm.children[anotherDSData.ID]; ok {
			if !firstFarm.children[anotherDSData.ID].ds.IsDisabled() {
				return util.Errorf("Expected daemon set to be disabled in only one farm")
			}
			if _, ok := secondFarm.children[anotherDSData.ID]; ok {
				return util.Errorf("Expected daemon set to be held by only one farm")
			}
		} else if _, ok := secondFarm.children[anotherDSData.ID]; ok {
			if !secondFarm.children[anotherDSData.ID].ds.IsDisabled() {
				return util.Errorf("Expected daemon set to be disabled in only one farm")
			}
			if _, ok := firstFarm.children[anotherDSData.ID]; ok {
				return util.Errorf("Expected daemon set to be held by only one farm")
			}
		} else {
			return util.Errorf("Expected daemon set to be disabled in only one farm")
		}
		return nil
	}
	err = waitForCondition(condition)
	Assert(t).IsNil(err, "Error disabling daemon set!")

	// Verify node2 is scheduled
	labeled, err = waitForPodLabel(applicator, true, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")
	// Verify node3 is unscheduled
	labeled, err = waitForPodLabel(applicator, false, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")

	//
	// Enable a daemon set should make the dameon set resume its regular activities
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = false
		return dsToUpdate, nil
	}
	_, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")

	// Verify it is enabled
	condition = func() error {
		anotherDSData, _, err = dsStore.Get(anotherDSData.ID)
		if err != nil {
			return util.Errorf("Expected no error getting daemon set")
		}
		if anotherDSData.Disabled {
			return util.Errorf("Expected daemon set to be enabled")
		}

		if _, ok := firstFarm.children[anotherDSData.ID]; ok {
			if firstFarm.children[anotherDSData.ID].ds.IsDisabled() {
				return util.Errorf("Expected daemon set to be enabled in only one farm")
			}
			if _, ok := secondFarm.children[anotherDSData.ID]; ok {
				return util.Errorf("Expected daemon set to be held by only one farm")
			}
		} else if _, ok := secondFarm.children[anotherDSData.ID]; ok {
			if secondFarm.children[anotherDSData.ID].ds.IsDisabled() {
				return util.Errorf("Expected daemon set to be enabled in only one farm")
			}
			if _, ok := firstFarm.children[anotherDSData.ID]; ok {
				return util.Errorf("Expected daemon set to be held by only one farm")
			}
		} else {
			return util.Errorf("Expected daemon set to be enabled in only one farm")
		}
		return nil
	}
	err = waitForCondition(condition)
	Assert(t).IsNil(err, "Error enabling daemon set!")

	// Verify node2 is unscheduled
	labeled, err = waitForPodLabel(applicator, false, "node2/testPod")
	Assert(t).IsNil(err, "Expected pod not to have a dsID label")
	// Verify node3 is scheduled
	labeled, err = waitForPodLabel(applicator, true, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	// If no nodes are eligible, DS should take no action.
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})
		dsToUpdate.NodeSelector = someSelector
		return dsToUpdate, nil
	}
	anotherDSData, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	err = waitForMutateSelectorFarms(firstFarm, secondFarm, anotherDSData)
	Assert(t).IsNil(err, "Expected daemon set to be mutated in farm")

	// Verify node3 is still scheduled
	labeled, err = waitForPodLabel(applicator, true, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	err = dsStore.Delete(anotherDSData.ID)
	Assert(t).IsNil(err, "Expected no error deleting daemon set")

	// Verify node3 is unscheduled
	// behavior change: Daemon Set deletions do not delete their pods (for now)
	labeled, err = waitForPodLabel(applicator, true, "node3/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")

	session1.Destroy()
	session2.Destroy()

	wg.Wait()
}

func TestRelock(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)
	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := createAndSeedApplicator(fixture.Client, t)

	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	session, renewalErrCh, err := consulStore.NewSession("ds_test", nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		err, ok := <-renewalErrCh
		if ok {
			t.Error(err)
		}
	}()

	allNodes := []types.NodeName{"node1", "node2", "node3"}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	mkFarm := func(logName string) (chan<- struct{}, <-chan struct{}) {
		farm := &Farm{
			dsStore:               dsStore,
			dsLocker:              dsStore,
			store:                 consulStore,
			txner:                 fixture.Client.KV(),
			scheduler:             scheduler.NewApplicatorScheduler(applicator),
			labeler:               applicator,
			watcher:               applicator,
			labelsAggregationRate: 1 * time.Nanosecond,
			children:              make(map[ds_fields.ID]*childDS),
			session:               session,
			logger: logging.DefaultLogger.SubLogger(logrus.Fields{
				"farm": logName,
			}),
			alerter:         alerting.NewNop(),
			healthChecker:   &happyHealthChecker,
			dsRetryInterval: testFarmRetryInterval,
		}
		quitCh := make(chan struct{})
		farmHasQuit := make(chan struct{})
		go func() {
			go farm.cleanupDaemonSetPods(quitCh)
			farm.mainLoop(quitCh)
			close(farmHasQuit)
		}()

		return quitCh, farmHasQuit
	}

	firstQuitCh, farmHasQuit := mkFarm("firstRelock")

	mkds := func(zone string) ds_fields.DaemonSet {
		podID := types.PodID("testPod")
		minHealth := 0
		clusterName := ds_fields.ClusterName("some_name")

		manifestBuilder := manifest.NewBuilder()
		manifestBuilder.SetID(podID)
		podManifest := manifestBuilder.GetManifest()

		nodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{zone})
		ctx, cancel := transaction.New(context.Background())
		defer cancel()
		dsData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
		Assert(t).IsNil(err, "Expected no error creating request")
		err = transaction.MustCommit(ctx, fixture.Client.KV())
		Assert(t).IsNil(err, "Expected no error committing transaction")
		return dsData
	}

	dsData := mkds("az1")

	assertLabel := func(node, desc string) {
		applicator.SetLabel(labels.NODE, node, pc_fields.AvailabilityZoneLabel, "az1")
		labeled, err := waitForPodLabel(applicator, true, node+"/testPod")
		Assert(t).IsNil(err, "Expected pod to have a dsID label when "+desc)
		dsID := labeled.Labels.Get(DSIDLabel)
		Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled when "+desc)
	}

	assertLabel("node1", "one farm")

	secondQuitCh, _ := mkFarm("secondRelock")
	defer close(secondQuitCh)

	assertLabel("node2", "second farm created, first farm still active")

	close(firstQuitCh)
	<-farmHasQuit

	// Create a DS to make the watch fire.
	// Don't modify the existing DS.
	// The point of this test is to see if the second farm picks up the existing DS despite it not changing.
	_ = mkds("az2")

	// After the first farm quits, the second farm should take over.
	assertLabel("node3", "first farm quit")
}

func TestDieAndUpdate(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	dsStore := dsstore.NewConsul(fixture.Client, 0, &logging.DefaultLogger)
	consulStore := consul.NewConsulStore(fixture.Client)
	applicator := createAndSeedApplicator(fixture.Client, t)

	preparer := consultest.NewFakePreparer(consulStore, logging.DefaultLogger)
	preparer.Enable()
	defer preparer.Disable()

	session, renewalErrCh, err := consulStore.NewSession("ds_test", nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		err, ok := <-renewalErrCh
		if ok {
			t.Error(err)
		}
	}()

	allNodes := []types.NodeName{"node1", "node2", "node3"}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	mkFarm := func(logName string) (chan<- struct{}, <-chan struct{}) {
		farm := &Farm{
			dsStore:               dsStore,
			dsLocker:              dsStore,
			store:                 consulStore,
			txner:                 fixture.Client.KV(),
			scheduler:             scheduler.NewApplicatorScheduler(applicator),
			labeler:               applicator,
			watcher:               applicator,
			labelsAggregationRate: 1 * time.Nanosecond,
			children:              make(map[ds_fields.ID]*childDS),
			session:               session,
			logger: logging.DefaultLogger.SubLogger(logrus.Fields{
				"farm": logName,
			}),
			alerter:         alerting.NewNop(),
			healthChecker:   &happyHealthChecker,
			dsRetryInterval: testFarmRetryInterval,
		}
		quitCh := make(chan struct{})
		farmHasQuit := make(chan struct{})
		go func() {
			go farm.cleanupDaemonSetPods(quitCh)
			farm.mainLoop(quitCh)
			close(farmHasQuit)
		}()

		return quitCh, farmHasQuit
	}

	firstQuitCh, farmHasQuit := mkFarm("firstUpdateCrash")

	mkds := func(zone string) ds_fields.DaemonSet {
		podID := types.PodID("testPod")
		minHealth := 0
		clusterName := ds_fields.ClusterName("some_name")

		manifestBuilder := manifest.NewBuilder()
		manifestBuilder.SetID(podID)
		podManifest := manifestBuilder.GetManifest()

		nodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{zone})
		ctx, cancel := transaction.New(context.Background())
		defer cancel()
		dsData, err := dsStore.Create(ctx, podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
		Assert(t).IsNil(err, "Expected no error creating request")
		err = transaction.MustCommit(ctx, fixture.Client.KV())
		Assert(t).IsNil(err, "Expected no error committing transaction")
		return dsData
	}

	dsData := mkds("az1")

	assertLabel := func(node, desc string) {
		applicator.SetLabel(labels.NODE, node, pc_fields.AvailabilityZoneLabel, "az1")
		labeled, err := waitForPodLabel(applicator, true, node+"/testPod")
		Assert(t).IsNil(err, "Expected pod to have a dsID label when "+desc)
		dsID := labeled.Labels.Get(DSIDLabel)
		Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled when "+desc)
	}

	assertLabel("node1", "one farm")

	secondQuitCh, _ := mkFarm("secondUpdateCrash")
	defer close(secondQuitCh)

	assertLabel("node2", "second farm created, first farm still active")

	close(firstQuitCh)
	<-farmHasQuit

	_, err = dsStore.MutateDS(dsData.ID, func(ds ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		ds.Name = ds_fields.ClusterName("name_has_changed")
		return ds, nil
	})
	Assert(t).IsNil(err, "Expected no error changing name of daemon set")

	// After the first farm quits, the second farm should take over.
	assertLabel("node3", "first farm quit")
}

func waitForPodLabel(applicator labels.Applicator, hasDSIDLabel bool, podPath string) (labels.Labeled, error) {
	var labeled labels.Labeled
	var err error

	condition := func() error {
		var val bool
		labeled, err = applicator.GetLabels(labels.POD, podPath)
		if err == nil {
			val = labeled.Labels.Has(DSIDLabel)
			if hasDSIDLabel == val {
				return nil
			}
		}
		return util.Errorf("Expected hasIDLabel to be '%v', got '%v'", hasDSIDLabel, val)
	}

	err = waitForCondition(condition)
	return labeled, err
}

type testDSGetter interface {
	Get(ds_fields.ID) (ds_fields.DaemonSet, *api.QueryMeta, error)
}

func waitForDisabled(
	dsf *Farm,
	dsStore testDSGetter,
	dsID ds_fields.ID,
	isDisabled bool,
) error {
	var newDS ds_fields.DaemonSet
	var err error

	condition := func() error {
		newDS, _, err = dsStore.Get(dsID)
		if err != nil {
			return util.Errorf("Expected no error getting daemon set")
		}
		if newDS.Disabled != isDisabled {
			return util.Errorf("Unexpected disabled value. Expected '%v', got '%v'",
				newDS.Disabled,
				isDisabled,
			)
		}
		if _, ok := dsf.children[newDS.ID]; !ok {
			return util.Errorf("Expected farm to create child daemon set with id '%v'", dsID)
		}
		if dsf.children[newDS.ID].ds.IsDisabled() != isDisabled {
			return util.Errorf("Unexpected disabled value in farm. Expected '%v', got '%v'",
				dsf.children[newDS.ID].ds.IsDisabled(),
				isDisabled,
			)
		}
		return nil
	}
	return waitForCondition(condition)
}

// Polls for the farm to get be populated by a daemon set with the same id
func waitForCreate(dsf *Farm, dsID ds_fields.ID) error {
	condition := func() error {
		dsf.childMu.Lock()
		defer dsf.childMu.Unlock()
		if _, ok := dsf.children[dsID]; ok {
			return nil
		}
		return util.Errorf("Farm does not have daemon set id")
	}
	return waitForCondition(condition)
}

// Polls for the farm to not have a daemon set with the ID.
func waitForDelete(dsf *Farm, dsID ds_fields.ID) error {
	condition := func() error {
		dsf.childMu.Lock()
		defer dsf.childMu.Unlock()
		if _, ok := dsf.children[dsID]; !ok {
			return nil
		}
		return util.Errorf("Farm still has daemon set id")
	}
	return waitForCondition(condition)
}

// Polls for the farm to be populated by a daemon set with the same
// id, disabled value, and node selector as the daemon set in the argument
func waitForMutateSelector(dsf *Farm, ds ds_fields.DaemonSet) error {
	condition := func() error {
		dsf.childMu.Lock()
		defer dsf.childMu.Unlock()
		if anotherDS, ok := dsf.children[ds.ID]; ok {
			if ds.ID != anotherDS.ds.ID() ||
				ds.NodeSelector.String() != anotherDS.ds.GetNodeSelector().String() {
				return util.Errorf(
					"Daemon sets do not match, expected '%v', '%v', got '%v', '%v'",
					ds.ID,
					ds.NodeSelector.String(),
					anotherDS.ds.ID(),
					anotherDS.ds.GetNodeSelector().String(),
				)
			}
			return nil
		}
		return util.Errorf("Farm does not have daemon set id: '%v'", ds.ID)
	}
	return waitForCondition(condition)
}

// Polls for either farm to get be populated by a daemon set with the same
// node selector as the daemon set in the argument
func waitForMutateSelectorFarms(firstFarm *Farm, secondFarm *Farm, ds ds_fields.DaemonSet) error {
	condition := func() error {
		firstFarm.childMu.Lock()
		defer firstFarm.childMu.Unlock()
		secondFarm.childMu.Lock()
		defer secondFarm.childMu.Unlock()
		if anotherDS, ok := firstFarm.children[ds.ID]; ok {
			if ds.ID != anotherDS.ds.ID() ||
				ds.NodeSelector.String() != anotherDS.ds.GetNodeSelector().String() {
				return util.Errorf(
					"Daemon sets do not match, expected '%v', '%v', got '%v', '%v'",
					ds.ID,
					ds.NodeSelector.String(),
					anotherDS.ds.ID(),
					anotherDS.ds.GetNodeSelector().String(),
				)
			}
			return nil
		} else if anotherDS, ok := secondFarm.children[ds.ID]; ok {
			if ds.ID != anotherDS.ds.ID() ||
				ds.NodeSelector.String() != anotherDS.ds.GetNodeSelector().String() {
				return util.Errorf(
					"Daemon sets do not match, expected '%v', '%v', got '%v', '%v'",
					ds.ID,
					ds.NodeSelector.String(),
					anotherDS.ds.ID(),
					anotherDS.ds.GetNodeSelector().String(),
				)
			}
			return nil
		}
		return util.Errorf("Farm does not have daemon set id")
	}
	return waitForCondition(condition)
}

func createAndSeedApplicator(client consulutil.ConsulClient, t *testing.T) labels.Applicator {
	applicator := labels.NewConsulApplicator(client, 0)
	// seed the label trees that daemon set farm uses
	err := applicator.SetLabel(labels.NODE, "key", "whatever", "whatever")
	if err != nil {
		t.Fatal(err)
	}

	err = applicator.SetLabel(labels.POD, "key", "whatever", "whatever")
	if err != nil {
		t.Fatal(err)
	}

	return applicator
}
