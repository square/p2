package ds

import (
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore/dsstoretest"
	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/types"

	. "github.com/anthonybishopric/gotcha"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	fake_checker "github.com/square/p2/pkg/health/checker/test"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	replicationTimeout    = replication.NoTimeout
	testFarmRetryInterval = time.Duration(1000 * time.Millisecond)
)

// Tests dsContends for changes to both daemon sets and nodes
func TestContendNodes(t *testing.T) {
	retryInterval = testFarmRetryInterval

	//
	// Instantiate farm
	//
	dsStore := dsstoretest.NewFake()
	kpStore := kptest.NewFakePodStore(make(map[kptest.FakePodStoreKey]manifest.Manifest), make(map[string]kp.WatchResult))
	applicator := labels.NewFakeApplicator()
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "contendNodes",
	})

	var allNodes []types.NodeName
	allNodes = append(allNodes, "node1")
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	dsf := &Farm{
		dsStore:       dsStore,
		kpStore:       kpStore,
		scheduler:     scheduler.NewApplicatorScheduler(applicator),
		applicator:    applicator,
		children:      make(map[ds_fields.ID]*childDS),
		session:       kptest.NewSession(),
		logger:        logger,
		alerter:       alerting.NewNop(),
		healthChecker: &happyHealthChecker,
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
	dsData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = waitForCreate(dsf, dsData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Make a node and verify that it was scheduled
	applicator.SetLabel(labels.NODE, "node1", pc_fields.AvailabilityZoneLabel, "az1")

	labeled, err := waitForPodLabel(applicator, true, "node1/testPod")
	Assert(t).IsNil(err, "Expected pod to have a dsID label")
	dsID := labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make another daemon set with a contending AvailabilityZoneLabel and verify
	// that it gets disabled and that the node label does not change
	anotherDSData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
	Assert(t).AreNotEqual(dsData.ID.String(), anotherDSData.ID.String(), "Precondition failed")
	Assert(t).IsNil(err, "Expected no error creating request")
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
	badDS, err := dsStore.Create(podManifest, minHealth, clusterName, anotherSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
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
	retryInterval = testFarmRetryInterval

	//
	// Instantiate farm
	//
	dsStore := dsstoretest.NewFake()
	kpStore := kptest.NewFakePodStore(make(map[kptest.FakePodStoreKey]manifest.Manifest), make(map[string]kp.WatchResult))
	applicator := labels.NewFakeApplicator()
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "contendSelectors",
	})

	var allNodes []types.NodeName
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	dsf := &Farm{
		dsStore:       dsStore,
		kpStore:       kpStore,
		scheduler:     scheduler.NewApplicatorScheduler(applicator),
		applicator:    applicator,
		children:      make(map[ds_fields.ID]*childDS),
		session:       kptest.NewSession(),
		logger:        logger,
		alerter:       alerting.NewNop(),
		healthChecker: &happyHealthChecker,
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
	firstDSData, err := dsStore.Create(podManifest, minHealth, clusterName, everythingSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = waitForCreate(dsf, firstDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	secondDSData, err := dsStore.Create(podManifest, minHealth, clusterName, everythingSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
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
	thirdDSData, err := dsStore.Create(podManifest, minHealth, clusterName, someSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
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
	anotherPodManifest := manifestBuilder.GetManifest()

	equalSelector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})

	fourthDSData, err := dsStore.Create(anotherPodManifest, minHealth, clusterName, equalSelector, anotherPodID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = waitForCreate(dsf, fourthDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Verify the fourth daemon set is enabled
	err = waitForDisabled(dsf, dsStore, fourthDSData.ID, false)
	Assert(t).IsNil(err, "Error enabling fourth daemon set")

	fifthDSData, err := dsStore.Create(anotherPodManifest, minHealth, clusterName, equalSelector, anotherPodID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = waitForCreate(dsf, fifthDSData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Verify that the fifth daemon set contends and gets disabled
	err = waitForDisabled(dsf, dsStore, fifthDSData.ID, true)
	Assert(t).IsNil(err, "Error disabling fifth daemon set")
}

func TestFarmSchedule(t *testing.T) {
	retryInterval = testFarmRetryInterval

	//
	// Instantiate farm
	//
	dsStore := dsstoretest.NewFake()
	kpStore := kptest.NewFakePodStore(make(map[kptest.FakePodStoreKey]manifest.Manifest), make(map[string]kp.WatchResult))
	applicator := labels.NewFakeApplicator()
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "farmSchedule",
	})

	var allNodes []types.NodeName
	allNodes = append(allNodes, "node1", "node2", "node3")
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		allNodes = append(allNodes, types.NodeName(nodeName))
	}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	dsf := &Farm{
		dsStore:       dsStore,
		kpStore:       kpStore,
		scheduler:     scheduler.NewApplicatorScheduler(applicator),
		applicator:    applicator,
		children:      make(map[ds_fields.ID]*childDS),
		session:       kptest.NewSession(),
		logger:        logger,
		alerter:       alerting.NewNop(),
		healthChecker: &happyHealthChecker,
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
	dsData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
	err = waitForCreate(dsf, dsData.ID)
	Assert(t).IsNil(err, "Expected daemon set to be created")

	// Second daemon set
	anotherNodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az2"})
	anotherDSData, err := dsStore.Create(podManifest, minHealth, clusterName, anotherNodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")
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

	// Make a third unschedulable node and verify it doesn't get scheduled by anything
	applicator.SetLabel(labels.NODE, "node3", pc_fields.AvailabilityZoneLabel, "undefined")

	labeled, err = waitForPodLabel(applicator, false, "node3/testPod")
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
	// Update a daemon set's node selector and expect a node to be unscheduled
	//
	mutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})
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
	// Disabling a daemon set should not unschedule any nodes
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = true

		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})
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
}

func TestCleanupPods(t *testing.T) {
	retryInterval = testFarmRetryInterval

	dsStore := dsstoretest.NewFake()
	kpStore := kptest.NewFakePodStore(make(map[kptest.FakePodStoreKey]manifest.Manifest), make(map[string]kp.WatchResult))
	applicator := labels.NewFakeApplicator()

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

		_, err = kpStore.SetPod(kp.INTENT_TREE, types.NodeName(nodeName), podManifest)
		Assert(t).IsNil(err, "Expected no error added pod to intent tree")
	}

	// Assert that precondition is true
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("node%v", i)
		id := labels.MakePodLabelKey(types.NodeName(nodeName), podID)
		labeled, err := applicator.GetLabels(labels.POD, id)
		Assert(t).IsNil(err, "Expected no error getting labels")
		Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Precondition failed: Pod must have a dsID label")

		_, _, err = kpStore.Pod(kp.INTENT_TREE, types.NodeName(nodeName), podID)
		Assert(t).IsNil(err, "Expected no error getting pod from intent store")
		Assert(t).AreNotEqual(err, pods.NoCurrentManifest, "Precondition failed: Pod was not in intent store")
	}

	// Instantiate farm
	logger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "cleanupPods",
	})
	dsf := &Farm{
		dsStore:       dsStore,
		kpStore:       kpStore,
		scheduler:     scheduler.NewApplicatorScheduler(applicator),
		applicator:    applicator,
		children:      make(map[ds_fields.ID]*childDS),
		session:       kptest.NewSession(),
		logger:        logger,
		alerter:       alerting.NewNop(),
		healthChecker: &happyHealthChecker,
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
			_, _, err = kpStore.Pod(kp.INTENT_TREE, types.NodeName(nodeName), podID)
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
	retryInterval = testFarmRetryInterval

	dsStore := dsstoretest.NewFake()
	kpStore := kptest.NewFakePodStore(make(map[kptest.FakePodStoreKey]manifest.Manifest), make(map[string]kp.WatchResult))
	applicator := labels.NewFakeApplicator()
	session := kptest.NewSession()
	firstLogger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"farm": "firstMultiple",
	})

	var allNodes []types.NodeName
	allNodes = append(allNodes, "node1", "node2", "node3")
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		allNodes = append(allNodes, types.NodeName(nodeName))
	}
	happyHealthChecker := fake_checker.HappyHealthChecker(allNodes)

	//
	// Instantiate first farm
	//
	firstFarm := &Farm{
		dsStore:       dsStore,
		kpStore:       kpStore,
		scheduler:     scheduler.NewApplicatorScheduler(applicator),
		applicator:    applicator,
		children:      make(map[ds_fields.ID]*childDS),
		session:       session,
		logger:        firstLogger,
		alerter:       alerting.NewNop(),
		healthChecker: &happyHealthChecker,
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
		dsStore:       dsStore,
		kpStore:       kpStore,
		scheduler:     scheduler.NewApplicatorScheduler(applicator),
		applicator:    applicator,
		children:      make(map[ds_fields.ID]*childDS),
		session:       session,
		logger:        secondLogger,
		alerter:       alerting.NewNop(),
		healthChecker: &happyHealthChecker,
	}
	secondQuitCh := make(chan struct{})
	defer close(secondQuitCh)
	go func() {
		go secondFarm.cleanupDaemonSetPods(secondQuitCh)
		secondFarm.mainLoop(secondQuitCh)
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
	dsData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")

	// Second daemon set
	anotherNodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az2"})
	anotherDSData, err := dsStore.Create(podManifest, minHealth, clusterName, anotherNodeSelector, podID, replicationTimeout)
	Assert(t).IsNil(err, "Expected no error creating request")

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

	// Make a third unschedulable node and verify it doesn't get scheduled by anything
	applicator.SetLabel(labels.NODE, "node3", pc_fields.AvailabilityZoneLabel, "undefined")

	labeled, err = waitForPodLabel(applicator, false, "node3/testPod")
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
	// Update a daemon set's node selector and expect a node to be unscheduled
	//
	mutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})
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

	//
	// Disabling a daemon set should not unschedule any nodes
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = true

		someSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})
		dsToUpdate.NodeSelector = someSelector
		return dsToUpdate, nil
	}
	_, err = dsStore.MutateDS(anotherDSData.ID, mutator)
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

func waitForDisabled(
	dsf *Farm,
	dsStore *dsstoretest.FakeDSStore,
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
		if _, ok := dsf.children[dsID]; ok {
			return nil
		}
		return util.Errorf("Farm does not have daemon set id")
	}
	return waitForCondition(condition)
}

// Polls for the farm to be populated by a daemon set with the same
// id, disabled value, and node selector as the daemon set in the argument
func waitForMutateSelector(dsf *Farm, ds ds_fields.DaemonSet) error {
	condition := func() error {
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

// Polls for a condition to happen, will return an error if it does not happen
// before the timeout
func waitForCondition(condition func() error) error {
	timeout := time.After(10 * time.Second)
	timedOut := false
	err := condition()

	for err != nil && !timedOut {
		select {
		case <-timeout:
			timedOut = true
		case <-time.Tick(100 * time.Millisecond):
			err = condition()
		}
	}
	return err
}
