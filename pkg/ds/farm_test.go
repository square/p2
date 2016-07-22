package ds

import (
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore/dsstoretest"
	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"

	. "github.com/anthonybishopric/gotcha"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	klabels "k8s.io/kubernetes/pkg/labels"
)

func waitForFarm() {
	// If the tests fail, increasing this value may fix your problem
	// One reason is beacause of the request rate put on the the
	// consulutil/watch's WatchDiff function which the farm --> dsStore uses
	time.Sleep(250 * time.Millisecond)
}

// Tests dsContends for changes to both daemon sets and nodes
func TestContendNodes(t *testing.T) {
	//
	// Instantiate farm
	//
	dsStore := dsstoretest.NewFake()
	kpStore := kptest.NewFakePodStore(make(map[kptest.FakePodStoreKey]manifest.Manifest), make(map[string]kp.WatchResult))
	applicator := labels.NewFakeApplicator()
	dsf := NewFarm(kpStore, dsStore, applicator, logging.DefaultLogger, nil)
	quitCh := make(chan struct{})
	defer close(quitCh)
	go func() {
		dsf.Start(quitCh)
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
	dsData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID)
	Assert(t).IsNil(err, "Expected no error creating request")

	// Make a node and verify that it was scheduled
	applicator.SetLabel(labels.NODE, "node1", pc_fields.AvailabilityZoneLabel, "az1")
	waitForFarm()

	labeled, err := dsf.applicator.GetLabels(labels.POD, "node1/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Expected pod to have a dsID label")
	dsID := labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make another daemon set with a contending AvailabilityZoneLabel and verify
	// that it gets disabled and that the node label does not change
	anotherDSData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID)
	Assert(t).AreNotEqual(dsData.ID.String(), anotherDSData.ID.String(), "Precondition failed")
	Assert(t).IsNil(err, "Expected no error creating request")

	// Wait for farm
	waitForFarm()

	// Labels should not have been overwritten
	labeled, err = dsf.applicator.GetLabels(labels.POD, "node1/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Expected pod to have a dsID label")
	anotherDSID := labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), anotherDSID, "Expected pod label not to be overwritten")

	// Expect the new daemon set to be disabled both in the farm and in the dsStore
	newDS, _, err := dsStore.Get(anotherDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsTrue(newDS.Disabled, "Expected daemon set to be disabled")
	Assert(t).IsTrue(dsf.children[newDS.ID].ds.IsDisabled(), "Expected daemon set to be disabled")

	//
	// Make a third daemon set and update its node selector to force a contend,
	// then verify that it has been disabled and the node hasn't been overwritten
	//
	anotherSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"undefined"})
	badDS, err := dsStore.Create(podManifest, minHealth, clusterName, anotherSelector, podID)
	Assert(t).IsNil(err, "Expected no error creating request")

	mutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.NodeSelector = nodeSelector
		return dsToUpdate, nil
	}

	newDS, err = dsStore.MutateDS(badDS.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	waitForFarm()

	// Labels should not have been overwritten
	labeled, err = dsf.applicator.GetLabels(labels.POD, "node1/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Expected pod to have a dsID label")
	anotherDSID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), anotherDSID, "Expected pod label not to be overwritten")

	newDS, _, err = dsStore.Get(anotherDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsTrue(newDS.Disabled, "Expected daemon set to be disabled")
	Assert(t).IsTrue(dsf.children[newDS.ID].ds.IsDisabled(), "Expected daemon set to be disabled")

}

// Tests dsContends for NodeSelectors
func TestContendSelectors(t *testing.T) {
	//
	// Instantiate farm
	//
	dsStore := dsstoretest.NewFake()
	kpStore := kptest.NewFakePodStore(make(map[kptest.FakePodStoreKey]manifest.Manifest), make(map[string]kp.WatchResult))
	applicator := labels.NewFakeApplicator()
	dsf := NewFarm(kpStore, dsStore, applicator, logging.DefaultLogger, nil)
	quitCh := make(chan struct{})
	defer close(quitCh)
	go func() {
		dsf.Start(quitCh)
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
	firstDSData, err := dsStore.Create(podManifest, minHealth, clusterName, everythingSelector, podID)
	Assert(t).IsNil(err, "Expected no error creating request")
	waitForFarm()
	secondDSData, err := dsStore.Create(podManifest, minHealth, clusterName, everythingSelector, podID)
	Assert(t).IsNil(err, "Expected no error creating request")
	waitForFarm()

	// Verify that only the second daemon set is disabled
	firstDS, _, err := dsStore.Get(firstDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsFalse(firstDS.Disabled, "Expected daemon set to be enabled")
	Assert(t).IsFalse(dsf.children[firstDS.ID].ds.IsDisabled(), "Expected daemon set to be enabled")

	secondDS, _, err := dsStore.Get(secondDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsTrue(secondDS.Disabled, "Expected daemon set to be disabled")
	Assert(t).IsTrue(dsf.children[secondDS.ID].ds.IsDisabled(), "Expected daemon set to be disabled")

	// Add another daemon set with different selector and verify it gets disabled
	someSelector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"nowhere"})
	thirdDSData, err := dsStore.Create(podManifest, minHealth, clusterName, someSelector, podID)
	Assert(t).IsNil(err, "Expected no error creating request")
	waitForFarm()

	thirdDS, _, err := dsStore.Get(thirdDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsTrue(thirdDS.Disabled, "Expected daemon set to be disabled")
	Assert(t).IsTrue(dsf.children[thirdDS.ID].ds.IsDisabled(), "Expected daemon set to be disabled")

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
	_, err = dsStore.MutateDS(firstDS.ID, disableMutator)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	waitForFarm()
	firstDS, _, err = dsStore.Get(firstDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsTrue(firstDS.Disabled, "Precondition failed: Expected daemon set to be disabled")
	Assert(t).IsTrue(dsf.children[firstDS.ID].ds.IsDisabled(), "Precondition failed: Expected daemon set to be disabled")

	// Enable second ds and verify it is enabled
	_, err = dsStore.MutateDS(secondDS.ID, enableMutator)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	waitForFarm()
	secondDS, _, err = dsStore.Get(secondDS.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsFalse(secondDS.Disabled, "Precondition failed: Expected daemon set to be enabled")
	Assert(t).IsFalse(dsf.children[secondDS.ID].ds.IsDisabled(), "Expected daemon set to be enabled")

	// Enabled third ds and verify it disabled because it contends with second ds
	_, err = dsStore.MutateDS(thirdDS.ID, enableMutator)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	waitForFarm()
	thirdDS, _, err = dsStore.Get(thirdDS.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsTrue(thirdDS.Disabled, "Expected daemon set to be disabled")
	Assert(t).IsTrue(dsf.children[thirdDS.ID].ds.IsDisabled(), "Expected daemon set to be disabled")

	//
	// Test equivalent selectors, fifth ds should contend with fourth
	//
	anotherPodID := types.PodID("anotherPodID")

	anotherManifestBuilder := manifest.NewBuilder()
	anotherManifestBuilder.SetID(anotherPodID)
	anotherPodManifest := manifestBuilder.GetManifest()

	equalSelector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az99"})

	fourthDSData, err := dsStore.Create(anotherPodManifest, minHealth, clusterName, equalSelector, anotherPodID)
	Assert(t).IsNil(err, "Expected no error creating request")
	waitForFarm()

	// Verify it is enabled
	fourthDS, _, err := dsStore.Get(fourthDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsFalse(fourthDS.Disabled, "Expected daemon set to be enabled")
	Assert(t).IsFalse(dsf.children[fourthDS.ID].ds.IsDisabled(), "Expected daemon set to be enabled")

	fifthDSData, err := dsStore.Create(anotherPodManifest, minHealth, clusterName, equalSelector, anotherPodID)
	Assert(t).IsNil(err, "Expected no error creating request")
	waitForFarm()

	// Verify it gets disabled for its contending selector
	fifthDS, _, err := dsStore.Get(fifthDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsTrue(fifthDS.Disabled, "Expected daemon set to be disabled")
	Assert(t).IsTrue(dsf.children[fifthDS.ID].ds.IsDisabled(), "Expected daemon set to be disabled")
}

func TestFarmSchedule(t *testing.T) {
	//
	// Instantiate farm
	//
	dsStore := dsstoretest.NewFake()
	kpStore := kptest.NewFakePodStore(make(map[kptest.FakePodStoreKey]manifest.Manifest), make(map[string]kp.WatchResult))
	applicator := labels.NewFakeApplicator()
	dsf := NewFarm(kpStore, dsStore, applicator, logging.DefaultLogger, nil)
	quitCh := make(chan struct{})
	defer close(quitCh)
	go func() {
		dsf.Start(quitCh)
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
	dsData, err := dsStore.Create(podManifest, minHealth, clusterName, nodeSelector, podID)
	Assert(t).IsNil(err, "Expected no error creating request")

	// Second daemon set
	anotherNodeSelector := klabels.Everything().Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{"az2"})
	anotherDSData, err := dsStore.Create(podManifest, minHealth, clusterName, anotherNodeSelector, podID)
	Assert(t).IsNil(err, "Expected no error creating request")

	// Make a node and verify that it was scheduled by the first daemon set
	applicator.SetLabel(labels.NODE, "node1", pc_fields.AvailabilityZoneLabel, "az1")
	waitForFarm()

	labeled, err := dsf.applicator.GetLabels(labels.POD, "node1/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Expected pod to have a dsID label")
	dsID := labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(dsData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make a second node and verify that it was scheduled by the second daemon set
	applicator.SetLabel(labels.NODE, "node2", pc_fields.AvailabilityZoneLabel, "az2")
	waitForFarm()

	labeled, err = dsf.applicator.GetLabels(labels.POD, "node2/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Expected pod to have a dsID label")
	dsID = labeled.Labels.Get(DSIDLabel)
	Assert(t).AreEqual(anotherDSData.ID.String(), dsID, "Unexpected dsID labeled")

	// Make a third unschedulable node and verify it doesn't get scheduled by anything
	applicator.SetLabel(labels.NODE, "node3", pc_fields.AvailabilityZoneLabel, "undefined")
	waitForFarm()

	labeled, err = dsf.applicator.GetLabels(labels.POD, "node3/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsFalse(labeled.Labels.Has(DSIDLabel), "Expected pod not to have a dsID label")

	// Now add 10 new nodes and verify that they are scheduled by the first daemon set
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("good_node%v", i)
		err := applicator.SetLabel(labels.NODE, nodeName, pc_fields.AvailabilityZoneLabel, "az1")
		Assert(t).IsNil(err, "expected no error labeling node")
	}
	waitForFarm()
	for i := 0; i < 10; i++ {
		podPath := fmt.Sprintf("good_node%v/testPod", i)
		labeled, err = dsf.applicator.GetLabels(labels.POD, podPath)
		Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Expected pod to have a dsID label")
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
	_, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	waitForFarm()

	// Verify node2 is unscheduled
	labeled, err = dsf.applicator.GetLabels(labels.POD, "node2/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsFalse(labeled.Labels.Has(DSIDLabel), "Expected pod not to have a dsID label")

	//
	// Now update the node selector to schedule node2 again and verify
	//
	mutator = func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.NodeSelector = anotherNodeSelector
		return dsToUpdate, nil
	}
	_, err = dsStore.MutateDS(anotherDSData.ID, mutator)
	Assert(t).IsNil(err, "Expected no error mutating daemon set")
	waitForFarm()

	// Verify node2 is scheduled
	labeled, err = dsf.applicator.GetLabels(labels.POD, "node2/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Expected pod to have a dsID label")
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
	waitForFarm()

	// Verify it is disabled
	anotherDSData, _, err = dsStore.Get(anotherDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsTrue(anotherDSData.Disabled, "Expected daemon set to be disabled")
	Assert(t).IsTrue(dsf.children[anotherDSData.ID].ds.IsDisabled(), "Expected daemon set to be disabled")

	// Verify node2 is scheduled
	labeled, err = dsf.applicator.GetLabels(labels.POD, "node2/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsTrue(labeled.Labels.Has(DSIDLabel), "Expected pod to have a dsID label")
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
	waitForFarm()

	// Verify it is enabled
	anotherDSData, _, err = dsStore.Get(anotherDSData.ID)
	Assert(t).IsNil(err, "Expected no error getting daemon set")
	Assert(t).IsFalse(anotherDSData.Disabled, "Expected daemon set to be enabled")
	Assert(t).IsFalse(dsf.children[anotherDSData.ID].ds.IsDisabled(), "Expected daemon set to be enabled")

	// Verify node2 is unscheduled
	labeled, err = dsf.applicator.GetLabels(labels.POD, "node2/testPod")
	Assert(t).IsNil(err, "Expected no error getting labels")
	Assert(t).IsFalse(labeled.Labels.Has(DSIDLabel), "Expected pod not to have a dsID label")
}
