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
