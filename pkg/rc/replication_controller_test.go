// +build !race

package rc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/alerting/alertingtest"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/types"

	. "github.com/anthonybishopric/gotcha"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type testRCStore interface {
	ReplicationControllerStore
	ReplicationControllerWatcher
	Create(manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (fields.RC, error)
	SetDesiredReplicas(id fields.ID, n int) error
}

type testConsulStore interface {
	consulStore
	AllPods(podPrefix consul.PodPrefix) ([]consul.ManifestResult, time.Duration, error)
	SetPod(podPrefix consul.PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
	DeletePod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (time.Duration, error)
}

// union of labels.Applicator and Labeler.
type testApplicator interface {
	labels.Applicator
	RemoveLabelsTxn(ctx context.Context, labelType labels.Type, id string, keysToRemove []string) error
	SetLabelsTxn(ctx context.Context, labelType labels.Type, id string, values map[string]string) error
}

func setup(t *testing.T) (
	rcStore testRCStore,
	consulStore testConsulStore,
	applicator testApplicator,
	rc *replicationController,
	alerter *alertingtest.AlertRecorder,
	closeFn func(),
) {
	fixture := consulutil.NewFixture(t)
	closeFn = fixture.Stop
	applicator = labels.NewConsulApplicator(fixture.Client, 0)

	// set a bogus label so we don't get "no labels" errors
	err := applicator.SetLabel(labels.POD, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}
	err = applicator.SetLabel(labels.NODE, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}
	rcStore = rcstore.NewConsul(fixture.Client, applicator, 0)
	consulStore = consul.NewConsulStore(fixture.Client)

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID("testPod")
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add("nodeQuality", klabels.EqualsOperator, []string{"good"})
	podLabels := map[string]string{"podTest": "successful"}

	rcData, err := rcStore.Create(podManifest, nodeSelector, podLabels)
	Assert(t).IsNil(err, "expected no error creating request")

	alerter = alertingtest.NewRecorder()

	rc = New(
		rcData,
		consulStore,
		fixture.Client.KV(),
		rcStore,
		scheduler.NewApplicatorScheduler(applicator),
		applicator,
		logging.DefaultLogger,
		alerter,
	).(*replicationController)

	return
}

func scheduledPods(t *testing.T, pods labels.Applicator) []labels.Labeled {
	podSelector := klabels.Everything().Add("podTest", klabels.EqualsOperator, []string{"successful"})
	labeled, err := pods.GetMatches(podSelector, labels.POD, false)
	Assert(t).IsNil(err, "expected no error matching pods")
	return labeled
}

func waitForNodes(t *testing.T, rc ReplicationController, desired int) int {
	timeout := time.After(1 * time.Second)
	current, err := rc.CurrentPods()
	Assert(t).IsNil(err, "expected no error getting current nodes")
	timedOut := false

	for len(current) != desired && !timedOut {
		select {
		case <-time.Tick(100 * time.Millisecond):
			// TODO: this tick within the loop means we are constantly rechecking something.
			// Does this imply we want a rc.WatchCurrentNodes() ?
			var err error
			current, err = rc.CurrentPods()
			Assert(t).IsNil(err, "expected no error getting current nodes")
		case <-timeout:
			timedOut = true
		}
	}
	return len(current)
}

func TestDoNothing(t *testing.T) {
	_, consulStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()

	err := rc.meetDesires()
	Assert(t).IsNil(err, "expected no error meeting")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 0, "expected no pods to have been labeled")
	manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 0 {
		t.Fatalf("expected no manifests to have been scheduled but there were %d", len(manifests))
	}
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to have occurred")
}

func TestCantSchedule(t *testing.T) {
	rcStore, consulStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()

	quit := make(chan struct{})
	errors := rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.ID(), 1)

	select {
	case rcErr := <-errors:
		scheduled := scheduledPods(t, applicator)
		Assert(t).AreEqual(len(scheduled), 0, "expected no pods to have been labeled")
		manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
		if err != nil {
			t.Fatal(err)
		}
		if len(manifests) != 0 {
			t.Fatalf("expected no manifests to have been scheduled but there were %d", len(manifests))
		}

		if len(alerter.Alerts) < 1 {
			t.Fatalf("Expected an alert to fire due to not enough nodes being scheduled, but instead got %s", rcErr)
		}
	case <-time.After(1 * time.Second):
		Assert(t).Fail("took too long to receive error")
	}
}

func TestSchedule(t *testing.T) {
	rcStore, consulStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	quit := make(chan struct{})
	defer close(quit)
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.ID(), 1)
	numNodes := waitForNodes(t, rc, 1)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 1, "expected a pod to have been labeled")
	Assert(t).AreEqual(scheduled[0].ID, "node2/testPod", "expected pod labeled on the right node")

	manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range manifests {
		if v.PodLocation.PodID != "testPod" {
			t.Errorf("expected manifest to be scheduled with pod id %s but was scheduled with %s", "testPod", v.PodLocation.PodID)
		}
		if v.PodLocation.Node != "node2" {
			t.Errorf("expected manifest to be scheduled on %s but was scheduled on %s", "node2", v.PodLocation.Node)
		}

		Assert(t).AreEqual(string(v.Manifest.ID()), "testPod", "expected manifest with correct ID")
	}

	Assert(t).AreEqual(len(alerter.Alerts), 0, "Expected no alerts to fire")
}

func TestSchedulePartial(t *testing.T) {
	_, consulStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	rc.ReplicasDesired = 2

	err = rc.meetDesires()
	if err == nil {
		t.Fatal("expected an error when there weren't enough nodes to schedule")
	}

	current, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 {
		t.Errorf("expected 1 node to be scheduled but there were %d", len(current))
	}

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 1, "expected a pod to have been labeled")
	Assert(t).AreEqual(scheduled[0].ID, "node2/testPod", "expected pod labeled on the right node")

	manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range manifests {
		if v.PodLocation.PodID != "testPod" {
			t.Errorf("expected manifest to be scheduled with pod id %s but was scheduled with %s", "testPod", v.PodLocation.PodID)
		}
		if v.PodLocation.Node != "node2" {
			t.Errorf("expected manifest to be scheduled on %s but was scheduled on %s", "node2", v.PodLocation.Node)
		}

		Assert(t).AreEqual(string(v.Manifest.ID()), "testPod", "expected manifest with correct ID")
	}

	Assert(t).AreEqual(len(alerter.Alerts), 1, "Expected an alert due to there not being enough nodes to schedule on")
}

func TestUnschedulePartial(t *testing.T) {
	_, consulStore, applicator, rc, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	rc.ReplicasDesired = 1

	err = rc.meetDesires()
	if err != nil {
		t.Fatal(err)
	}

	current, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 {
		t.Errorf("expected 1 node to be scheduled but there were %d", len(current))
	}

	// we would never set this to a negative number in production but we're testing an error path here
	rc.ReplicasDesired = -1
	err = rc.meetDesires()
	if err == nil {
		t.Fatal("expected an error setting replicas desired to a negative number because there aren't enough nodes to unschedule to meet such a request")
	}

	manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 0 {
		t.Fatalf("expected all manifests to be unscheduled even though there weren't enough to unschedule, but there were %d", len(manifests))
	}
}

func TestScheduleTwice(t *testing.T) {
	rcStore, consulStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	quit := make(chan struct{})
	defer close(quit)
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.ID(), 1)
	numNodes := waitForNodes(t, rc, 1)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule first")

	rcStore.SetDesiredReplicas(rc.ID(), 2)
	numNodes = waitForNodes(t, rc, 2)
	Assert(t).AreEqual(numNodes, 2, "took too long to schedule second")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 2, "expected two pods to have been labeled")
	firstPossibleOrdering := scheduled[0].ID == "node1/testPod" && scheduled[1].ID == "node2/testPod"
	secondPossibleOrdering := scheduled[0].ID == "node2/testPod" && scheduled[1].ID == "node1/testPod"
	if !firstPossibleOrdering && !secondPossibleOrdering {
		Assert(t).Fail("expected manifests to have been scheduled on both nodes")
	}

	manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	Assert(t).AreEqual(len(manifests), 2, "expected two manifests to have been scheduled")
	for _, v := range manifests {
		if v.PodLocation.PodID != "testPod" || (v.PodLocation.Node != "node1" && v.PodLocation.Node != "node2") {
			t.Errorf("expected manifest to be scheduled with pod id %s but was scheduled with %s", "testPod", v.PodLocation.PodID)
		}
		Assert(t).AreEqual(string(v.Manifest.ID()), "testPod", "expected manifest with correct ID")
	}
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestUnschedule(t *testing.T) {
	rcStore, consulStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	quit := make(chan struct{})
	defer close(quit)
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.ID(), 1)
	numNodes := waitForNodes(t, rc, 1)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 1, "expected a pod to have been labeled")
	manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	Assert(t).AreEqual(len(manifests), 1, "expected a manifest to have been scheduled")

	rcStore.SetDesiredReplicas(rc.ID(), 0)
	numNodes = waitForNodes(t, rc, 0)
	Assert(t).AreEqual(numNodes, 0, "took too long to unschedule")

	scheduled = scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 0, "expected a pod to have been unlabeled")
	manifests, _, err = consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	Assert(t).AreEqual(len(manifests), 0, "expected manifest to have been unscheduled")
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestPreferUnscheduleIneligible(t *testing.T) {
	rcStore, consulStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()
	for i := 0; i < 1000; i++ {
		nodeName := fmt.Sprintf("node%d", i)
		err := applicator.SetLabel(labels.NODE, nodeName, "nodeQuality", "good")
		Assert(t).IsNil(err, "expected no error labeling "+nodeName)
	}

	quit := make(chan struct{})
	defer close(quit)
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.ID(), 1000)
	numNodes := waitForNodes(t, rc, 1000)
	Assert(t).AreEqual(numNodes, 1000, "took too long to schedule")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 1000, "expected 1000 pods to have been labeled")
	manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	Assert(t).AreEqual(len(manifests), 1000, "expected a manifest to have been scheduled on 1000 nodes")

	// Make node503 ineligible, so that it will be preferred for unscheduling
	// when we decrease ReplicasDesired
	err = applicator.SetLabel(labels.NODE, "node503", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error marking node503 as bad")

	rcStore.SetDesiredReplicas(rc.ID(), 999)
	numNodes = waitForNodes(t, rc, 999)
	Assert(t).AreEqual(numNodes, 999, "took too long to unschedule")

	current, err := rc.CurrentPods()
	Assert(t).IsNil(err, "expected no error finding current nodes for rc")
	for _, pod := range current {
		Assert(t).AreNotEqual(pod.Node, "node503", "node503 should have been the one unscheduled, but it's still present")
	}
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestConsistencyNoChange(t *testing.T) {
	_, kvStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()
	rcSHA, _ := rc.Manifest.SHA()
	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rc.ReplicasDesired = 1
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	numNodes := waitForNodes(t, rc, 1)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule")

	// Verify that the node is consistent
	manifest, _, err := kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ := manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller did not set intent initially")

	// Make no changes

	// The controller shouldn't alter the node
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err = kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ = manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller modified the node's intent")
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestConsistencyModify(t *testing.T) {
	_, kvStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()
	rcSHA, _ := rc.Manifest.SHA()
	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rc.ReplicasDesired = 1
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")

	// Modify the intent manifest
	b := rc.Manifest.GetBuilder()
	b.SetConfig(map[interface{}]interface{}{"test": true})
	manifest2 := b.GetManifest()
	sha2, _ := manifest2.SHA()
	Assert(t).AreNotEqual(rcSHA, sha2, "failed to set different intent manifest")
	kvStore.SetPod(consul.INTENT_TREE, "node1", manifest2)

	// Controller should force the node back to the canonical manifest
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err := kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ := manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller did not reset intent")
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestConsistencyDelete(t *testing.T) {
	_, kvStore, applicator, rc, alerter, closeFn := setup(t)
	defer closeFn()
	rcSHA, _ := rc.Manifest.SHA()
	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rc.ReplicasDesired = 1
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")

	// Delete the intent manifest
	_, err = kvStore.DeletePod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "unexpected error deleting intent manifest")
	_, _, err = kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).AreEqual(pods.NoCurrentManifest, err, "unexpected pod result")

	// Controller should force the node back to the canonical manifest
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err := kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ := manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller did not reset intent")
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestReservedLabels(t *testing.T) {
	_, _, applicator, rc, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rc.ReplicasDesired = 1
	err = rc.meetDesires()
	if err != nil {
		t.Fatalf("unexpected error scheduling nodes: %s", err)
	}

	labeled, err := applicator.GetLabels(labels.POD, labels.MakePodLabelKey("node1", "testPod"))
	Assert(t).IsNil(err, "unexpected error getting pod labels")

	Assert(t).AreEqual(labeled.Labels[rcstore.PodIDLabel], "testPod", "Pod label not set as expected")
	Assert(t).AreEqual(labeled.Labels[RCIDLabel], rc.ID().String(), "RC label not set as expected")
}
