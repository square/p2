package rc

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

type fakeKpStore struct {
	manifests map[string]pods.Manifest
}

func (s *fakeKpStore) SetPod(podPrefix kp.PodPrefix, nodeName string, manifest pods.Manifest) (time.Duration, error) {
	key := path.Join(string(podPrefix), nodeName, string(manifest.ID()))
	s.manifests[key] = manifest
	return 0, nil
}

func (s *fakeKpStore) DeletePod(podPrefix kp.PodPrefix, nodeName string, podID types.PodID) (time.Duration, error) {
	key := path.Join(string(podPrefix), nodeName, string(podID))
	delete(s.manifests, key)
	return 0, nil
}

func (s *fakeKpStore) Pod(podPrefix kp.PodPrefix, nodeName string, podID types.PodID) (
	pods.Manifest, time.Duration, error) {
	key := path.Join(string(podPrefix), nodeName, string(podID))
	if manifest, ok := s.manifests[key]; ok {
		return manifest, 0, nil
	}
	return nil, 0, pods.NoCurrentManifest
}

func setup(t *testing.T) (
	rcStore rcstore.Store,
	kpStore fakeKpStore,
	applicator labels.Applicator,
	rc *replicationController) {

	rcStore = rcstore.NewFake()

	manifestBuilder := pods.NewManifestBuilder()
	manifestBuilder.SetID("testPod")
	manifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add("nodeQuality", klabels.EqualsOperator, []string{"good"})
	podLabels := map[string]string{"podTest": "successful"}

	rcData, err := rcStore.Create(manifest, nodeSelector, podLabels)
	Assert(t).IsNil(err, "expected no error creating request")

	kpStore = fakeKpStore{manifests: make(map[string]pods.Manifest)}
	applicator = labels.NewFakeApplicator()

	rc = New(
		rcData,
		&kpStore,
		rcStore,
		NewApplicatorScheduler(applicator),
		applicator,
		logging.DefaultLogger,
	).(*replicationController)

	return
}

func scheduledPods(t *testing.T, pods labels.Applicator) []labels.Labeled {
	podSelector := klabels.Everything().Add("podTest", klabels.EqualsOperator, []string{"successful"})
	labeled, err := pods.GetMatches(podSelector, labels.POD)
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
	_, kp, applicator, rc := setup(t)

	err := rc.meetDesires()
	Assert(t).IsNil(err, "expected no error meeting")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 0, "expected no pods to have been labeled")
	Assert(t).AreEqual(len(kp.manifests), 0, "expected no manifests to have been scheduled")
}

func TestCantSchedule(t *testing.T) {
	rcStore, kp, applicator, rc := setup(t)

	quit := make(chan struct{})
	errors := rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.ID(), 1)

	select {
	case <-errors:
		scheduled := scheduledPods(t, applicator)
		Assert(t).AreEqual(len(scheduled), 0, "expected no pods to have been labeled")
		Assert(t).AreEqual(len(kp.manifests), 0, "expected no manifests to have been scheduled")
	case <-time.After(1 * time.Second):
		Assert(t).Fail("took too long to receive error")
	}
}

func TestSchedule(t *testing.T) {
	rcStore, kp, applicator, rc := setup(t)

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

	for k, v := range kp.manifests {
		Assert(t).AreEqual(k, "intent/node2/testPod", "expected manifest scheduled on the right node")
		Assert(t).AreEqual(string(v.ID()), "testPod", "expected manifest with correct ID")
	}
}

func TestSchedulePartial(t *testing.T) {
	rcStore, kp, applicator, rc := setup(t)

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	quit := make(chan struct{})
	errors := rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.ID(), 2)
	numNodes := waitForNodes(t, rc, 1)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 1, "expected a pod to have been labeled")
	Assert(t).AreEqual(scheduled[0].ID, "node2/testPod", "expected pod labeled on the right node")

	for k, v := range kp.manifests {
		Assert(t).AreEqual(k, "intent/node2/testPod", "expected manifest scheduled on the right node")
		Assert(t).AreEqual(string(v.ID()), "testPod", "expected manifest with correct ID")
	}

	select {
	case <-errors:
		// Good, we should receive an error (not enough nodes to meet desire)
	case <-time.After(1 * time.Second):
		Assert(t).Fail("took too long to receive error")
	}
}

func TestScheduleTwice(t *testing.T) {
	rcStore, kp, applicator, rc := setup(t)

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

	Assert(t).AreEqual(len(kp.manifests), 2, "expected two manifests to have been scheduled")
	for k, v := range kp.manifests {
		if k != "intent/node1/testPod" && k != "intent/node2/testPod" {
			Assert(t).Fail("expected manifest scheduled on the right node")
		}
		Assert(t).AreEqual(string(v.ID()), "testPod", "expected manifest with correct ID")
	}
}

func TestUnschedule(t *testing.T) {
	rcStore, kp, applicator, rc := setup(t)

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
	Assert(t).AreEqual(len(kp.manifests), 1, "expected a manifest to have been scheduled")

	rcStore.SetDesiredReplicas(rc.ID(), 0)
	numNodes = waitForNodes(t, rc, 0)
	Assert(t).AreEqual(numNodes, 0, "took too long to unschedule")

	scheduled = scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 0, "expected a pod to have been unlabeled")
	Assert(t).AreEqual(len(kp.manifests), 0, "expected manifest to have been unscheduled")
}

func TestPreferUnscheduleIneligible(t *testing.T) {
	rcStore, kp, applicator, rc := setup(t)
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
	Assert(t).AreEqual(len(kp.manifests), 1000, "expected a manifest to have been scheduled on 1000 nodes")

	// Make node503 ineligible, so that it will be preferred for unscheduling
	// when we decrease ReplicasDesired
	err := applicator.SetLabel(labels.NODE, "node503", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error marking node503 as bad")

	rcStore.SetDesiredReplicas(rc.ID(), 999)
	numNodes = waitForNodes(t, rc, 999)
	Assert(t).AreEqual(numNodes, 999, "took too long to unschedule")

	current, err := rc.CurrentPods()
	Assert(t).IsNil(err, "expected no error finding current nodes for rc")
	for _, pod := range current {
		Assert(t).AreNotEqual(pod.Node, "node503", "node503 should have been the one unscheduled, but it's still present")
	}
}

func TestConsistencyNoChange(t *testing.T) {
	_, kvStore, applicator, rc := setup(t)
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
	manifest, _, err := kvStore.Pod(kp.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ := manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller did not set intent initially")

	// Make no changes

	// The controller shouldn't alter the node
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err = kvStore.Pod(kp.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ = manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller modified the node's intent")
}

func TestConsistencyModify(t *testing.T) {
	_, kvStore, applicator, rc := setup(t)
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
	kvStore.SetPod(kp.INTENT_TREE, "node1", manifest2)

	// Controller should force the node back to the canonical manifest
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err := kvStore.Pod(kp.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ := manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller did not reset intent")
}

func TestConsistencyDelete(t *testing.T) {
	_, kvStore, applicator, rc := setup(t)
	rcSHA, _ := rc.Manifest.SHA()
	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rc.ReplicasDesired = 1
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")

	// Delete the intent manifest
	_, err = kvStore.DeletePod(kp.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "unexpected error deleting intent manifest")
	_, _, err = kvStore.Pod(kp.INTENT_TREE, "node1", "testPod")
	Assert(t).AreEqual(pods.NoCurrentManifest, err, "unexpected pod result")

	// Controller should force the node back to the canonical manifest
	err = rc.meetDesires()
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err := kvStore.Pod(kp.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ := manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller did not reset intent")
}
