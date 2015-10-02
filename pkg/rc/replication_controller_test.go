package rc

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/pods"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
)

type fakeKpStore struct {
	manifests map[string]pods.Manifest
}

func (s *fakeKpStore) SetPod(key string, manifest pods.Manifest) (time.Duration, error) {
	s.manifests[key] = manifest
	return 0, nil
}

func (s *fakeKpStore) DeletePod(key string) (time.Duration, error) {
	delete(s.manifests, key)
	return 0, nil
}

func setup(t *testing.T) (
	rcStore rcstore.Store,
	kpStore fakeKpStore,
	applicator labels.Applicator,
	rc ReplicationController) {

	rcStore = rcstore.NewFake()

	manifestBuilder := pods.NewManifestBuilder()
	manifestBuilder.SetID("testPod")
	manifest := manifestBuilder.GetManifest()

	nodeSelector := labels.Everything().Add("nodeQuality", labels.EqualsOperator, []string{"good"})
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
	)

	return
}

func scheduledPods(t *testing.T, pods labels.Applicator) []labels.Labeled {
	podSelector := labels.Everything().Add("podTest", labels.EqualsOperator, []string{"successful"})
	labeled, err := pods.GetMatches(podSelector, labels.POD)
	Assert(t).IsNil(err, "expected no error matching pods")
	return labeled
}

func waitForNodes(t *testing.T, rc ReplicationController, desired int) int {
	timeout := time.After(1 * time.Second)
	currentNodes, err := rc.CurrentNodes()
	Assert(t).IsNil(err, "expected no error getting current nodes")
	timedOut := false

	for len(currentNodes) != desired && !timedOut {
		select {
		case <-time.Tick(100 * time.Millisecond):
			// TODO: this tick within the loop means we are constantly rechecking something.
			// Does this imply we want a rc.WatchCurrentNodes() ?
			var err error
			currentNodes, err = rc.CurrentNodes()
			Assert(t).IsNil(err, "expected no error getting current nodes")
		case <-timeout:
			timedOut = true
		}
	}
	return len(currentNodes)
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
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.ID(), 1)
	numNodes := waitForNodes(t, rc, 1)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 1, "expected a pod to have been labeled")
	Assert(t).AreEqual(scheduled[0].ID, "node2/testPod", "expected pod labeled on the right node")

	for k, v := range kp.manifests {
		Assert(t).AreEqual(k, "intent/node2/testPod", "expected manifest scheduled on the right node")
		Assert(t).AreEqual(v.ID(), "testPod", "expected manifest with correct ID")
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
		Assert(t).AreEqual(v.ID(), "testPod", "expected manifest with correct ID")
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
		Assert(t).AreEqual(v.ID(), "testPod", "expected manifest with correct ID")
	}
}

func TestUnschedule(t *testing.T) {
	rcStore, kp, applicator, rc := setup(t)

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	quit := make(chan struct{})
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
