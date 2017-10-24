// +build !race

package rc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/square/p2/pkg/alerting/alertingtest"
	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/health"
	fake_checker "github.com/square/p2/pkg/health/checker/test"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	. "github.com/anthonybishopric/gotcha"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	newTransferNode = types.NodeName("newNode")
)

type testRCStore interface {
	ReplicationControllerStore
	ReplicationControllerWatcher
	Create(
		manifest manifest.Manifest,
		nodeSelector klabels.Selector,
		availabilityZone pc_fields.AvailabilityZone,
		clusterName pc_fields.ClusterName,
		podLabels klabels.Set,
		additionalLabels klabels.Set,
		allocationStrategy fields.Strategy,
	) (fields.RC, error)
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

type testAuditLogStore interface {
	AuditLogStore
	List() (map[audit.ID]audit.AuditLog, error)
}

func (s testScheduler) EligibleNodes(manifest manifest.Manifest, nodeSelector klabels.Selector) ([]types.NodeName, error) {
	as := scheduler.NewApplicatorScheduler(s.applicator)
	return as.EligibleNodes(manifest, nodeSelector)
}

func (s testScheduler) AllocateNodes(manifest manifest.Manifest, nodeSelector klabels.Selector, allocationCount int) ([]types.NodeName, error) {
	if s.shouldErr {
		return nil, util.Errorf("Intentional error allocating nodes.")
	}

	err := s.applicator.SetLabel(labels.NODE, string(newTransferNode), "nodeQuality", "good")
	if err != nil {
		return nil, err
	}
	return []types.NodeName{newTransferNode}, nil
}

func (s testScheduler) DeallocateNodes(nodeSelector klabels.Selector, nodes []types.NodeName) error {
	return nil
}

type testScheduler struct {
	applicator testApplicator
	shouldErr  bool
}

func setup(t *testing.T) (
	rcStore testRCStore,
	consulStore testConsulStore,
	applicator testApplicator,
	rc *replicationController,
	alerter *alertingtest.AlertRecorder,
	auditLogStore testAuditLogStore,
	closeFn func(),
) {
	fixture := consulutil.NewFixture(t)
	closeFn = fixture.Stop
	applicator = labels.NewConsulApplicator(fixture.Client, 0, 0)

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

	statusStore := statusstore.NewConsul(fixture.Client)
	rcStatusStore := rcstatus.NewConsul(statusStore, consul.RCStatusNamespace)

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID("testPod")
	podManifest := manifestBuilder.GetManifest()

	nodeSelector := klabels.Everything().Add("nodeQuality", klabels.EqualsOperator, []string{"good"})
	podLabels := map[string]string{"podTest": "successful"}

	rcData, err := rcStore.Create(podManifest, nodeSelector, "some_az", "some_cn", podLabels, nil, "some_strategy")
	Assert(t).IsNil(err, "expected no error creating request")

	alerter = alertingtest.NewRecorder()
	auditLogStore = auditlogstore.NewConsulStore(fixture.Client.KV())

	healthChecker := fake_checker.NewSingleService("", nil)

	rc = New(
		rcData,
		consulStore,
		fixture.Client,
		rcStatusStore,
		auditLogStore,
		fixture.Client.KV(),
		rcStore,
		testScheduler{applicator, false},
		applicator,
		logging.DefaultLogger,
		alerter,
		healthChecker,
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
	timeout := time.After(5 * time.Second)
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
	_, consulStore, applicator, rc, alerter, _, closeFn := setup(t)
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
	rcStore, consulStore, applicator, rc, alerter, _, closeFn := setup(t)
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
	rcStore, consulStore, applicator, rc, alerter, auditLogStore, closeFn := setup(t)
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

	records, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected a single audit log record but there were %d", len(records))
	}
	for _, record := range records {
		if record.EventType != audit.RCRetargetingEvent {
			t.Errorf("expected audit log type to be %q but was %q", audit.RCRetargetingEvent, record.EventType)
		}
		var details audit.RCRetargetingDetails
		err = json.Unmarshal([]byte(*record.EventDetails), &details)
		if err != nil {
			t.Fatal(err)
		}
		if len(details.Nodes) != 1 {
			t.Error("expected one node")
		} else {
			if details.Nodes[0] != "node2" {
				t.Errorf("expected node list to only have %v but had %v", "node2", details.Nodes[0])
			}
		}
	}
}

func TestSchedulePartial(t *testing.T) {
	_, consulStore, applicator, rc, alerter, _, closeFn := setup(t)
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
	_, consulStore, applicator, rc, _, _, closeFn := setup(t)
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
	rcStore, consulStore, applicator, rc, alerter, _, closeFn := setup(t)
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
	rcStore, consulStore, applicator, rc, alerter, auditLogStore, closeFn := setup(t)
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

	records, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 audit log records but there were %d", len(records))
	}

	foundNone := false
	for _, record := range records {
		if record.EventType != audit.RCRetargetingEvent {
			t.Errorf("expected audit log type to be %q but was %q", audit.RCRetargetingEvent, record.EventType)
		}
		var details audit.RCRetargetingDetails
		err = json.Unmarshal([]byte(*record.EventDetails), &details)
		if err != nil {
			t.Fatal(err)
		}

		if len(details.Nodes) == 0 {
			if foundNone {
				t.Fatal("both audit records had no nodes in them")
			}
			foundNone = true
		} else {
			if len(details.Nodes) != 1 {
				t.Error("expected one node")
			} else {
				if details.Nodes[0] != "node2" {
					t.Errorf("expected node list to only have %v but had %v", "node2", details.Nodes[0])
				}
			}
		}
	}

	if !foundNone {
		t.Fatal("should have found an audit record with no nodes but didn't")
	}
}

func TestPreferUnscheduleIneligible(t *testing.T) {
	rcStore, consulStore, applicator, rc, alerter, _, closeFn := setup(t)
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
	_, kvStore, applicator, rc, alerter, _, closeFn := setup(t)
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
	_, kvStore, applicator, rc, alerter, _, closeFn := setup(t)
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
	_, kvStore, applicator, rc, alerter, _, closeFn := setup(t)
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
	_, _, applicator, rc, _, _, closeFn := setup(t)
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

func TestScheduleMoreThan5(t *testing.T) {
	rcStore, _, applicator, rc, _, _, closeFn := setup(t)
	defer closeFn()

	for i := 0; i < 7; i++ {
		err := applicator.SetLabel(labels.NODE, fmt.Sprintf("node%d", i), "nodeQuality", "good")
		if err != nil {
			t.Fatal(err)
		}
	}

	quit := make(chan struct{})
	errors := rc.WatchDesires(quit)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errors {
			t.Error(err)
		}
	}()

	rcStore.SetDesiredReplicas(rc.ID(), 7)

	numNodes := waitForNodes(t, rc, 7)
	Assert(t).AreEqual(numNodes, 7, "took too long to schedule")

	close(quit)
	wg.Wait()
}

func TestUnscheduleMoreThan5(t *testing.T) {
	rcStore, _, applicator, rc, _, _, closeFn := setup(t)
	defer closeFn()

	for i := 0; i < 7; i++ {
		err := applicator.SetLabel(labels.NODE, fmt.Sprintf("node%d", i), "nodeQuality", "good")
		if err != nil {
			t.Fatal(err)
		}
	}

	quit := make(chan struct{})
	errors := rc.WatchDesires(quit)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errors {
			t.Error(err)
		}
	}()

	rcStore.SetDesiredReplicas(rc.ID(), 7)

	numNodes := waitForNodes(t, rc, 7)
	Assert(t).AreEqual(numNodes, 7, "took too long to schedule")

	rcStore.SetDesiredReplicas(rc.ID(), 0)

	numNodes = waitForNodes(t, rc, 0)
	Assert(t).AreEqual(numNodes, 0, "took too long to unschedule")

	close(quit)
	wg.Wait()
}

func TestAlertIfNodeBecomesIneligibleIfNotCattleStrategy(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	err := testIneligibleNodesCommon(applicator, rc, alerter)
	if err != nil {
		t.Fatal(err)
	}

	if len(alerter.Alerts) != 1 {
		t.Fatalf("the RC should have alerted since replicas desired is greater than the number of eligible nodes, but there were %d alerts", len(alerter.Alerts))
	}
}

func TestAllocateOnIneligibleIfCattleStrategy(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	rc.AllocationStrategy = fields.CattleStrategy

	err := testIneligibleNodesCommon(applicator, rc, alerter)
	if err != nil {
		t.Fatal(err)
	}

	if len(alerter.Alerts) != 0 {
		t.Fatalf("the RC should not have alerted since it allocated cattle nodes, but there were %d alerts", len(alerter.Alerts))
	}

	status, _, _ := rc.rcStatusStore.Get(rc.ID())
	if status.NodeTransfer.NewNode != newTransferNode {
		t.Fatalf("the rc failed to update the node transfer status new node to %s from %s", newTransferNode, status.NodeTransfer.NewNode)
	}
}

func TestNoOpIfNodeTransferInProgress(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	rc.AllocationStrategy = fields.CattleStrategy

	// Simulate node transfer in progress with non-nil node transfer
	testStatus := rcstatus.Status{
		NodeTransfer: &rcstatus.NodeTransfer{
			OldNode: types.NodeName("old.123"),
			NewNode: types.NodeName("new.456"),
		},
	}
	err := rc.rcStatusStore.Set(rc.ID(), testStatus)
	if err != nil {
		t.Fatalf("Unexpected error putting in fake node transfer status")
	}

	err = testIneligibleNodesCommon(applicator, rc, alerter)
	if err != nil {
		t.Fatal(err)
	}

	if len(alerter.Alerts) != 0 {
		t.Fatalf("the RC should not have alerted since a transfer was in progress, but there were %d alerts", len(alerter.Alerts))
	}

	status, _, _ := rc.rcStatusStore.Get(rc.ID())
	if *(status.NodeTransfer) != *(testStatus.NodeTransfer) {
		t.Fatalf("the rc should not have updated the status from %v to %v", testStatus, status)
	}
}

func TestAlertIfCannotAllocateNodes(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	rc.AllocationStrategy = fields.CattleStrategy

	// Force an allocate nodes failure
	fixture := consulutil.NewFixture(t)
	closeFn = fixture.Stop
	applicator = labels.NewConsulApplicator(fixture.Client, 0, 0)
	rc.scheduler = testScheduler{applicator, true}

	err := testIneligibleNodesCommon(applicator, rc, alerter)
	if err == nil {
		t.Fatalf("Expected intentional error but there was none")
	}

	if len(alerter.Alerts) != 1 {
		t.Fatalf("the RC should have alerted since the scheduler could not allocate nodes, but there were %d alerts", len(alerter.Alerts))
	}
}

// testIneligibleNodesCommmon labels nodes and meets desires, then marks one as
// bad and meets desires again. It is the shared to code the establish
// the ineligible node state and cause the RC to act on it
func testIneligibleNodesCommon(applicator testApplicator, rc *replicationController, alerter *alertingtest.AlertRecorder) error {
	for i := 0; i < 7; i++ {
		err := applicator.SetLabel(labels.NODE, fmt.Sprintf("node%d", i), "nodeQuality", "good")
		if err != nil {
			return err
		}
	}

	rc.ReplicasDesired = 7

	err := rc.meetDesires()
	if err != nil {
		return err
	}

	current, err := rc.CurrentPods()
	if err != nil {
		return err
	}

	if len(current) != 7 {
		return util.Errorf("rc should have scheduled 7 pods but found %d", len(current))
	}

	if len(alerter.Alerts) != 0 {
		return util.Errorf("there shouldn't have been any alerts yet but there were %d", len(alerter.Alerts))
	}

	// now make one of the nodes ineligible, creating a situation where the
	// RC has 7 "current" nodes and 7 desired recplicas, but only 6 of
	// those nodes meet the node selector's criteria
	err = applicator.SetLabel(labels.NODE, "node3", "nodeQuality", "bad")
	if err != nil {
		return err
	}

	err = rc.meetDesires()
	if err != nil {
		return err
	}

	return nil
}

// Tests that an RC will not do any scheduling/unscheduling if the only thing
// that changes is the set of nodes that match the node selector. This might be
// counter-intuitive but we don't want an RC to risk an outage by swapping
// pods. For example imagine there is a single node in an RC, and that node
// becomes ineligible and another node becomes eligible. We require that the
// operator increase the RC replica count to 2 to deploy the eligible node, and
// then (likely after some time has passed or some application-specific
// conditions have been met) decrease the replica count back to 1 to unschedule
// the ineligible node.
func TestRCDoesNotFixMembership(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	if err != nil {
		t.Fatal(err)
	}

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
		t.Fatalf("rc should have scheduled 1 pods but found %d", len(current))
	}

	if len(alerter.Alerts) != 0 {
		t.Fatalf("there shouldn't have been any alerts yet but there were %d", len(alerter.Alerts))
	}

	// now mark node1 as ineligible and node2 as eligible. We want to test
	// that the RC does not take any action because replicas desired ==
	// len(current nodes)
	err = applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	if err != nil {
		t.Fatal(err)
	}
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	if err != nil {
		t.Fatal(err)
	}

	err = rc.meetDesires()
	if err != nil {
		t.Fatal(err)
	}

	current, err = rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(current) != 1 {
		t.Fatalf("RC should have still only had 1 node but it had %d", len(current))
	}

	if current[0].Node != "node1" {
		t.Fatalf("expected the RC to still consider node1 to be current, but the single node was %s", current[0].Node)
	}

	if len(alerter.Alerts) != 1 {
		t.Fatalf("the RC should have alerted since it has some current nodes that aren't eligible and is unable to correct this. There were %d alerts", len(alerter.Alerts))
	}
}

func TestTransferRolledBackByQuitCh(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	rc.AllocationStrategy = fields.CattleStrategy

	err := testIneligibleNodesCommon(applicator, rc, alerter)
	if err != nil {
		t.Fatal(err)
	}

	close(rc.nodeTransfer.quit)

	testRolledBackTransfer(rc, t)
}

func TestTransferRolledBackOnRCDisabled(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	rc.AllocationStrategy = fields.CattleStrategy

	err := testIneligibleNodesCommon(applicator, rc, alerter)
	if err != nil {
		t.Fatal(err)
	}

	rc.Disabled = true

	err = rc.meetDesires()
	if err != nil {
		t.Fatal(err)
	}

	testRolledBackTransfer(rc, t)
}

func TestTransferRolledBackOnReplicasDesiredDecrease(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	rc.AllocationStrategy = fields.CattleStrategy

	err := testIneligibleNodesCommon(applicator, rc, alerter)
	if err != nil {
		t.Fatal(err)
	}

	rc.ReplicasDesired = rc.ReplicasDesired - 1

	err = rc.meetDesires()
	if err != nil {
		t.Fatal(err)
	}

	testRolledBackTransfer(rc, t)
}

func testRolledBackTransfer(rc *replicationController, t *testing.T) {
	// Give async goroutine time to rollback transfer
	time.Sleep(1 * time.Second)

	status, _, err := rc.rcStatusStore.Get(rc.ID())
	if !statusstore.IsNoStatus(err) {
		t.Fatalf("Expected no node transfer status to exist, got %v", status)
	}

	man, _, err := rc.consulStore.Pod(consul.INTENT_TREE, newTransferNode, rc.Manifest.ID())
	if err != pods.NoCurrentManifest {
		t.Fatalf("Expected new node to have been erased from intent, but man was %v", man)
	}

	nilTransfer := nodeTransfer{}
	// We have to compare each field because we can't compare nilTransfer and
	// rc.nodeTransfer directly (nodeTransfer has a func() field)
	if rc.nodeTransfer.newNode != nilTransfer.newNode ||
		rc.nodeTransfer.oldNode != nilTransfer.oldNode ||
		rc.nodeTransfer.quit != nilTransfer.quit ||
		rc.nodeTransfer.unlocker != nilTransfer.unlocker {
		t.Fatalf("Expected rc.nodeTransfer to be %v, was %v", nilTransfer, rc.nodeTransfer)
	}
}

func TestNewTransferNodeCannotBeScheduledOnReplicasDesiredIncrease(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	rc.AllocationStrategy = fields.CattleStrategy

	err := testIneligibleNodesCommon(applicator, rc, alerter)
	if err != nil {
		t.Fatal(err)
	}

	eligible, err := rc.eligibleNodes()
	if err != nil {
		t.Fatal(err)
	}

	foundNewTransferNode := false
	for _, node := range eligible {
		if node == newTransferNode {
			foundNewTransferNode = true
		}
	}

	if !foundNewTransferNode {
		t.Fatal("new transfer node should've been eligible but it was not")
	}

	rc.ReplicasDesired = rc.ReplicasDesired + 1
	err = rc.meetDesires()
	if err == nil {
		t.Fatal("expected not enough replicas to meet desires")
	}

	if len(alerter.Alerts) != 1 {
		t.Fatalf("the RC should have alerted not enough replicas to meet desires. There were %d alerts", len(alerter.Alerts))
	}

	// Add another eligible node
	err = applicator.SetLabel(labels.NODE, "node7", "nodeQuality", "good")
	if err != nil {
		t.Fatal(err)
	}

	err = rc.meetDesires()
	if err != nil {
		t.Fatal("meetDesires should succeed, it now has an eligible node for addPods()")
	}

	current, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	for _, node := range current.Nodes() {
		if node == newTransferNode {
			t.Fatal("new transfer node should not be a current node")
		}
	}

}

func TestTransferNodeHappyPath(t *testing.T) {
	_, _, applicator, rc, alerter, _, closeFn := setup(t)
	defer closeFn()

	rc.AllocationStrategy = fields.CattleStrategy

	healthMap := map[types.NodeName]health.Result{
		newTransferNode: health.Result{Status: health.Passing},
	}
	rc.healthChecker = fake_checker.NewSingleService("", healthMap)

	err := testIneligibleNodesCommon(applicator, rc, alerter)
	if err != nil {
		t.Fatal(err)
	}

	// give async goroutine time to finish transfer
	time.Sleep(1 * time.Second)

	current, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	nodes := current.Nodes()
	foundNewTransferNode := false
	foundBadNode := false
	for _, node := range nodes {
		if node == newTransferNode {
			foundNewTransferNode = true
		}
		if node == types.NodeName("node3") {
			foundBadNode = true
		}
	}
	if !foundNewTransferNode {
		t.Fatal("Expected transferred node to be a current node but it is not")
	}

	if foundBadNode {
		t.Fatal("Expected to have dropped ineligible node but it is still a current node")
	}
}
