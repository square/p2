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
	rcStatusStore rcstatus.ConsulStore,
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
	rcStatusStore = rcstatus.NewConsul(statusStore, consul.RCStatusNamespace)

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
		rcData.ID,
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
	rcStore, consulStore, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	err = rc.meetDesires(rcFields)
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
	rcStore, consulStore, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	quit := make(chan struct{})
	errors := rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.rcID, 1)

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
	rcStore, consulStore, applicator, rc, alerter, auditLogStore, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	quit := make(chan struct{})
	defer close(quit)
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.rcID, 1)
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
	rcStore, consulStore, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.ReplicasDesired = 2

	err = rc.meetDesires(rcFields)
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
	rcStore, consulStore, applicator, rc, _, _, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.ReplicasDesired = 1

	err = rc.meetDesires(rcFields)
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
	rcFields.ReplicasDesired = -1
	err = rc.meetDesires(rcFields)
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
	rcStore, consulStore, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	quit := make(chan struct{})
	defer close(quit)
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.rcID, 1)
	numNodes := waitForNodes(t, rc, 1)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule first")

	rcStore.SetDesiredReplicas(rc.rcID, 2)
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
	rcStore, consulStore, applicator, rc, alerter, auditLogStore, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "bad")
	Assert(t).IsNil(err, "expected no error labeling node1")
	err = applicator.SetLabel(labels.NODE, "node2", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error labeling node2")

	quit := make(chan struct{})
	defer close(quit)
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.rcID, 1)
	numNodes := waitForNodes(t, rc, 1)
	Assert(t).AreEqual(numNodes, 1, "took too long to schedule")

	scheduled := scheduledPods(t, applicator)
	Assert(t).AreEqual(len(scheduled), 1, "expected a pod to have been labeled")
	manifests, _, err := consulStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatal(err)
	}
	Assert(t).AreEqual(len(manifests), 1, "expected a manifest to have been scheduled")

	rcStore.SetDesiredReplicas(rc.rcID, 0)
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
	rcStore, consulStore, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()
	for i := 0; i < 1000; i++ {
		nodeName := fmt.Sprintf("node%d", i)
		err := applicator.SetLabel(labels.NODE, nodeName, "nodeQuality", "good")
		Assert(t).IsNil(err, "expected no error labeling "+nodeName)
	}

	quit := make(chan struct{})
	defer close(quit)
	rc.WatchDesires(quit)

	rcStore.SetDesiredReplicas(rc.rcID, 1000)
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

	rcStore.SetDesiredReplicas(rc.rcID, 999)
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
	rcStore, kvStore, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcSHA, _ := rcFields.Manifest.SHA()
	err = applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rcFields.ReplicasDesired = 1
	err = rc.meetDesires(rcFields)
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
	err = rc.meetDesires(rcFields)
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err = kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ = manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller modified the node's intent")
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestConsistencyModify(t *testing.T) {
	rcStore, kvStore, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcSHA, _ := rcFields.Manifest.SHA()

	err = applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rcFields.ReplicasDesired = 1
	err = rc.meetDesires(rcFields)
	Assert(t).IsNil(err, "unexpected error scheduling nodes")

	// Modify the intent manifest
	b := rcFields.Manifest.GetBuilder()
	b.SetConfig(map[interface{}]interface{}{"test": true})
	manifest2 := b.GetManifest()
	sha2, _ := manifest2.SHA()
	Assert(t).AreNotEqual(rcSHA, sha2, "failed to set different intent manifest")
	kvStore.SetPod(consul.INTENT_TREE, "node1", manifest2)

	// Controller should force the node back to the canonical manifest
	err = rc.meetDesires(rcFields)
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err := kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ := manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller did not reset intent")
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestConsistencyDelete(t *testing.T) {
	rcStore, kvStore, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcSHA, _ := rcFields.Manifest.SHA()
	err = applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rcFields.ReplicasDesired = 1
	err = rc.meetDesires(rcFields)
	Assert(t).IsNil(err, "unexpected error scheduling nodes")

	// Delete the intent manifest
	_, err = kvStore.DeletePod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "unexpected error deleting intent manifest")
	_, _, err = kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).AreEqual(pods.NoCurrentManifest, err, "unexpected pod result")

	// Controller should force the node back to the canonical manifest
	err = rc.meetDesires(rcFields)
	Assert(t).IsNil(err, "unexpected error scheduling nodes")
	manifest, _, err := kvStore.Pod(consul.INTENT_TREE, "node1", "testPod")
	Assert(t).IsNil(err, "could not fetch intent")
	sha, _ := manifest.SHA()
	Assert(t).AreEqual(rcSHA, sha, "controller did not reset intent")
	Assert(t).AreEqual(len(alerter.Alerts), 0, "expected no alerts to fire")
}

func TestReservedLabels(t *testing.T) {
	rcStore, _, applicator, rc, _, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	err = applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	Assert(t).IsNil(err, "expected no error assigning label")

	// Install manifest on a single node
	rcFields.ReplicasDesired = 1
	err = rc.meetDesires(rcFields)
	if err != nil {
		t.Fatalf("unexpected error scheduling nodes: %s", err)
	}

	labeled, err := applicator.GetLabels(labels.POD, labels.MakePodLabelKey("node1", "testPod"))
	Assert(t).IsNil(err, "unexpected error getting pod labels")

	Assert(t).AreEqual(labeled.Labels[rcstore.PodIDLabel], "testPod", "Pod label not set as expected")
	Assert(t).AreEqual(labeled.Labels[RCIDLabel], rc.rcID.String(), "RC label not set as expected")
}

func TestScheduleMoreThan5(t *testing.T) {
	rcStore, _, applicator, rc, _, _, _, closeFn := setup(t)
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

	rcStore.SetDesiredReplicas(rc.rcID, 7)

	numNodes := waitForNodes(t, rc, 7)
	Assert(t).AreEqual(numNodes, 7, "took too long to schedule")

	close(quit)
	wg.Wait()
}

func TestUnscheduleMoreThan5(t *testing.T) {
	rcStore, _, applicator, rc, _, _, _, closeFn := setup(t)
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

	rcStore.SetDesiredReplicas(rc.rcID, 7)

	numNodes := waitForNodes(t, rc, 7)
	Assert(t).AreEqual(numNodes, 7, "took too long to schedule")

	rcStore.SetDesiredReplicas(rc.rcID, 0)

	numNodes = waitForNodes(t, rc, 0)
	Assert(t).AreEqual(numNodes, 0, "took too long to unschedule")

	close(quit)
	wg.Wait()
}

func TestAlertIfNodeBecomesIneligibleIfNotDynamicStrategy(t *testing.T) {
	rcStore, _, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
	if err != nil {
		t.Fatal(err)
	}

	if len(alerter.Alerts) != 1 {
		t.Fatalf("the RC should have alerted since replicas desired is greater than the number of eligible nodes, but there were %d alerts", len(alerter.Alerts))
	}
}

func TestAllocateOnIneligibleIfDynamicStrategy(t *testing.T) {
	rcStore, _, applicator, rc, alerter, auditLogStore, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.AllocationStrategy = fields.DynamicStrategy

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
	if err != nil {
		t.Fatal(err)
	}

	if len(alerter.Alerts) != 0 {
		t.Fatalf("the RC should not have alerted since it allocated dynamic nodes, but there were %d alerts", len(alerter.Alerts))
	}

	status, _, _ := rc.rcStatusStore.Get(rc.rcID)
	if status.NodeTransfer.NewNode != newTransferNode {
		t.Fatalf("the rc failed to update the node transfer status new node to %s from %s", newTransferNode, status.NodeTransfer.NewNode)
	}

	if status.NodeTransfer.ID == "" {
		t.Fatal("no ID was set for the node transfer in RC status")
	}

	nodeTransferAuditLogs := getNodeTransferAuditLogs(t, auditLogStore)
	if len(nodeTransferAuditLogs) != 1 {
		t.Fatalf("expected an audit log record to be created when a node transfer is started but found %d", len(nodeTransferAuditLogs))
	}

	if nodeTransferAuditLogs[0].EventType != audit.NodeTransferStartEvent {
		t.Fatalf("expected audit log event type to be %q but was %q", audit.NodeTransferStartEvent, nodeTransferAuditLogs[0].EventType)
	}

	var details audit.NodeTransferStartDetails
	err = json.Unmarshal([]byte(*nodeTransferAuditLogs[0].EventDetails), &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.NodeTransferID != status.NodeTransfer.ID {
		t.Fatalf("the node transfer ID in the start audit log did not match the one from the RC status: expected %q but got %q", status.NodeTransfer.ID, details.NodeTransferID)
	}
}

func TestNoOpIfNodeTransferInProgress(t *testing.T) {
	rcStore, _, applicator, rc, alerter, auditLogStore, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.AllocationStrategy = fields.DynamicStrategy

	// Simulate node transfer in progress with non-nil node transfer
	testStatus := rcstatus.Status{
		NodeTransfer: &rcstatus.NodeTransfer{
			OldNode: types.NodeName("old.123"),
			NewNode: types.NodeName("new.456"),
		},
	}
	err = rc.rcStatusStore.Set(rc.rcID, testStatus)
	if err != nil {
		t.Fatalf("Unexpected error putting in fake node transfer status")
	}

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
	if err != nil {
		t.Fatal(err)
	}

	if len(alerter.Alerts) != 0 {
		t.Fatalf("the RC should not have alerted since a transfer was in progress, but there were %d alerts", len(alerter.Alerts))
	}

	status, _, _ := rc.rcStatusStore.Get(rc.rcID)
	if *(status.NodeTransfer) != *(testStatus.NodeTransfer) {
		t.Fatalf("the rc should not have updated the status from %v to %v", testStatus, status)
	}

	nodeTransferAuditLogs := getNodeTransferAuditLogs(t, auditLogStore)
	if len(nodeTransferAuditLogs) != 0 {
		t.Fatalf("expected no node transfer audit log to be created if the node transfer was already started but found %d", len(nodeTransferAuditLogs))
	}
}

func TestAlertIfCannotAllocateNodes(t *testing.T) {
	rcStore, _, applicator, rc, alerter, auditLogStore, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}
	rcFields.AllocationStrategy = fields.DynamicStrategy

	// Force an allocate nodes failure
	fixture := consulutil.NewFixture(t)
	closeFn = fixture.Stop
	applicator = labels.NewConsulApplicator(fixture.Client, 0, 0)
	rc.scheduler = testScheduler{applicator, true}

	_, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
	if err == nil {
		t.Fatalf("Expected intentional error but there was none")
	}

	if len(alerter.Alerts) != 1 {
		t.Fatalf("the RC should have alerted since the scheduler could not allocate nodes, but there were %d alerts", len(alerter.Alerts))
	}

	nodeTransferAuditLogs := getNodeTransferAuditLogs(t, auditLogStore)
	if len(nodeTransferAuditLogs) != 0 {
		t.Fatalf("expected no audit log to be created if the allocation call failed but found %d", len(nodeTransferAuditLogs))
	}
}

// testIneligibleNodesCommmon labels nodes and meets desires, then marks one as
// bad and meets desires again. It is the shared to code the establish
// the ineligible node state and cause the RC to act on it
func testIneligibleNodesCommon(applicator testApplicator, rc *replicationController, rcFields fields.RC, alerter *alertingtest.AlertRecorder) (fields.RC, error) {
	for i := 0; i < 7; i++ {
		err := applicator.SetLabel(labels.NODE, fmt.Sprintf("node%d", i), "nodeQuality", "good")
		if err != nil {
			return fields.RC{}, err
		}
	}

	rcFields.ReplicasDesired = 7

	err := rc.meetDesires(rcFields)
	if err != nil {
		return fields.RC{}, err
	}

	current, err := rc.CurrentPods()
	if err != nil {
		return fields.RC{}, err
	}

	if len(current) != 7 {
		return fields.RC{}, util.Errorf("rc should have scheduled 7 pods but found %d", len(current))
	}

	if len(alerter.Alerts) != 0 {
		return fields.RC{}, util.Errorf("there shouldn't have been any alerts yet but there were %d", len(alerter.Alerts))
	}

	// now make one of the nodes ineligible, creating a situation where the
	// RC has 7 "current" nodes and 7 desired recplicas, but only 6 of
	// those nodes meet the node selector's criteria
	err = applicator.SetLabel(labels.NODE, "node3", "nodeQuality", "bad")
	if err != nil {
		return fields.RC{}, err
	}

	err = rc.meetDesires(rcFields)
	if err != nil {
		return fields.RC{}, err
	}

	return rcFields, nil
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
	rcStore, _, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	err := applicator.SetLabel(labels.NODE, "node1", "nodeQuality", "good")
	if err != nil {
		t.Fatal(err)
	}

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}
	rcFields.ReplicasDesired = 1

	err = rc.meetDesires(rcFields)
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

	err = rc.meetDesires(rcFields)
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
	rcStore, _, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.AllocationStrategy = fields.DynamicStrategy

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
	if err != nil {
		t.Fatal(err)
	}

	close(rc.nodeTransfer.quit)

	testRolledBackTransfer(rc, rcFields, t)
}

func TestTransferRolledBackOnRCDisabled(t *testing.T) {
	rcStore, _, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.AllocationStrategy = fields.DynamicStrategy

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.Disabled = true

	err = rc.meetDesires(rcFields)
	if err != nil {
		t.Fatal(err)
	}

	testRolledBackTransfer(rc, rcFields, t)
}

func TestTransferRolledBackOnReplicasDesiredDecrease(t *testing.T) {
	rcStore, _, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.AllocationStrategy = fields.DynamicStrategy

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.ReplicasDesired = rcFields.ReplicasDesired - 1

	err = rc.meetDesires(rcFields)
	if err != nil {
		t.Fatal(err)
	}

	testRolledBackTransfer(rc, rcFields, t)
}

func testRolledBackTransfer(rc *replicationController, rcFields fields.RC, t *testing.T) {
	// Give async goroutine time to rollback transfer
	time.Sleep(1 * time.Second)

	status, _, err := rc.rcStatusStore.Get(rc.rcID)
	if !statusstore.IsNoStatus(err) {
		t.Fatalf("Expected no node transfer status to exist, got %v", status)
	}

	man, _, err := rc.consulStore.Pod(consul.INTENT_TREE, newTransferNode, rcFields.Manifest.ID())
	if err != pods.NoCurrentManifest {
		t.Fatalf("Expected new node to have been erased from intent, but man was %v", man)
	}

	nilTransfer := nodeTransfer{}
	// We have to compare each field because we can't compare nilTransfer and
	// rc.nodeTransfer directly (nodeTransfer has a func() field)
	if rc.nodeTransfer.newNode != nilTransfer.newNode ||
		rc.nodeTransfer.oldNode != nilTransfer.oldNode ||
		rc.nodeTransfer.quit != nilTransfer.quit ||
		rc.nodeTransfer.session != nilTransfer.session {
		t.Fatalf("Expected rc.nodeTransfer to be %v, was %v", nilTransfer, rc.nodeTransfer)
	}

	auditLogs, err := rc.auditLogStore.(testAuditLogStore).List()
	if err != nil {
		t.Fatal(err)
	}

	rollbackAuditLogFound := false
	for _, auditLog := range auditLogs {
		if auditLog.EventType == audit.NodeTransferRollbackEvent {
			var details audit.NodeTransferRollbackDetails
			err = json.Unmarshal([]byte(*auditLog.EventDetails), &details)
			if err != nil {
				t.Fatal(err)
			}

			if details.ReplicationControllerID == rc.rcID {
				rollbackAuditLogFound = true
				break
			}
		}
	}

	if !rollbackAuditLogFound {
		t.Fatal("found no node transfer rollback audit log record for this RC")
	}
}

func TestNewTransferNodeCannotBeScheduledOnReplicasDesiredIncrease(t *testing.T) {
	rcStore, _, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.AllocationStrategy = fields.DynamicStrategy

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
	if err != nil {
		t.Fatal(err)
	}

	eligible, err := rc.eligibleNodes(rcFields)
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

	rcFields.ReplicasDesired = rcFields.ReplicasDesired + 1
	err = rc.meetDesires(rcFields)
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

	err = rc.meetDesires(rcFields)
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
	rcStore, _, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.AllocationStrategy = fields.DynamicStrategy

	healthMap := map[types.NodeName]health.Result{
		newTransferNode: health.Result{Status: health.Passing},
	}
	rc.healthChecker = fake_checker.NewSingleService("", healthMap)

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
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

func TestTransferOnAlreadyAllocatedNodeIfPossible(t *testing.T) {
	rcStore, _, applicator, rc, alerter, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields, err := rcStore.Get(rc.rcID)
	if err != nil {
		t.Fatal(err)
	}

	rcFields.AllocationStrategy = fields.DynamicStrategy

	allocatedNode := types.NodeName("node8")
	err = applicator.SetLabel(labels.NODE, allocatedNode.String(), "nodeQuality", "good")
	if err != nil {
		t.Fatal(err)
	}

	healthMap := map[types.NodeName]health.Result{
		allocatedNode: health.Result{Status: health.Passing},
	}
	rc.healthChecker = fake_checker.NewSingleService("", healthMap)

	rcFields, err = testIneligibleNodesCommon(applicator, rc, rcFields, alerter)
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
	foundAllocatedNode := false
	foundBadNode := false
	for _, node := range nodes {
		if node == newTransferNode {
			foundNewTransferNode = true
		}
		if node == types.NodeName("node3") {
			foundBadNode = true
		}
		if node == allocatedNode {
			foundAllocatedNode = true
		}
	}

	if !foundAllocatedNode {
		t.Fatal("Expected to transfer to already allocated node but did not")
	}

	if foundNewTransferNode {
		t.Fatal("Expected not allocate another node but did")
	}

	if foundBadNode {
		t.Fatal("Expected to have dropped ineligible node but it is still a current node")
	}
}

func TestNodeTransferFailsOnScheduleStep(t *testing.T) {
	_, consulStore, _, rc, _, auditLogStore, rcStatusStore, closeFn := setup(t)
	defer closeFn()

	oldNode := types.NodeName("old_node")
	newNode := types.NodeName("new_node")
	id := rcstatus.NodeTransferID("abcdefg")
	rc.nodeTransferMu.Lock()
	rc.nodeTransfer.oldNode = oldNode
	rc.nodeTransfer.newNode = newNode
	rc.nodeTransfer.id = id
	rc.nodeTransferMu.Unlock()

	err := rcStatusStore.Set(rc.rcID, rcstatus.Status{
		NodeTransfer: &rcstatus.NodeTransfer{
			OldNode: oldNode,
			NewNode: newNode,
			ID:      id,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	rcFields := fields.RC{
		AllocationStrategy: fields.DynamicStrategy,
		Manifest:           testManifest(),
		NodeSelector:       klabels.Everything(),
	}

	// rig transferNodes() to fail at the scheduleWithSession step by writing
	// the key that it wants to write ahead of time
	_, err = consulStore.SetPod(consul.INTENT_TREE, newNode, testManifest())
	if err != nil {
		t.Fatal(err)
	}

	err = rc.transferNodes(rcFields, nil)
	if err == nil {
		t.Fatal("expected an error because we rigged it to fail")
	}

	auditLogs := getNodeTransferAuditLogs(t, auditLogStore)
	if len(auditLogs) != 1 {
		// In normal operation we'd expect a start record as well, but we set the RC status
		// ahead of time to make this test simpler, so we only expect a rollback record
		t.Fatalf("expected one audit log to exist but there were %d", len(auditLogs))
	}

	al := auditLogs[0]
	if al.EventType != audit.NodeTransferRollbackEvent {
		t.Errorf("expected audit log to have event type %q but was %q", audit.NodeTransferRollbackEvent, al.EventType)
	}
	var details audit.NodeTransferRollbackDetails
	err = json.Unmarshal([]byte(*al.EventDetails), &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.OldNode != oldNode {
		t.Errorf("expected node transfer rollback record to have OldNode %q but was %q", oldNode, details.OldNode)
	}
	if details.NewNode != newNode {
		t.Errorf("expected node transfer rollback record to have NewNode %q but was %q", newNode, details.NewNode)
	}
	if details.NodeTransferID != id {
		t.Errorf("expected node transfer rollback record to have NodeTransferID %q but was %q", id, details.NodeTransferID)
	}

	_, _, err = rcStatusStore.Get(rc.rcID)
	switch {
	case statusstore.IsNoStatus(err):
		// this is what we expect
	case err != nil:
		t.Fatalf("unexpected error checking for status not existing: %s", err)
	case err == nil:
		t.Fatal("rc status was not deleted when scheduleWithSession failed")
	}

	rc.nodeTransferMu.Lock()
	defer rc.nodeTransferMu.Unlock()
	if rc.nodeTransfer.oldNode != "" {
		t.Fatal("local node transfer state should have been zeroed when scheduleWithSession() failed")
	}
	if rc.nodeTransfer.newNode != "" {
		t.Fatal("local node transfer state should have been zeroed when scheduleWithSession() failed")
	}
	if rc.nodeTransfer.id != "" {
		t.Fatal("local node transfer state should have been zeroed when scheduleWithSession() failed")
	}
}

func TestAddPods(t *testing.T) {
	_, _, _, rc, _, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields := fields.RC{
		ID:              rc.rcID,
		ReplicasDesired: 5,
		Manifest:        testManifest(),
	}

	// empty
	current := make(types.PodLocations, 0)

	eligible := []types.NodeName{"node1", "node2", "node3", "node4", "node5"}

	err := rc.addPods(rcFields, current, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 5 pods were scheduled
	currentPods, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(currentPods) != 5 {
		t.Fatalf("5 pods should have been scheduled but found %d", len(currentPods))
	}
}

func TestAddPodsDisabled(t *testing.T) {
	_, _, _, rc, _, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields := fields.RC{
		ID:              rc.rcID,
		ReplicasDesired: 5,
		Manifest:        testManifest(),
		Disabled:        true,
	}

	// empty
	current := make(types.PodLocations, 0)

	eligible := []types.NodeName{"node1", "node2", "node3", "node4", "node5"}

	err := rc.addPods(rcFields, current, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 5 pods were scheduled
	currentPods, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(currentPods) != 0 {
		t.Fatalf("0 pods should have been scheduled (because RC is disabled) but found %d", len(currentPods))
	}
}

func TestRemovePods(t *testing.T) {
	_, _, _, rc, _, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields := fields.RC{
		ID:              rc.rcID,
		ReplicasDesired: 5,
		Manifest:        testManifest(),
	}

	current := types.PodLocations{}

	eligible := []types.NodeName{"node1", "node2", "node3", "node4", "node5"}

	// first add the pods so the labels get set up correctly
	err := rc.addPods(rcFields, current, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 5 pods were scheduled
	currentPods, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(currentPods) != 5 {
		t.Fatalf("5 pods should have been scheduled (because RC has nonzero count) but found %d", len(currentPods))
	}

	rcFields.ReplicasDesired = 3

	err = rc.removePods(rcFields, currentPods, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 3 pods were scheduled (since replicas desired fell to 3)
	currentPods, err = rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(currentPods) != 3 {
		t.Fatalf("3 pods should have been scheduled but found %d", len(currentPods))
	}
}

func TestRemovePodsDisabled(t *testing.T) {
	_, _, _, rc, _, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields := fields.RC{
		ID:              rc.rcID,
		ReplicasDesired: 5,
		Manifest:        testManifest(),
		Disabled:        false,
	}

	current := types.PodLocations{}
	eligible := []types.NodeName{"node1", "node2", "node3", "node4", "node5"}

	// first add the pods so the labels get set up correctly
	err := rc.addPods(rcFields, current, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 5 pods were scheduled
	currentPods, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(currentPods) != 5 {
		t.Fatalf("5 pods should have been scheduled (because RC has nonzero count) but found %d", len(currentPods))
	}

	rcFields.Disabled = true
	rcFields.ReplicasDesired = 3

	err = rc.removePods(rcFields, current, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 5 pods were scheduled (since the RC was disabled so it shouldn't have done anything)
	currentPods, err = rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(currentPods) != 5 {
		t.Fatalf("5 pods should have been scheduled (because RC is disabled) but found %d", len(currentPods))
	}
}

func TestRemovePodsDisabledCount0AndIneligible(t *testing.T) {
	// For explanation for why this test exists, see the comments in removePods()
	//
	// TL;DR we expect a disabled RC to unschedule nodes that are in current but
	// not in eligible if and only if its count is 0
	_, _, _, rc, _, _, _, closeFn := setup(t)
	defer closeFn()

	rcFields := fields.RC{
		ID:              rc.rcID,
		ReplicasDesired: 5,
		Manifest:        testManifest(),
		Disabled:        false,
	}

	// 5 pods
	current := types.PodLocations{}

	eligible := []types.NodeName{"node1", "node2", "node3", "node4", "node5"}

	// first add the pods so the labels get set up correctly
	err := rc.addPods(rcFields, current, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 5 pods were scheduled
	currentPods, err := rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	currentNodes := currentPods.Nodes()

	if len(currentPods) != 5 {
		t.Fatalf("5 pods should have been scheduled (because RC has nonzero count) but found %d", len(currentPods))
	}

	// node4 and node5 are "ineligible"
	eligible = []types.NodeName{"node1", "node2", "node3"}
	rcFields.Disabled = true

	err = rc.removePods(rcFields, currentPods, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 5 pods were scheduled (since the RC's count was not zero)
	currentPods, err = rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(currentPods) != 5 {
		t.Fatalf("5 pods should have been scheduled (because RC has nonzero count) but found %d", len(currentPods))
	}

	// Set the count to zero, and now we should see the ineligible nodes become unscheduled
	rcFields.ReplicasDesired = 0

	err = rc.removePods(rcFields, currentPods, eligible)
	if err != nil {
		t.Fatal(err)
	}

	// now confirm that 5 pods were scheduled
	currentPods, err = rc.CurrentPods()
	if err != nil {
		t.Fatal(err)
	}

	if len(currentPods) != 3 {
		t.Fatalf("3 pods should have been scheduled (because RC has zero count) but found %d", len(currentPods))
	}

	// confirm that only node4 and node5 were removed

	removed := types.NewNodeSet(currentNodes...).Difference(types.NewNodeSet(currentPods.Nodes()...))
	node4Found := false
	node5Found := false

	for _, removedNode := range removed.List() {
		switch removedNode {
		case "node4":
			node4Found = true
		case "node5":
			node5Found = true
		default:
			t.Fatalf("node %q was removed in error", removedNode)
		}
	}

	if !node4Found {
		t.Error("expected node4 to be removed because it was ineligible")
	}
	if !node5Found {
		t.Error("expected node5 to be removed because it was ineligible")
	}
}

func getNodeTransferAuditLogs(t *testing.T, auditLogStore testAuditLogStore) []audit.AuditLog {
	var ret []audit.AuditLog
	auditLogs, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	for _, al := range auditLogs {
		switch al.EventType {
		case audit.NodeTransferStartEvent, audit.NodeTransferCompletionEvent, audit.NodeTransferRollbackEvent:
			ret = append(ret, al)
		default:
		}
	}

	return ret
}
