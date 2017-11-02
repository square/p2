package rc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/square/p2/pkg/audit"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestAuditingTransaction(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())
	rcFields := rc_fields.RC{
		Manifest: testManifest(),
		PodLabels: klabels.Set{
			pc_fields.AvailabilityZoneLabel: "some_az",
			pc_fields.ClusterNameLabel:      "some_cn",
		},
	}

	rc := &replicationController{
		auditLogStore: auditLogStore,
	}

	ctx, cancel := rc.newAuditingTransaction(context.Background(), rcFields, []types.NodeName{"node1", "node2"})
	defer cancel()

	ctx.AddNode("node3")
	ctx.RemoveNode("node2")

	ok, resp, err := ctx.Commit(fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit audit log record: %s", err)
	}
	if !ok {
		t.Fatalf("could not commit audit log record: %s", transaction.TxnErrorsToString(resp.Errors))
	}

	testSuccessfulCommit(auditLogStore, t)
}

func testSuccessfulCommit(auditLogStore auditlogstore.ConsulStore, t *testing.T) {
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

		if details.PodID != testManifest().ID() {
			t.Errorf("expected audit log details to have pod ID %q but was %q", testManifest().ID(), details.PodID)
		}
		if details.AvailabilityZone != "some_az" {
			t.Errorf("expected audit log details to have availability zone %q but was %q", "some_az", details.AvailabilityZone)
		}
		if details.ClusterName != "some_cn" {
			t.Errorf("expected audit log details to have cluster name %q but was %q", "some_cn", details.ClusterName)
		}

		if len(details.Nodes) != 2 {
			t.Fatalf("expected 2 nodes but there were %d", len(details.Nodes))
		}
		found1 := false
		found3 := false
		for _, node := range details.Nodes {
			switch node {
			case "node1":
				found1 = true
			case "node3":
				found3 = true
			default:
				t.Errorf("found node %s but that wasn't expected", node)
			}
		}

		if !found1 {
			t.Error("expected to find node1 in the list")
		}

		if !found3 {
			t.Error("expected to find node3 in the list")
		}
	}
}

func TestCommitWithRetriesHappy(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	txner, store := testCommitWithRetries(fixture, t, false, false)
	defer close(txner.calls)
	defer txner.cancel()

	select {
	case <-txner.calls:
	case <-time.After(5 * time.Second):
		t.Fatal("took too long for Txn() to be called")
	}

	select {
	case <-txner.calls:
	case <-time.After(5 * time.Second):
	}

	testSuccessfulCommit(store, t)
}

func TestCommitWithRetriesRetriesErrorUntilCancelled(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	txner, auditLogStore := testCommitWithRetries(fixture, t, true, false)
	defer close(txner.calls)

	// make sure this gets called at least twice
	select {
	case <-txner.calls:
	case <-time.After(5 * time.Second):
		t.Fatal("took too long for Txn() to be called")
	}

	select {
	case <-txner.calls:
	case <-time.After(5 * time.Second):
		t.Fatal("took too long for Txn() to be called")
	}

	txner.cancel()

	records, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("Expected 0 audit log records, was: %d", len(records))
	}
}

func TestCommitWithRetriesDoesntRetryRollback(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	txner, auditLogStore := testCommitWithRetries(fixture, t, false, true)
	defer close(txner.calls)
	defer txner.cancel()

	select {
	case <-txner.calls:
	case <-time.After(5 * time.Second):
		t.Fatal("took too long for Txn() to be called")
	}

	select {
	case <-txner.calls:
		t.Fatal("CommitWithRetries() retried on rollback")
	case <-time.After(5 * time.Second):
		return
	}

	records, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("Expected 0 audit log records, was: %d", len(records))
	}
}

func testCommitWithRetries(fixture consulutil.Fixture, t *testing.T, shouldErr bool, shouldRollback bool) (signalingTxner, auditlogstore.ConsulStore) {

	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())
	rcFields := rc_fields.RC{
		Manifest: testManifest(),
		PodLabels: klabels.Set{
			pc_fields.AvailabilityZoneLabel: "some_az",
			pc_fields.ClusterNameLabel:      "some_cn",
		},
	}
	rc := &replicationController{
		auditLogStore: auditLogStore,
	}

	ctx, cancel := rc.newAuditingTransaction(context.Background(), rcFields, []types.NodeName{"node1", "node2"})

	ctx.AddNode("node3")
	ctx.RemoveNode("node2")

	callsCh := make(chan struct{})
	txner := signalingTxner{
		txner:          fixture.Client.KV(),
		shouldErr:      shouldErr,
		shouldRollback: shouldRollback,
		calls:          callsCh,
		cancel:         cancel,
	}

	go ctx.CommitWithRetries(txner)
	return txner, auditLogStore
}

type signalingTxner struct {
	txner          transaction.Txner
	shouldErr      bool
	shouldRollback bool
	calls          chan struct{}
	cancel         func()
}

func (s signalingTxner) Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error) {
	s.calls <- struct{}{}
	if s.shouldErr {
		return false, nil, nil, util.Errorf("a test error occurred")
	}

	if s.shouldRollback {
		return false, new(api.KVTxnResponse), nil, nil
	}

	return s.txner.Txn(txn, q)
}
