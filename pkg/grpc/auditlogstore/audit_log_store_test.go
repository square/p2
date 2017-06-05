package auditlogstore

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/square/p2/pkg/audit"
	audit_log_protos "github.com/square/p2/pkg/grpc/auditlogstore/protos"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
)

func TestList(t *testing.T) {
	f := consulutil.NewFixture(t)
	defer f.Stop()

	alStore := auditlogstore.NewConsulStore(f.Client.KV())
	store := New(
		alStore,
		logging.TestLogger(),
		f.Client.KV(),
	)

	resp, err := store.List(context.Background(), new(audit_log_protos.ListRequest))
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.GetAuditLogs()) != 0 {
		t.Errorf("expected 0 records to be returned but there were %d", len(resp.GetAuditLogs()))
	}

	ctx, cancelFunc := transaction.New(context.Background())
	eventDetails0 := json.RawMessage(`{"bogus_event_details":0}`)
	eventType := audit.EventType("bogus_event_type")
	err = alStore.Create(
		ctx,
		eventType,
		eventDetails0,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.Commit(ctx, cancelFunc, f.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	resp, err = store.List(context.Background(), new(audit_log_protos.ListRequest))
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.GetAuditLogs()) != 1 {
		t.Errorf("expected 1 record to be returned but there were %d", len(resp.GetAuditLogs()))
	}

	for _, val := range resp.GetAuditLogs() {
		if val.EventType != eventType.String() {
			t.Errorf("event type for record didn't match expected. wanted %q got %q", eventType, val.EventType)
		}

		if !bytes.Equal([]byte(val.EventDetails), []byte(eventDetails0)) {
			t.Errorf("event details for record didn't match expected. wanted %q got %q", string(eventDetails0), val.EventDetails)
		}
	}

	ctx, cancelFunc = transaction.New(context.Background())
	err = alStore.Create(
		ctx,
		"bogus_event_type",
		json.RawMessage(`{"bogus_event_details1":1}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = alStore.Create(
		ctx,
		"bogus_event_type",
		json.RawMessage(`{"bogus_event_details":2}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.Commit(ctx, cancelFunc, f.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	resp, err = store.List(context.Background(), new(audit_log_protos.ListRequest))
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.GetAuditLogs()) != 3 {
		t.Errorf("expected 3 records to be returned but there were %d", len(resp.GetAuditLogs()))
	}
}

func TestDelete(t *testing.T) {
	f := consulutil.NewFixture(t)
	defer f.Stop()

	alStore := auditlogstore.NewConsulStore(f.Client.KV())
	store := New(
		alStore,
		logging.TestLogger(),
		f.Client.KV(),
	)

	ctx, cancelFunc := transaction.New(context.Background())
	eventDetails0 := json.RawMessage(`{"bogus_event_details":0}`)
	eventType := audit.EventType("bogus_event_type")
	err := alStore.Create(
		ctx,
		eventType,
		eventDetails0,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.Commit(ctx, cancelFunc, f.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	resp, err := store.List(context.Background(), new(audit_log_protos.ListRequest))
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.GetAuditLogs()) != 1 {
		t.Errorf("expected 1 record to be returned but there were %d", len(resp.GetAuditLogs()))
	}

	var idToDelete string
	for id, _ := range resp.GetAuditLogs() {
		idToDelete = id
	}

	req := &audit_log_protos.DeleteRequest{
		AuditLogIds: []string{idToDelete},
	}

	deleteResp, err := store.Delete(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if deleteResp == nil {
		t.Fatal("a nil response was returned along with nil error")
	}

	resp, err = store.List(context.Background(), new(audit_log_protos.ListRequest))
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.GetAuditLogs()) != 0 {
		t.Errorf("expected 0 records to be returned after deletion but there were %d", len(resp.GetAuditLogs()))
	}
}
