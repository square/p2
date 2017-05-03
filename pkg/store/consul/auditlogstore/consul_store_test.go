package auditlogstore

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/store/consul/transaction"

	"github.com/hashicorp/consul/api"
)

// fakeTxner just records the operations it gets
type fakeTxner struct {
	ops api.KVTxnOps
}

func (f *fakeTxner) Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error) {
	f.ops = txn
	return true, new(api.KVTxnResponse), new(api.QueryMeta), nil
}

func TestCreate(t *testing.T) {
	now := time.Now()

	txn := transaction.New()
	details := json.RawMessage(`{"some":"details"}`)
	err := ConsulStore{}.Create(txn, "some_event", details)
	if err != nil {
		t.Fatalf("could not create audit log record: %s", err)
	}

	fakeTxner := &fakeTxner{}
	err = txn.Commit(fakeTxner)
	if err != nil {
		t.Fatal(err)
	}

	if len(fakeTxner.ops) == 0 {
		t.Fatal("expected a txn operation to be created but none were")
	} else if len(fakeTxner.ops) > 1 {
		t.Fatalf("expected a single txn operation to be created but there were %d", len(fakeTxner.ops))
	}

	op := fakeTxner.ops[0]
	if op.Verb != string(api.KVSet) {
		t.Errorf("created transaction had wrong verb, should have been %q but was %q", api.KVSet, op.Verb)
	}

	var al audit.AuditLog
	err = json.Unmarshal(op.Value, &al)
	if err != nil {
		t.Fatalf("could not unmarshal audit log from JSON: %s", err)
	}

	if al.EventType != "some_event" {
		t.Errorf("unexpected event type %q", al.EventType)
	}

	if !bytes.Equal([]byte(details), []byte(*al.EventDetails)) {
		t.Errorf("event details did not match expected. got %q but expected %q", string(*al.EventDetails), string(details))
	}

	if al.Timestamp.Before(now) {
		t.Errorf("expected the current timestamp to be added to record but it was %s", al.Timestamp)
	}
}
