package auditlogstore

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

func TestCreate(t *testing.T) {
	now := time.Now()

	txn := new(api.KVTxnOps)
	details := json.RawMessage(`{"some":"details"}`)
	err := ConsulStore{}.Create(txn, "some_event", details)
	if err != nil {
		t.Fatalf("could not create audit log record: %s", err)
	}

	if len(*txn) == 0 {
		t.Fatal("expected a txn operation to be created but none were")
	} else if len(*txn) > 1 {
		t.Fatalf("expected a single txn operation to be created but there were %d", len(*txn))
	}

	op := []*api.KVTxnOp(*txn)[0]
	if op.Verb != string(api.KVSet) {
		t.Errorf("created transaction had wrong verb, should have been %q but was %q", api.KVSet, op.Verb)
	}

	var al AuditLog
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
