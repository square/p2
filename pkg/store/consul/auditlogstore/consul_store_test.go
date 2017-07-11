package auditlogstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/store/consul/transaction"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
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

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	details := json.RawMessage(`{"some":"details"}`)
	err := ConsulStore{}.Create(ctx, "some_event", details)
	if err != nil {
		t.Fatalf("could not create audit log record: %s", err)
	}

	fakeTxner := &fakeTxner{}
	err = transaction.MustCommit(ctx, fakeTxner)
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

func TestDelete(t *testing.T) {
	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	id := audit.ID(uuid.New())
	err := ConsulStore{}.Delete(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	fakeTxner := &fakeTxner{}
	err = transaction.MustCommit(ctx, fakeTxner)
	if err != nil {
		t.Fatal(err)
	}

	if len(fakeTxner.ops) == 0 {
		t.Fatal("expected a txn operation to be created but none were")
	} else if len(fakeTxner.ops) > 1 {
		t.Fatalf("expected a single txn operation to be created but there were %d", len(fakeTxner.ops))
	}
	if len(fakeTxner.ops) == 0 {
		t.Fatal("expected a txn operation to be created but none were")
	} else if len(fakeTxner.ops) > 1 {
		t.Fatalf("expected a single txn operation to be created but there were %d", len(fakeTxner.ops))
	}

	op := []*api.KVTxnOp(fakeTxner.ops)[0]
	if op.Verb != api.KVDelete {
		t.Errorf("created transaction had wrong verb, should have been %q but was %q", api.KVDelete, op.Verb)
	}

	if op.Key != fmt.Sprintf("%s/%s", auditLogTree, id) {
		t.Errorf("wrong key being deleted, wanted %q but got %q", id.String(), op.Key)
	}
}
