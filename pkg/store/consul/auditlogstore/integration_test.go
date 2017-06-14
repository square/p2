package auditlogstore

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
)

// The tests in this file utilize a real consul server within the test process
// in order to perform transactions.

func TestCreateListAndDelete(t *testing.T) {
	f := consulutil.NewFixture(t)
	defer f.Stop()

	consulStore := NewConsulStore(f.Client.KV())

	// First create two records to confirm Create() works as well as our
	// transaction pattern
	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	var eventType1, eventType2 audit.EventType = "event_type_1", "event_type_2"
	details1, details2 := json.RawMessage(`{"some":"details"}`), json.RawMessage(`{"some":"details2"}`)
	err := consulStore.Create(ctx, eventType1, details1)
	if err != nil {
		t.Fatalf("could not create first audit record: %s", err)
	}

	err = consulStore.Create(ctx, eventType2, details2)
	if err != nil {
		t.Fatalf("could not create second audit record: %s", err)
	}

	err = transaction.Commit(ctx, f.Client.KV())
	if err != nil {
		t.Fatalf("could not apply txn with two audit record creations: %s", err)
	}

	// Now list them back to confirm List() works
	logs, err := consulStore.List()
	if err != nil {
		t.Fatalf("unexpected error listing audit records: %s", err)
	}

	if len(logs) != 2 {
		t.Fatalf("list() operation should have found 2 records but found %d", len(logs))
	}

	var firstID audit.ID
	for id, al := range logs {
		if al.EventType == eventType1 && bytes.Equal([]byte(*al.EventDetails), []byte(details1)) {
			firstID = id
			break
		}
	}
	if firstID == "" {
		t.Fatal("didn't find the first audit record created")
	}

	var secondID audit.ID
	for id, al := range logs {
		if al.EventType == eventType2 && bytes.Equal([]byte(*al.EventDetails), []byte(details2)) {
			secondID = id
			break
		}
	}
	if secondID == "" {
		t.Fatal("didn't find the second audit record created")
	}

	// Now delete a record to confirm that works
	ctx, cancelFunc = transaction.New(context.Background())
	defer cancelFunc()
	err = consulStore.Delete(ctx, firstID)
	if err != nil {
		t.Fatalf("error deleting audit log record: %s", err)
	}

	err = transaction.Commit(ctx, f.Client.KV())
	if err != nil {
		t.Fatalf("could not apply txn with a record deletion: %s", err)
	}

	// Now List() again to confirm that only the second record remains
	// Now list them back to confirm List() works
	logs, err = consulStore.List()
	if err != nil {
		t.Fatalf("unexpected error listing audit records: %s", err)
	}

	if len(logs) != 1 {
		t.Fatalf("list() operation should have found 1 record but found %d", len(logs))
	}

	if _, ok := logs[secondID]; !ok {
		t.Fatal("expected to find the second record in the consul store still but it wasn't present")
	}
}
