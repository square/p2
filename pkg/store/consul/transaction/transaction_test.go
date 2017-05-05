package transaction

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/consul/api"
)

func TestAdd(t *testing.T) {
	txn := New()
	for i := 0; i < 64; i++ {
		err := txn.Add(api.KVTxnOp{})
		if err != nil {
			t.Fatalf("couldn't add operation %d to the transaction but should work up to 64: %s", i+1, err)
		}
	}

	err := txn.Add(api.KVTxnOp{})
	if err == nil {
		t.Fatal("expected an error adding the 64th transaction")
	}

	if err != TooManyOperations {
		t.Fatalf("unexpected error adding 64th transaction, wanted %q got %q", TooManyOperations, err)
	}
}

func TestMerge(t *testing.T) {
	txn1 := New()
	for i := 0; i < 30; i++ {
		err := txn1.Add(api.KVTxnOp{})
		if err != nil {
			t.Fatalf("couldn't add operation %d to the transaction but should work up to 64: %s", i+1, err)
		}
	}

	txn2 := New()
	for i := 0; i < 30; i++ {
		err := txn2.Add(api.KVTxnOp{})
		if err != nil {
			t.Fatalf("couldn't add operation %d to the transaction but should work up to 64: %s", i+1, err)
		}
	}

	var err error
	err = txn1.Merge(txn2)
	if err != nil {
		t.Fatalf("unexpected error adding a 30 operation txn to another: %s", err)
	}

	for i := 0; i < 4; i++ {
		err := txn1.Add(api.KVTxnOp{})
		if err != nil {
			t.Fatalf("couldn't add operation %d to the transaction but should work up to 64: %s", i+61, err)
		}
	}

	err = txn1.Add(api.KVTxnOp{})
	if err == nil {
		t.Fatal("expected an error adding the 64th transaction")
	}

	if err != TooManyOperations {
		t.Fatalf("unexpected error adding 64th transaction, wanted %q got %q", TooManyOperations, err)
	}
}

type testTxner struct {
	shouldOK  bool
	shouldErr bool
	errors    api.TxnErrors

	recordedCall *api.KVTxnOps
}

func (t *testTxner) Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error) {
	t.recordedCall = &txn
	if t.shouldErr {
		return false, nil, nil, errors.New("a test error occurred")
	}

	return t.shouldOK, &api.KVTxnResponse{Errors: t.errors}, nil, nil
}

func TestCommitHappy(t *testing.T) {
	txn := New()
	for i := 0; i < 10; i++ {
		txn.Add(api.KVTxnOp{
			Verb:  string(api.KVSet),
			Key:   fmt.Sprintf("key%d", i),
			Value: []byte("whatever"),
		})
	}

	txner := &testTxner{shouldOK: true}

	err := txn.Commit(txner)
	if err != nil {
		t.Fatalf("unexpected error committing transaction: %s", err)
	}

	if txner.recordedCall == nil {
		t.Fatal("Txn() function was not called on the Txner")
	}

	if len(*txner.recordedCall) != 10 {
		t.Fatalf("expected 10 operations in transaction but there were %d", len(*txner.recordedCall))
	}

	for i, op := range *txner.recordedCall {
		if op.Verb != string(api.KVSet) {
			t.Errorf("one of the operations had unexpected verb, wanted %s got %s", string(api.KVSet), op.Verb)
		}

		expectedKey := fmt.Sprintf("key%d", i)
		if op.Key != expectedKey {
			t.Errorf("one of the operations had unexpected key, wanted %s got %s", expectedKey, op.Key)
		}

		expectedBytes := []byte("whatever")
		if !bytes.Equal(expectedBytes, op.Value) {
			t.Errorf("one of the operations had unexpected value: wanted %q got %q", string(expectedBytes), string(op.Value))
		}
	}
}

func TesetAlreadyCommitted(t *testing.T) {
	txn := New()
	err := txn.Add(api.KVTxnOp{})
	if err != nil {
		t.Fatal(err)
	}

	err = txn.Merge(New())
	if err != nil {
		t.Fatal(err)
	}

	txner := &testTxner{shouldOK: true}
	err = txn.Commit(txner)
	if err != nil {
		t.Fatal(err)
	}

	txner.recordedCall = nil
	err = txn.Commit(txner)
	if err == nil {
		t.Error("should have failed to commit a transaction twice")
	}
	if txner.recordedCall != nil {
		t.Error("should not have called Txn() twice on consul client")
	}

	err = txn.Add(api.KVTxnOp{})
	if err == nil {
		t.Error("should have erred adding an operation to a committed transaction")
	}

	err = txn.Merge(New())
	if err == nil {
		t.Error("should have erred merging a transaction that has already been committed")
	}

	err = New().Merge(txn)
	if err == nil {
		t.Error("should have erred merging with a transaction that has already been committed")
	}
}
