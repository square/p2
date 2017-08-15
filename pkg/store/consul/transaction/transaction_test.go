package transaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

func TestAdd(t *testing.T) {
	ctx, _ := New(context.Background())
	for i := 0; i < 64; i++ {
		err := Add(ctx, api.KVTxnOp{})
		if err != nil {
			t.Fatalf("couldn't add operation %d to the transaction but should work up to 64: %s", i+1, err)
		}
	}

	err := Add(ctx, api.KVTxnOp{})
	if err == nil {
		t.Fatal("expected an error adding the 64th transaction")
	}

	if err != ErrTooManyOperations {
		t.Fatalf("unexpected error adding 64th transaction, wanted %q got %q", ErrTooManyOperations, err)
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

func TestMustCommitHappy(t *testing.T) {
	ctx, cancelFunc := New(context.Background())
	defer cancelFunc()
	for i := 0; i < 10; i++ {
		err := Add(ctx, api.KVTxnOp{
			Verb:  string(api.KVSet),
			Key:   fmt.Sprintf("key%d", i),
			Value: []byte("whatever"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	txner := &testTxner{shouldOK: true}

	err := MustCommit(ctx, txner)
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

func TesetErrAlreadyMustCommitted(t *testing.T) {
	ctx, cancelFunc := New(context.Background())
	defer cancelFunc()
	err := Add(ctx, api.KVTxnOp{})
	if err != nil {
		t.Fatal(err)
	}

	txner := &testTxner{shouldOK: true}
	err = MustCommit(ctx, txner)
	if err != nil {
		t.Fatal(err)
	}

	txner.recordedCall = nil
	err = MustCommit(ctx, txner)
	if err == nil {
		t.Error("should have failed to commit a transaction twice")
	}
	if txner.recordedCall != nil {
		t.Error("should not have called Txn() twice on consul client")
	}

	err = Add(ctx, api.KVTxnOp{})
	if err == nil {
		t.Error("should have erred adding an operation to a committed transaction")
	}
}

func TestMustCommitErrNoTransaction(t *testing.T) {
	err := MustCommit(context.Background(), &testTxner{})
	if err == nil {
		t.Fatal("should have gotten an error committing using a context that does not have a transaction")
	}
}

func TestMustCommitTransactionWithNoOperations(t *testing.T) {
	ctx, cancelFunc := New(context.Background())
	defer cancelFunc()
	txner := &testTxner{shouldOK: true}
	err := MustCommit(ctx, txner)
	if err != nil {
		t.Fatalf("unexpected error committing transaction: %s", err)
	}

	if txner.recordedCall != nil {
		t.Error("no txn call should have been made to consul if there are no operations on the transaction")
	}
}

func TestErrAddingToCommittedTransaction(t *testing.T) {
	ctx, cancelFunc := New(context.Background())
	defer cancelFunc()
	txner := &testTxner{shouldOK: true}

	err := Add(ctx, api.KVTxnOp{})
	if err != nil {
		t.Fatal(err)
	}

	err = MustCommit(ctx, txner)
	if err != nil {
		t.Fatalf("unexpected error committing transaction: %s", err)
	}

	err = Add(ctx, api.KVTxnOp{})
	if err == nil {
		t.Fatal("expected an error adding an operation to a transaction that was already committed")
	}
}

// signalingTxner has a channel that it passes a value on whenever Txn() is called. This is useful for testing the behavior of CommitWithRetries()
type signalingTxner struct {
	shouldErr      bool
	shouldRollback bool
	calls          chan<- struct{}
}

func (s signalingTxner) Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error) {
	s.calls <- struct{}{}
	if s.shouldErr {
		return false, nil, nil, errors.New("a test error occurred")
	}

	if s.shouldRollback {
		return false, new(api.KVTxnResponse), nil, nil
	}

	return true, new(api.KVTxnResponse), nil, nil
}

func TestCommitWithRetriesHappy(t *testing.T) {
	ctx, cancel := New(context.Background())
	defer cancel()

	callsCh := make(chan struct{})
	txner := signalingTxner{
		calls: callsCh,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(callsCh)
		CommitWithRetries(ctx, txner)
	}()

	select {
	case <-callsCh:
	case <-time.After(5 * time.Second):
		t.Fatal("took too long for Txn() to be called")
	}

	select {
	case _, ok := <-callsCh:
		if ok {
			t.Fatal("CommitWithRetries() called Txn() again after a successful commit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CommitWithRetries() didn't exit quickly enough after a successful commit")
	}
}

func TestCommitWithRetriesDoesntRetryRollback(t *testing.T) {
	ctx, cancel := New(context.Background())
	defer cancel()

	callsCh := make(chan struct{})
	txner := signalingTxner{
		calls:          callsCh,
		shouldRollback: true,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(callsCh)
		CommitWithRetries(ctx, txner)
	}()

	select {
	case <-callsCh:
	case <-time.After(5 * time.Second):
		t.Fatal("took too long for Txn() to be called")
	}

	select {
	case _, ok := <-callsCh:
		if ok {
			t.Fatal("CommitWithRetries() called Txn() again after a rolled back commit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CommitWithRetries() didn't exit quickly enough after a successful commit")
	}
}

func TestCommitWithRetriesRetriesErrorsUntilCanceled(t *testing.T) {
	ctx, cancel := New(context.Background())
	defer cancel()

	callsCh := make(chan struct{})
	txner := signalingTxner{
		calls:          callsCh,
		shouldRollback: true,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(callsCh)
		CommitWithRetries(ctx, txner)
	}()

	// make sure this gets called at least twice
	select {
	case <-callsCh:
	case <-time.After(5 * time.Second):
		t.Fatal("took too long for Txn() to be called")
	}

	select {
	case <-callsCh:
	case <-time.After(5 * time.Second):
		t.Fatal("took too long for Txn() to be called")
	}

	// now cancel the context, which means we should get at most one more Txn() call
	cancel()
	select {
	case _, ok := <-callsCh:
		if ok {
			select {
			case _, ok := <-callsCh:
				if ok {
					t.Fatal("CommitWithRetries() called Txn() twice after being canceled")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("CommitWithRetries() didn't exit quickly enough after being canceled")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CommitWithRetries() didn't exit quickly enough after being canceled")
	}
}
