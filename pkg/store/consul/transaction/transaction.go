// Package transaction provides an interface for crafting transactional updates
// to consul.
package transaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

// contextKeyType is only used as a key into a context's value
// map[interface{}]interface{}. It's a private type to guarantee no key
// collisions in the map.
type contextKeyType struct{}

// Per https://www.consul.io/api/txn.html
const maxAllowedOperations = 64

var (
	ErrTooManyOperations = errors.New("consul transactions cannot have more than 64 operations")
	ErrAlreadyCommitted  = errors.New("this transaction has already been committed")

	contextKey = contextKeyType{}
)

type tx struct {
	kvOps       *api.KVTxnOps
	committed   bool
	committedMu sync.Mutex
}

// New() returns a new context derived from the one passed as an argument and
// its cancel function with the context containing metadata used to build a
// consul transaction. If the passed context already has consul operations
// defined, the new context will inherit those operations defined but in a
// separate transaction.
func New(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(ctx)
	kvOps := new(api.KVTxnOps)
	txn, err := getTxnFromContext(ctx)
	if err == nil {
		*kvOps = *txn.kvOps
	}
	ctx = context.WithValue(ctx, contextKey, &tx{
		kvOps: kvOps,
	})
	return ctx, cancelFunc
}

func Add(ctx context.Context, op api.KVTxnOp) error {
	txn, err := getTxnFromContext(ctx)
	if err != nil {
		return err
	}

	txn.committedMu.Lock()
	defer txn.committedMu.Unlock()
	if txn.committed {
		return util.Errorf("transaction was already committed")
	}

	if len(*txn.kvOps) == maxAllowedOperations {
		return ErrTooManyOperations
	}
	*txn.kvOps = append(*txn.kvOps, &op)

	return nil
}

type Txner interface {
	Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error)
}

// MustCommit is a convenience wrapper for Commit that returns a single error
// if the transaction fails OR if the transaction is rolled back. If the caller
// wishes to take different action depending on if the transaction failed or
// was rolled back then Commit() should be used instead
func MustCommit(ctx context.Context, txner Txner) error {
	ok, resp, err := Commit(ctx, txner)
	if err != nil {
		return err
	}

	if !ok {
		return util.Errorf("transaction was rolled back: %s", TxnErrorsToString(resp.Errors))
	}

	return nil
}

// Commit attempts to run all of the kv operations in the context's
// transaction. The cancel function with which the context was created must
// also be passed to guarantee that the transaction won't be applied twice
func Commit(ctx context.Context, txner Txner) (bool, *api.KVTxnResponse, error) {
	txn, err := getTxnFromContext(ctx)
	if err != nil {
		return false, nil, err
	}

	txn.committedMu.Lock()
	defer txn.committedMu.Unlock()
	if txn.committed {
		return false, nil, util.Errorf("transaction was already run")
	}

	// make it more convenient for callers to call Commit() even if they're
	// not sure if there are in fact any operations
	if len(*txn.kvOps) == 0 {
		txn.committed = true
		return true, new(api.KVTxnResponse), nil
	}

	select {
	case <-ctx.Done():
		return false, nil, ctx.Err()
	default:
	}

	ok, resp, _, err := txner.Txn(*txn.kvOps, nil)
	if err != nil {
		return false, nil, util.Errorf("transaction failed: %s", err)
	}

	// we mark the transaction as completed. Any further Commit() calls
	// using this context will now error since the transaction is already
	// applied. we do NOT mark the transaction ias completed an error
	// applying the transaction because the caller may wish to retry
	// temporary failures without rebuilding the whole transaction
	txn.committed = true

	return ok, resp, err
}

func TxnErrorsToString(errors api.TxnErrors) string {
	str := ""
	for _, err := range errors {
		str = str + fmt.Sprintf("Op %d: %s\n", err.OpIndex, err.What)
	}

	return str
}

// CommitWithRetries retries Commit() until the transaction is applied without
// an error. It will not retry the transaction if there is no error but the
// transaction was rolled back (and it wouldn't make sense to because it won't
// succeed after that point)
//
// An exponential backoff strategy is used until the context is cancelled with
// a max backoff time of 10 seconds
func CommitWithRetries(ctx context.Context, txner Txner) (bool, *api.KVTxnResponse, error) {
	var ok bool
	var resp *api.KVTxnResponse
	var err error
	f := func() error {
		ok, resp, err = Commit(ctx, txner)
		return err
	}

	// Need this anonymous function because you can't use "break" in a
	// select, you have to return :(
	func() {
		backoff := 100 * time.Millisecond
		for err := f(); err != nil; err = f() {
			select {
			case <-ctx.Done():
				return
			default:
			}

			backoff = backoff * 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
			time.Sleep(backoff)
		}
	}()

	return ok, resp, err
}

func getTxnFromContext(ctx context.Context) (*tx, error) {
	txnValue := ctx.Value(contextKey)
	if txnValue == nil {
		return nil, util.Errorf("no transaction was opened on the passed Context")
	}

	txn, ok := txnValue.(*tx)
	if !ok {
		return nil, util.Errorf("the transaction value on the context had the wrong type!")
	}

	return txn, nil
}
