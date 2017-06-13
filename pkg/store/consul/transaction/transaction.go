// Package transaction provides an interface for crafting transactional updates
// to consul.
package transaction

import (
	"context"
	"errors"
	"fmt"

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
	kvOps *api.KVTxnOps
}

func New(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(ctx)
	ctx = context.WithValue(ctx, contextKey, &tx{
		kvOps: new(api.KVTxnOps),
	})
	return ctx, cancelFunc
}

func Add(ctx context.Context, op api.KVTxnOp) error {
	txn, err := getTxnFromContext(ctx)
	if err != nil {
		return err
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

// Commit attempts to run all of the kv operations in the context's
// transaction. The cancel function with which the context was created must
// also be passed to guarantee that the transaction won't be applied twice
func Commit(ctx context.Context, cancel context.CancelFunc, txner Txner) error {
	txn, err := getTxnFromContext(ctx)
	if err != nil {
		return err
	}

	// make it more convenient for callers to call Commit() even if they're
	// not sure if there are in fact any operations
	if len(*txn.kvOps) == 0 {
		return nil
	}

	ok, resp, _, err := txner.Txn(*txn.kvOps, nil)
	if err != nil {
		return util.Errorf("transaction failed: %s", err)
	}

	// we call cancel() here to mark the transaction as completed. Any
	// further function calls using this context will now error via
	// getTxnFromContext() since the transaction is already applied.  we do
	// NOT call cancel() if there was an error applying the transaction
	// because the caller may wish to retry temporary failures without
	// rebuilding the whole transaction
	cancel()

	if len(resp.Errors) != 0 {
		return util.Errorf("some errors occurred when committing the transaction: %s", txnErrorsToString(resp.Errors))
	}

	// I think ok being false means there should be something in resp.Errors, so this
	// should be impossible.
	if !ok {
		return util.Errorf("an unknown error occurred when applying the transaction")
	}

	return nil
}

func txnErrorsToString(errors api.TxnErrors) string {
	str := ""
	for _, err := range errors {
		str = str + fmt.Sprintf("Op %d: %s\n", err.OpIndex, err.What)
	}

	return str
}

func getTxnFromContext(ctx context.Context) (*tx, error) {
	select {
	case <-ctx.Done():
		return nil, ErrAlreadyCommitted
	default:
	}

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
