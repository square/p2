// Package transaction provides an interface for crafting transactional updates
// to consul.
package transaction

import (
	"errors"
	"fmt"

	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

// Per https://www.consul.io/api/txn.html
const maxAllowedOperations = 64

var (
	ErrTooManyOperations = errors.New("consul transactions cannot have more than 64 operations")
	ErrAlreadyCommitted  = errors.New("this transaction has already been committed")
)

type Tx struct {
	kvOps            *api.KVTxnOps
	alreadyCommitted bool

	commitHooks []func()
}

func New() *Tx {
	return &Tx{
		kvOps: new(api.KVTxnOps),
	}
}

func (c *Tx) Add(op api.KVTxnOp) error {
	if c.alreadyCommitted {
		return ErrAlreadyCommitted
	}

	if len(*c.kvOps) == maxAllowedOperations {
		return ErrTooManyOperations
	}
	*c.kvOps = append(*c.kvOps, &op)

	return nil
}

// AddCommitHook adds a function that should be run when the transaction is
// committed.  This is useful for cleaning up resources, e.g. consul sessions
// that had to be opened as part of transaction setup.
func (c *Tx) AddCommitHook(f func()) error {
	if c.alreadyCommitted {
		return ErrAlreadyCommitted
	}

	c.commitHooks = append(c.commitHooks, f)
	return nil
}

func (c *Tx) Append(newTxn *Tx) error {
	if c.alreadyCommitted {
		return ErrAlreadyCommitted
	}

	if newTxn.alreadyCommitted {
		return ErrAlreadyCommitted
	}

	if len(*c.kvOps)+len(*newTxn.kvOps) > 64 {
		return ErrTooManyOperations
	}

	for _, op := range *newTxn.kvOps {
		*c.kvOps = append(*c.kvOps, op)
	}

	return nil
}

type Txner interface {
	Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error)
}

func (c *Tx) Commit(txner Txner) error {
	if c.alreadyCommitted {
		return ErrAlreadyCommitted
	}

	ok, resp, _, err := txner.Txn(*c.kvOps, nil)
	if err != nil {
		return util.Errorf("transaction failed: %s", err)
	}

	c.runCommitHooks()
	// Set this after we know err == nil because otherwise it could have
	// been a retry-able TCP error for example
	c.alreadyCommitted = true

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

func (c *Tx) runCommitHooks() {
	for _, f := range c.commitHooks {
		f()
	}
}

func txnErrorsToString(errors api.TxnErrors) string {
	str := ""
	for _, err := range errors {
		str = str + fmt.Sprintf("Op %d: %s\n", err.OpIndex, err.What)
	}

	return str
}
