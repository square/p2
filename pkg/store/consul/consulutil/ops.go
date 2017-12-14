package consulutil

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/hashicorp/consul/api"
)

// KVError encapsulates a consul error
type KVError struct {
	Op         string
	Key        string
	KVError    error
	filename   string
	function   string
	lineNumber int
}

// Error implements the error and "pkg/util".CallsiteError interfaces.
func (err KVError) Error() string {
	return fmt.Sprintf("%s failed for path %s: %s", err.Op, err.Key, err.KVError)
}

// LineNumber implements the "pkg/util".CallsiteError interface.
func (err KVError) LineNumber() int {
	return err.lineNumber
}

// Filename implements the "pkg/util".CallsiteError interface.
func (err KVError) Filename() string {
	return err.filename
}

// Function implements the "pkg/util".CallsiteError interface.
func (err KVError) Function() string {
	return err.function
}

// NewKVError constructs a new KVError to wrap errors from Consul.
func NewKVError(op string, key string, err error) KVError {
	var function string
	// Skip one stack frame to get the file & line number of caller.
	pc, file, line, ok := runtime.Caller(1)
	if ok {
		function = runtime.FuncForPC(pc).Name()
	}
	return KVError{
		Op:         op,
		Key:        key,
		KVError:    err,
		filename:   filepath.Base(file),
		function:   function,
		lineNumber: line,
	}
}

// CanceledError signifies that the Consul operation was explicitly canceled.
var CanceledError = errors.New("Consul operation canceled")

// ConsulLister is a portion of the interface for api.KV
type ConsulLister interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

type listReply struct {
	pairs     api.KVPairs
	queryMeta *api.QueryMeta
	err       error
}

// List performs a KV List operation that can be canceled. When the "done" channel is
// closed, CanceledError will be immediately returned. (The HTTP RPC can't be canceled,
// but it will be ignored.) Errors from Consul will be wrapped in a KVError value.
func List(
	clientKV ConsulLister,
	done <-chan struct{},
	prefix string,
	options *api.QueryOptions,
) (api.KVPairs, *api.QueryMeta, error) {
	resultChan := make(chan listReply, 1)
	go func() {
		pairs, queryMeta, err := clientKV.List(prefix, options)
		if err != nil {
			err = NewKVError("list", prefix, err)
		}
		resultChan <- listReply{pairs, queryMeta, err}
	}()
	select {
	case <-done:
		return nil, nil, CanceledError
	case r := <-resultChan:
		return r.pairs, r.queryMeta, r.err
	}
}

type ConsulKeyser interface {
	Keys(prefix, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error)
}

type keysReply struct {
	keys      []string
	queryMeta *api.QueryMeta
	err       error
}

// SafeKeys performs a KV Keys operation that can be canceled. When the "done"
// channel is closed, CanceledError will be immediately returned. (The HTTP RPC
// can't be canceled, but it will be ignored.) Errors from Consul will be
// wrapped in a KVError value.
func SafeKeys(
	clientKV ConsulKeyser,
	done <-chan struct{},
	prefix string,
	options *api.QueryOptions,
) ([]string, *api.QueryMeta, error) {
	resultChan := make(chan keysReply, 1)
	go func() {
		keys, queryMeta, err := clientKV.Keys(prefix, "", options)
		if err != nil {
			err = NewKVError("keys", prefix, err)
		}
		resultChan <- keysReply{keys, queryMeta, err}
	}()
	select {
	case <-done:
		return nil, nil, CanceledError
	case r := <-resultChan:
		return r.keys, r.queryMeta, r.err
	}
}

type ConsulGetter interface {
	Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
}

type getReply struct {
	kvp       *api.KVPair
	queryMeta *api.QueryMeta
	err       error
}

// Like List, but for a single key instead of a list.
func Get(
	ctx context.Context,
	clientKV ConsulGetter,
	key string,
	options *api.QueryOptions,
) (*api.KVPair, *api.QueryMeta, error) {
	resultChan := make(chan getReply, 1)

	go func() {
		kvp, queryMeta, err := clientKV.Get(key, options)
		if err != nil {
			err = NewKVError("get", key, err)
		}
		select {
		case resultChan <- getReply{kvp, queryMeta, err}:
		case <-ctx.Done():
		}
	}()

	select {
	case <-ctx.Done():
		return nil, nil, CanceledError
	case r := <-resultChan:
		return r.kvp, r.queryMeta, r.err
	}
}
