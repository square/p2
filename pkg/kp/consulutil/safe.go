package consulutil

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/util/param"
)

// Show detailed error messages from Consul (use only when debugging)
var showConsulErrors = param.Bool("show_consul_errors_unsafe", false)

// KVError encapsulates an error in a Store operation. Errors returned from the
// Consul API cannot be exposed because they may contain the URL of the request,
// which includes an ACL token as a query parameter.
type KVError struct {
	Op          string
	Key         string
	UnsafeError error
	filename    string
	function    string
	lineNumber  int
}

// Error implements the error and "pkg/util".CallsiteError interfaces.
func (err KVError) Error() string {
	cerr := ""
	if *showConsulErrors {
		cerr = fmt.Sprintf(": %s", err.UnsafeError)
	}
	return fmt.Sprintf("%s failed for path %s%s", err.Op, err.Key, cerr)
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
func NewKVError(op string, key string, unsafeError error) KVError {
	var function string
	// Skip one stack frame to get the file & line number of caller.
	pc, file, line, ok := runtime.Caller(1)
	if ok {
		function = runtime.FuncForPC(pc).Name()
	}
	return KVError{
		Op:          op,
		Key:         key,
		UnsafeError: unsafeError,
		filename:    filepath.Base(file),
		function:    function,
		lineNumber:  line,
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

// SafeList performs a KV List operation that can be canceled. When the "done" channel is
// closed, CanceledError will be immediately returned. (The HTTP RPC can't be canceled,
// but it will be ignored.)
func SafeList(
	clientKV ConsulLister,
	done <-chan struct{},
	prefix string,
	options *api.QueryOptions,
) (api.KVPairs, *api.QueryMeta, error) {
	resultChan := make(chan listReply, 1)
	go func() {
		pairs, queryMeta, err := clientKV.List(prefix, options)
		resultChan <- listReply{pairs, queryMeta, err}
	}()
	select {
	case <-done:
		return nil, nil, CanceledError
	case r := <-resultChan:
		return r.pairs, r.queryMeta, r.err
	}
}
