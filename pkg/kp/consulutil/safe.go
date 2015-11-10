package consulutil

import (
	"errors"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
)

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
