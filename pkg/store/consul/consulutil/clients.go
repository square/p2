package consulutil

import (
	"github.com/hashicorp/consul/api"
)

// Wrapper interface that allows retrieval of the underlying interfaces.
type ConsulClient interface {
	KV() ConsulKVClient
	Session() ConsulSessionClient
}

// Interface representing the functionality of the api.KV struct returned by
// calling KV() on an *api.Client. This is useful for swapping in KV
// implementations for tests for example
type ConsulKVClient interface {
	Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
	CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	DeleteCAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
	DeleteTree(prefix string, w *api.WriteOptions) (*api.WriteMeta, error)
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	Keys(prefix, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error)
	List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	Put(pair *api.KVPair, w *api.WriteOptions) (*api.WriteMeta, error)
	Release(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
	Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error)
}

// Specifies the functionality provided by the *api.Session struct for managing
// consul sessions. This is useful for swapping in session client
// implementations in tests
type ConsulSessionClient interface {
	Create(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error)
	CreateNoChecks(*api.SessionEntry, *api.WriteOptions) (string, *api.WriteMeta, error)
	Destroy(string, *api.WriteOptions) (*api.WriteMeta, error)
	Info(id string, q *api.QueryOptions) (*api.SessionEntry, *api.QueryMeta, error)
	List(q *api.QueryOptions) ([]*api.SessionEntry, *api.QueryMeta, error)
	Renew(id string, q *api.WriteOptions) (*api.SessionEntry, *api.WriteMeta, error)
	RenewPeriodic(initialTTL string, id string, q *api.WriteOptions, doneCh chan struct{}) error
}

// Sadly, *api.Client does not implement the ConsulClient interface because the
// return types of KV() and Session() don't match exactly, e.g. KV() returns an
// *api.KV not a ConsulKVCLient, even though *api.KV implements ConsulKVClient.
// This function wraps an *api.Client into something that implements
// ConsulClient.
func ConsulClientFromRaw(client *api.Client) ConsulClient {
	return consulClientWrapper{
		rawClient: client,
	}
}

type consulClientWrapper struct {
	rawClient *api.Client
}

func (c consulClientWrapper) KV() ConsulKVClient {
	return c.rawClient.KV()
}

func (c consulClientWrapper) Session() ConsulSessionClient {
	return c.rawClient.Session()
}
