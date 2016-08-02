package consulutil

import (
	"sync"

	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

// Provides a fake implementation of *api.KV{} which is useful in tests
type FakeKV struct {
	Entries map[string]*api.KVPair
	mu      sync.Mutex
}

func (f FakeKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Entries[key], nil, nil
}

func (f FakeKV) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ret := make(api.KVPairs, 0)
	for _, v := range f.Entries {
		ret = append(ret, v)
	}
	return ret, nil, nil
}

func (f FakeKV) CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if keyPair, ok := f.Entries[p.Key]; ok {
		if keyPair.ModifyIndex != p.ModifyIndex {
			return false, nil, util.Errorf("CAS error for %s", p.Key)
		}
	}

	f.Entries[p.Key] = p
	return true, nil, nil
}
func (f FakeKV) Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error) {
	return nil, util.Errorf("Not implemented")
}
func (f FakeKV) Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	return false, nil, util.Errorf("Not implemented")
}
