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

func NewKVWithEntries(entries map[string]*api.KVPair) FakeKV {
	if entries == nil {
		entries = make(map[string]*api.KVPair)
	}

	return FakeKV{
		Entries: entries,
	}
}

func (f FakeKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Entries[key], nil, nil
}

func (f FakeKV) Put(pair *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Entries[pair.Key] = pair
	return nil, nil
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.Entries[key]; ok {
		delete(f.Entries, key)
	} else {
		return nil, util.Errorf("Key '%s' not found, could not be deleted", key)
	}

	return nil, nil
}
func (f FakeKV) Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	panic("Not implemented")
}
