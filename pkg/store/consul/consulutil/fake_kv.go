package consulutil

import (
	"fmt"
	"strings"
	"sync"

	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

// Provides a fake implementation of *api.KV{} which is useful in tests
type FakeKV struct {
	Entries map[string]*api.KVPair
	mu      sync.Mutex
}

type FakeConsulClient struct {
	KV_ ConsulKVClient
}

var _ ConsulClient = &FakeConsulClient{}

func (c FakeConsulClient) KV() ConsulKVClient           { return c.KV_ }
func (c FakeConsulClient) Session() ConsulSessionClient { panic("not implemented") }

func NewFakeClient() *FakeConsulClient {
	return &FakeConsulClient{
		KV_: NewKVWithEntries(nil),
	}
}

func NewKVWithEntries(entries map[string]*api.KVPair) *FakeKV {
	if entries == nil {
		entries = make(map[string]*api.KVPair)
	}

	return &FakeKV{
		Entries: entries,
	}
}

func (f *FakeKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Entries[key], &api.QueryMeta{}, nil
}

func (f *FakeKV) Keys(prefix, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	return nil, nil, fmt.Errorf("not yet implemented in FakeKV")
}

func (f *FakeKV) Put(pair *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Entries[pair.Key] = pair
	return &api.WriteMeta{}, nil
}

func (f *FakeKV) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ret := make(api.KVPairs, 0)
	for _, v := range f.Entries {
		if strings.HasPrefix(v.Key, prefix) {
			ret = append(ret, v)
		}
	}
	return ret, &api.QueryMeta{}, nil
}

func (f *FakeKV) CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if keyPair, ok := f.Entries[p.Key]; ok {
		if keyPair.ModifyIndex != p.ModifyIndex {
			return false, nil, util.Errorf("CAS error for %s", p.Key)
		}
	}

	f.Entries[p.Key] = p
	return true, &api.WriteMeta{}, nil
}

func (f *FakeKV) Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.Entries, key)
	return &api.WriteMeta{}, nil
}

func (f *FakeKV) DeleteTree(prefix string, w *api.WriteOptions) (*api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key := range f.Entries {
		if strings.HasPrefix(key, prefix) {
			delete(f.Entries, key)
		}
	}
	return &api.WriteMeta{}, nil
}

// The fake implementation of this is just the same as writing a key, we expect
// callers to be using the /lock subtree so real keys won't ever appear like
// locks
func (f *FakeKV) Acquire(pair *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.Entries[pair.Key]; ok {
		return false, nil, util.Errorf("Key %s is already locked", pair.Key)
	}

	f.Entries[pair.Key] = pair
	return true, nil, nil
}

func (f *FakeKV) DeleteCAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.Entries[pair.Key]
	if !ok {
		return false, nil, util.Errorf("Key '%s' does not exist", pair.Key)
	}

	delete(f.Entries, pair.Key)
	return true, &api.WriteMeta{}, nil
}

func (f *FakeKV) Release(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	return false, nil, fmt.Errorf("not yet implemented in FakeKV")
}
