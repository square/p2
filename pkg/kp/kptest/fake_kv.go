package kptest

import (
	"strings"
	"sync"

	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
)

// Models the consul API client
type FakeKV struct {
	kvPairs map[string]*api.KVPair
	mu      sync.Mutex
}

func NewFakeKV() *FakeKV {
	return &FakeKV{
		kvPairs: make(map[string]*api.KVPair),
	}
}

func (f *FakeKV) Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	return f.kvPairs[key], nil, nil
}

func (f *FakeKV) List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	var res api.KVPairs
	for key, val := range f.kvPairs {
		if strings.HasPrefix(key, prefix) {
			res = append(res, val)
		}
	}

	return res, nil, nil
}

// The fake implementation of this is just the same as writing a key, we expect
// callers to be using the /lock subtree so real keys won't ever appear like
// locks
func (f *FakeKV) Acquire(pair *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.kvPairs[pair.Key]; ok {
		return false, nil, util.Errorf("Key %s is already locked", pair.Key)
	}

	f.kvPairs[pair.Key] = pair
	return true, nil, nil
}

func (f *FakeKV) CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	existingPair, ok := f.kvPairs[pair.Key]
	if !ok {
		// This hasn't been written before, so write it
		f.kvPairs[pair.Key] = pair
		return true, nil, nil
	}

	if pair.ModifyIndex == existingPair.ModifyIndex {
		f.kvPairs[pair.Key] = pair
		return true, nil, nil
	}

	return false, nil, util.Errorf("CAS failed: %d != %d", pair.ModifyIndex, existingPair.ModifyIndex)
}

func (f *FakeKV) DeleteCAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.kvPairs[pair.Key]
	if !ok {
		return false, nil, util.Errorf("Key '%s' does not exist", pair.Key)
	}

	delete(f.kvPairs, pair.Key)
	return true, nil, nil
}

func (f *FakeKV) Delete(key string, opts *api.WriteOptions) (*api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.kvPairs, key)
	return nil, nil
}
