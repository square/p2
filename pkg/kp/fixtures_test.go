package kp

import (
	"testing"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/consultest"
)

type ConsulTestFixture struct {
	consultest.Fixture
	Store Store
}

// Create a new test fixture that spins up a local Consul server.
func NewConsulTestFixture(t *testing.T) *ConsulTestFixture {
	f := new(ConsulTestFixture)
	f.Fixture = consultest.NewFixture(t)
	defer f.Fixture.StopOnPanic()
	f.Store = NewConsulStore(f.Client)
	return f
}

func (f *ConsulTestFixture) Close() {
	f.Fixture.Stop()
}

func (f *ConsulTestFixture) CreateSession() string {
	se := &api.SessionEntry{
		LockDelay: 1 * time.Nanosecond,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "600s", // long enough for any unit test
	}
	id, _, err := f.Client.Session().CreateNoChecks(se, nil)
	if err != nil {
		f.T.Fatal("error creating new session:", err)
	}
	return id
}

func (f *ConsulTestFixture) DestroySession(id string) {
	_, err := f.Client.Session().Destroy(id, nil)
	if err != nil {
		f.T.Fatal("error destroying session:", err)
	}
}

type KeyWaiter struct {
	key   string
	index uint64
	f     *ConsulTestFixture
}

// NewKeyWaiter creates a KeyWaiter that will wait for changes to the given key.
func (f *ConsulTestFixture) NewKeyWaiter(key string) *KeyWaiter {
	_, meta, err := f.Client.KV().Get(key, nil)
	if err != nil {
		f.T.Fatal("error initializing key index:", err)
	}
	return &KeyWaiter{
		key:   key,
		index: meta.LastIndex,
		f:     f,
	}
}

// WaitForChange waits until its key has been updated in Consul.
func (kw *KeyWaiter) WaitForChange() {
	_, meta, err := kw.f.Client.KV().Get(kw.key, &api.QueryOptions{WaitIndex: kw.index})
	if err != nil {
		kw.f.T.Log("error waiting for key:", err)
	} else {
		kw.index = meta.LastIndex
	}
}
