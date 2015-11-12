package kp

import (
	"testing"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/testutil"
)

type ConsulTestFixture struct {
	TestServer *testutil.TestServer
	Store      Store
	Client     *api.Client
	T          *testing.T
}

// Create a new test fixture that spins up a local Consul server.
func NewConsulTestFixture(t *testing.T) *ConsulTestFixture {
	if testing.Short() {
		t.Skip("skipping test dependent on consul because of short mode")
	}

	// testutil.NewTestServerConfig will skip the test if "consul" isn't in the system path.
	// We'd rather the test fail.
	defer func() {
		if t.Skipped() {
			t.Error("failing skipped test")
		}
	}()
	server := testutil.NewTestServer(t)
	client := NewConsulClient(Options{
		Address: server.HTTPAddr,
	})
	store := NewConsulStore(client)
	return &ConsulTestFixture{
		TestServer: server,
		Store:      store,
		Client:     client,
		T:          t,
	}
}

func (f *ConsulTestFixture) Close() {
	f.TestServer.Stop()
	time.Sleep(50 * time.Millisecond)
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
