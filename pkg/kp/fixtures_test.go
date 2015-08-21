package kp

import (
	"testing"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/testutil"
)

type ConsulTestFixture struct {
	TestServer *testutil.TestServer
	Store      *consulStore
	Client     *api.Client
	T          *testing.T
}

// Create a new test fixture that spins up a local Consul server.
func NewConsulTestFixture(t *testing.T) *ConsulTestFixture {
	// testutil.NewTestServerConfig will skip the test if "consul" isn't in the system path.
	// We'd rather the test fail.
	defer func() {
		if t.Skipped() {
			t.Error("failing skipped test")
		}
	}()
	server := testutil.NewTestServer(t)
	store := NewConsulStore(Options{
		Address: server.HTTPAddr,
	}).(*consulStore)
	return &ConsulTestFixture{
		TestServer: server,
		Store:      store,
		Client:     store.client,
		T:          t,
	}
}

func (f *ConsulTestFixture) Close() {
	f.TestServer.Stop()
	time.Sleep(50 * time.Millisecond)
}
