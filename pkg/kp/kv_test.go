package kp

import (
	"testing"

	"github.com/hashicorp/consul/testutil"
)

func TestGetHealthNoEntry(t *testing.T) {
	store, s := makeStore(t)
	defer s.Stop()

	// Get a get without a key
	_, err := store.GetHealth("testservice", "testnode")
	if err != nil {
		t.Fatalf("GetHealth returned an error: %v", err)
	}
	_, err = store.GetServiceHealth("testservice")
	if err != nil {
		t.Fatalf("GetServiceHealth returned an error: %v", err)
	}
}

func TestGetHealthWithEntry(t *testing.T) {
	store, s := makeStore(t)
	defer s.Stop()

	// Put the key
	watch := WatchResult{
		Id:      "id",
		Node:    "node",
		Service: "service",
	}
	_, _, err := store.PutHealth(watch)
	if err != nil {
		t.Fatalf("PutHealth failed: %v", err)
	}

	// Get should work
	watchRes, err := store.GetHealth(watch.Service, watch.Node)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if watchRes.Id != watch.Id {
		t.Fatalf("watchRes and watch ids did not match. GetHealth failed: %#v", watchRes)
	}
	if watchRes.Node != watch.Node {
		t.Fatalf("watchRes and watch Node did not match. GetHealth failed: %#v", watchRes)
	}
	if watchRes.Service != watch.Service {
		t.Fatalf("watchRes and watch Service did not match. GetHealth failed: %#v", watchRes)
	}
}

func makeStore(t *testing.T) (Store, *testutil.TestServer) {
	// Create server
	server := testutil.NewTestServerConfig(t, nil)
	store := NewConsulStore(Options{
		Address: server.HTTPAddr,
	})
	return store, server
}
