package kp

import (
	"testing"
)

var watch = WatchResult{
	Id:      "id",
	Node:    "node",
	Service: "service",
}

func TestGetHealthNoEntry(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	// Get a get without a key
	_, err := f.Store.GetHealth("testservice", "testnode")
	if err != nil {
		t.Fatalf("GetHealth returned an error: %v", err)
	}
	_, err = f.Store.GetServiceHealth("testservice")
	if err != nil {
		t.Fatalf("GetServiceHealth returned an error: %v", err)
	}
}

func TestGetHealthWithEntry(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	putTestHealthEntry(t, f)

	// Get should work
	watchRes, err := f.Store.GetHealth(watch.Service, watch.Node)
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

func TestDeleteHealth(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	putTestHealthEntry(t, f)

	f.Store.DeleteHealth("service", "node")

	// Get should return empty work
	watchRes, err := f.Store.GetHealth(watch.Service, watch.Node)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	emptyWatchResult := WatchResult{}
	if watchRes != emptyWatchResult {
		t.Errorf("Expected deleted watch result not to exist, was %#v", watchRes)
	}
}

func putTestHealthEntry(t *testing.T, f *ConsulTestFixture) {
	_, _, err := f.Store.PutHealth(watch)
	if err != nil {
		t.Fatalf("PutHealth failed: %v", err)
	}
}
