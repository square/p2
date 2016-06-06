package checker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/types"

	. "github.com/anthonybishopric/gotcha"
	"github.com/hashicorp/consul/api"
)

type fakeConsulStore struct {
	results map[string]kp.WatchResult
}

func (f fakeConsulStore) GetHealth(service string, node types.NodeName) (kp.WatchResult, error) {
	return f.results[node.String()], nil
}
func (f fakeConsulStore) GetServiceHealth(service string) (map[string]kp.WatchResult, error) {
	return f.results, nil
}

func TestService(t *testing.T) {
	result1 := kp.WatchResult{
		Id:      "abc123",
		Node:    "node1",
		Service: "slug",
		Status:  "passing",
		Output:  "OK",
	}
	fakeStore := fakeConsulStore{
		results: map[string]kp.WatchResult{"node1": result1},
	}
	consulHC := consulHealthChecker{
		consulStore: fakeStore,
	}

	results, err := consulHC.Service("some_service")
	Assert(t).IsNil(err, "Unexpected error calling Service()")

	expected := health.Result{
		ID:      "abc123",
		Node:    "node1",
		Service: "slug",
		Status:  "passing",
		Output:  "OK",
	}
	Assert(t).AreEqual(results["node1"], expected, "Unexpected results calling Service()")
}

func TestPublishLatestHealth(t *testing.T) {
	// This channel imitates the channel that consulutil.WatchPrefix would return
	healthListChan := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	outCh := make(chan []*health.Result, 1)
	defer close(outCh)
	defer close(quitCh)

	errCh := publishLatestHealth(healthListChan, quitCh, outCh)

	go func() {
		err, open := <-errCh
		if err != nil {
			t.Fatal(err)
		}
		if !open {
			return
		}
	}()

	oldStatus := health.HealthState("passing")
	newStatus := health.HealthState("critical")
	hrOld := &health.Result{
		Status: oldStatus,
	}
	hrOldJSON, err := json.Marshal(hrOld)
	if err != nil {
		t.Fatalf("json marshal err: %v", err)
	}
	oldKV := &api.KVPair{Key: "health/service/node1.example.com", Value: hrOldJSON}

	hrNew := &health.Result{
		Status: newStatus,
	}
	hrNewJSON, err := json.Marshal(hrNew)
	if err != nil {
		t.Fatalf("json marshal err: %v", err)
	}
	newKV := &api.KVPair{Key: "health/service/node1.example.com", Value: hrNewJSON}

	// Basic test that publishLatestHealth drains the channels correctly
	// We write three times to ensure that at least one of the newKV values has flushed through the channel
	select {
	case healthListChan <- api.KVPairs{oldKV}:
	case <-time.After(1 * time.Second):
		t.Fatal("Failed to write to chan. Deadlock?")
	}

	select {
	case healthListChan <- api.KVPairs{newKV}:
	case <-time.After(1 * time.Second):
		t.Fatal("Failed to write to chan. Deadlock?")
	}

	select {
	case healthListChan <- api.KVPairs{newKV}:
	case <-time.After(1 * time.Second):
		t.Fatal("Failed to write to chan. Deadlock?")
	}

	select {
	case result := <-outCh:
		if len(result) < 1 {
			t.Fatalf("Got wrong number of results. Expected 1, got %d", len(result))
		}
		if result[0].Status != newStatus {
			t.Fatalf("expected status to match %s, was %s", newStatus, result[0].Status)
		}
		return
	case <-time.After(1 * time.Second):
		t.Fatal("oh no, timeout")
	}
}
