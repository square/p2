package checker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/kptest"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
)

type fakeConsulStore struct {
	results map[string]kp.WatchResult
}

func (f fakeConsulStore) GetHealth(service, node string) (kp.WatchResult, error) {
	return f.results[node], nil
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

func TestWatchHealth(t *testing.T) {
	fakeKV := kptest.NewFakeKV()
	healthChecker := &consulHealthChecker{
		kv: fakeKV,
	}

	retChan := make(chan []*health.Result)
	errChan := make(chan error)
	quitChan := make(chan struct{})

	dummyHealthResult := &health.Result{
		ID:      "ID",
		Node:    "node1.example.com",
		Service: "Service",
		Status:  "passing",
		Output:  "output",
	}
	dummyBuf, err := json.Marshal(dummyHealthResult)
	if err != nil {
		t.Fatalf("json marshal err: %v", err)
	}

	_, _, err = fakeKV.CAS(&api.KVPair{Key: "health/service/node1.example.com", Value: dummyBuf}, nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	go func() {
		defer func() {
			quitChan <- struct{}{}
		}()
		for {
			select {
			case result := <-retChan:
				res := result[0]
				if res.ID != "ID" {
					t.Fatalf("Expected ID to match")
				}

				if res.Node != "node1.example.com" {
					t.Fatalf("Expected Node to match")
				}
				if res.Service != "Service" {
					t.Fatalf("Expected Service to match")
				}
				if res.Status != "passing" {
					t.Fatalf("Expected Status to match")
				}
				if res.Output != "output" {
					t.Fatalf("Expected Output to match")
				}
				return
			case err := <-errChan:
				t.Fatalf("unexpected error: %v", err)
			case <-time.After(5 * time.Second):
				t.Error("Timed out waiting for message")
			}
		}
	}()

	// blocks _and_ writes to our chan
	healthChecker.WatchHealth(retChan, errChan, quitChan)
}
