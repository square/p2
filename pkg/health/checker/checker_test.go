package checker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/health"
	hc "github.com/square/p2/pkg/health/client"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
)

type fakeConsulStore struct {
	results map[string]consul.WatchResult
}

func (f fakeConsulStore) GetHealth(service string, node types.NodeName) (consul.WatchResult, error) {
	return f.results[node.String()], nil
}
func (f fakeConsulStore) GetServiceHealth(service string) (map[string]consul.WatchResult, error) {
	return f.results, nil
}

type fakeHealthClient struct {
	HealthResponses map[string]hc.HealthResponse
	MonitorDelay    time.Duration
}

func (f fakeHealthClient) HealthCheck(ctx context.Context, req *hc.HealthRequest) (health.HealthState, error) {
	return f.HealthResponses[req.Url].Health, nil
}

func (f fakeHealthClient) HealthMonitor(ctx context.Context, req *hc.HealthRequest, resultCh chan *hc.HealthResponse) error {
	timer := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			timer.Reset(f.MonitorDelay)
			if healthResponse, ok := f.HealthResponses[req.Url]; ok {
				resultCh <- &healthResponse
				continue
			}
			resultCh <- &hc.HealthResponse{
				HealthRequest: *req,
				Health:        health.Unknown,
				Error:         nil,
			}
		}
	}
}

func (f fakeHealthClient) HealthCheckEndpoints(ctx context.Context, req *hc.HealthEndpointsRequest) (map[string]health.HealthState, error) {
	ret := make(map[string]health.HealthState)
	for _, endpoint := range req.Endpoints {
		healthResponse, ok := f.HealthResponses[endpoint]
		if ok {
			ret[endpoint] = healthResponse.Health
		} else {
			ret[endpoint] = health.Unknown
		}
	}
	return ret, nil
}

func TestNodeIDsToStatusEndpoints(t *testing.T) {
	nodeIDs := []types.NodeName{"node1"}
	statusStanza := manifest.StatusStanza{Port: 1}
	expected := []string{
		"https://node1:1/_status",
	}
	statusEndpoints := nodeIDsToStatusEndpoints(nodeIDs, statusStanza)
	if len(statusEndpoints) != len(nodeIDs) {
		t.Fatalf("Expected length of output of nodeIDsToStatusEndpoints to equal length of input nodeIDs. Expected %d but got %d", len(nodeIDs), len(statusEndpoints))
	}
	if statusEndpoints[0] != expected[0] {
		t.Fatalf("Expected statusEndpoint to be %s but got %s", expected[0], statusEndpoints[0])
	}

	statusStanza = manifest.StatusStanza{
		HTTP: true,
		Path: "path",
		Port: 1,
	}
	expected = []string{
		"http://node1:1/path",
	}
	statusEndpoints = nodeIDsToStatusEndpoints(nodeIDs, statusStanza)
	if statusEndpoints[0] != expected[0] {
		t.Fatalf("Expected statusEndpoint to be %s but got %s", expected[0], statusEndpoints[0])
	}
}

func TestStatusURLToNodeName(t *testing.T) {
	nodeIDs := []types.NodeName{"node1"}
	statusStanza := manifest.StatusStanza{
		Port: 1,
	}
	statusEndpoints := nodeIDsToStatusEndpoints(nodeIDs, statusStanza)
	nodeID, err := statusURLToNodeName(statusEndpoints[0])
	if err != nil {
		t.Fatalf("Unexpected error in statusURLToNodeName: %v", err)
	}
	if nodeID != nodeIDs[0] {
		t.Fatalf("Expected nodeID to be %s but got %s", nodeIDs[0], nodeID)
	}
}

func TestService(t *testing.T) {
	result1 := consul.WatchResult{
		Id:      "abc123",
		Node:    "node1",
		Service: "slug",
		Status:  "passing",
	}
	fakeStore := fakeConsulStore{
		results: map[string]consul.WatchResult{"node1": result1},
	}
	hc := healthChecker{
		consulStore: fakeStore,
	}

	results, err := hc.Service("some_service")
	Assert(t).IsNil(err, "Unexpected error calling Service()")

	expected := health.Result{
		ID:      "abc123",
		Node:    "node1",
		Service: "slug",
		Status:  "passing",
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
