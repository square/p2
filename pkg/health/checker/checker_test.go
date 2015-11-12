package checker

import (
	"testing"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
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
