package kptest

import (
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/store"

	"testing"
)

func TestFakeServiceHealth(t *testing.T) {
	fake := FakePodStore{}
	targetService := "paladin"
	targetHost := store.NodeName("aaa2.dfw.square")
	targetStatus := "healthy"

	fake.healthResults = map[string]kp.WatchResult{
		kp.HealthPath("shrimpy", "aaa1.dfw.square"): {
			Service: "shrimpy",
			Status:  "critical",
		},
		kp.HealthPath(targetService, targetHost): {
			Service: targetService,
			Status:  targetStatus,
		},
	}

	serviceRes, err := fake.GetServiceHealth(targetService)
	if err != nil {
		t.Fatal(err)
	}
	if len(serviceRes) != 1 {
		t.Fatalf("Expected %v to have a single health entry, found %v", targetService, len(serviceRes))
	}
	watchResult, ok := serviceRes[kp.HealthPath(targetService, targetHost)]
	if !ok {
		t.Fatalf("Expected to find a result for %v", targetHost)
	}
	if watchResult.Status != targetStatus {
		t.Fatalf("Status didn't match expected: %v", watchResult.Status)
	}
}
