package store

import (
	"testing"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/types"
)

type FakeHealthChecker struct {
	healthResults chan []*health.Result
}

func (hc *FakeHealthChecker) WatchNodeService(nodename string, serviceID string, resultCh chan<- health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	panic("not implemented")
}

func (hc *FakeHealthChecker) WatchService(serviceID string, resultCh chan<- map[string]health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	panic("not implemented")
}

func (hc *FakeHealthChecker) Service(serviceID string) (map[string]health.Result, error) {
	panic("not implemented")
}

func (hc *FakeHealthChecker) WatchHealth(resultCh chan []*health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	for {
		select {
		case result := <-hc.healthResults:
			resultCh <- result
		case <-quitCh:
			return
		}
	}
}

func NewFakeHealthStore() (healthChecker HealthStore, healthValues chan []*health.Result) {
	healthResults := make(chan []*health.Result) // real clients should use a buffered chan. This is unbuffered to simplify concurrency in this test
	hc := &FakeHealthChecker{
		healthResults: healthResults,
	}
	hs := NewHealthStore(hc)

	return hs, healthResults
}

func TestStartWatchBasic(t *testing.T) {
	hs, healthResults := NewFakeHealthStore()
	quitCh := make(chan struct{})

	go func() {
		hs.StartWatch(quitCh)
	}()

	node := "abc01.sjc1"
	podID1 := types.PodID("podID1")
	podID2 := types.PodID("podID2")

	result := hs.Fetch(podID1, node)
	if result != nil {
		t.Errorf("expected cache to start empty, found %v", result)
	}

	// Write to this channel three times to ensure that the 1st write is processed before
	// we validate the behaviour. Testing concurrent code is difficult
	healthResults <- []*health.Result{
		&health.Result{ID: podID1, Node: node},
		&health.Result{ID: podID2, Node: node},
	}

	healthResults <- []*health.Result{
		&health.Result{ID: podID1, Node: node},
		&health.Result{ID: podID2, Node: node},
	}

	healthResults <- []*health.Result{
		&health.Result{ID: podID1, Node: node},
		&health.Result{ID: podID2, Node: node},
	}

	result = hs.Fetch(podID1, node)
	if result == nil {
		t.Errorf("expected health store to have %s", podID1)
	}

	result = hs.Fetch(podID2, node)
	if result == nil {
		t.Errorf("expected health store to have %s", podID2)
	}
}
