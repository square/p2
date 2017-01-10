package store

import (
	"testing"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/store"
)

type FakeHealthChecker struct {
	results chan []*health.Result
	ready   chan struct{}
}

func (hc *FakeHealthChecker) WatchNodeService(nodename store.NodeName, serviceID string, resultCh chan<- health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	panic("not implemented")
}

func (hc *FakeHealthChecker) WatchService(serviceID string, resultCh chan<- map[store.NodeName]health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	panic("not implemented")
}

func (hc *FakeHealthChecker) Service(serviceID string) (map[store.NodeName]health.Result, error) {
	panic("not implemented")
}

func (hc *FakeHealthChecker) WatchHealth(resultCh chan []*health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	hc.results = resultCh
	close(hc.ready)
	<-quitCh
}

func (hc *FakeHealthChecker) Send(r []*health.Result) {
	<-hc.ready
	// Send three times to be sure at least one result was fully handled before returning
	hc.results <- r
	hc.results <- r
	hc.results <- r
}

func NewFakeHealthStore() (HealthStore, *FakeHealthChecker) {
	hc := &FakeHealthChecker{
		ready: make(chan struct{}),
	}
	return NewHealthStore(hc), hc
}

func TestStartWatchBasic(t *testing.T) {
	quitCh := make(chan struct{})
	defer close(quitCh)
	hs, checker := NewFakeHealthStore()
	go hs.StartWatch(quitCh)

	node := store.NodeName("abc01.sjc1")
	podID1 := store.PodID("podID1")
	podID2 := store.PodID("podID2")

	result := hs.Fetch(podID1, node)
	if result != nil {
		t.Errorf("expected cache to start empty, found %v", result)
	}

	checker.Send([]*health.Result{
		{ID: podID1, Node: node},
		{ID: podID2, Node: node},
	})

	result = hs.Fetch(podID1, node)
	if result == nil {
		t.Errorf("expected health store to have %s", podID1)
	}

	result = hs.Fetch(podID2, node)
	if result == nil {
		t.Errorf("expected health store to have %s", podID2)
	}
}
