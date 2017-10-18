package test

import (
	"fmt"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/types"
)

// TODO: replication/common_setup_test.go has some things that could be moved here

type singleServiceChecker struct {
	service string
	health  map[types.NodeName]health.Result
}

// NewSingleService reports a fixed health result for a single service only
func NewSingleService(service string, health map[types.NodeName]health.Result) checker.ConsulHealthChecker {
	return &singleServiceChecker{
		service: service,
		health:  health,
	}
}

func (s singleServiceChecker) WatchNodeService(nodename types.NodeName, serviceID string, resultCh chan<- health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	panic("WatchNodeService not implemented")
}

func (s singleServiceChecker) WatchService(serviceID string, resultCh chan<- map[types.NodeName]health.Result, errCh chan<- error, quitCh <-chan struct{}, watchDelay time.Duration) {
	panic("WatchService not implemented")

}

func (s singleServiceChecker) WatchHealth(_ chan []*health.Result, errCh chan<- error, quitCh <-chan struct{}, jitterWindow time.Duration) {
	panic("WatchHealth not implemented")
}

func (s singleServiceChecker) Service(serviceID string) (map[types.NodeName]health.Result, error) {
	if serviceID != s.service {
		return nil, fmt.Errorf("Wrong service %s given, I only have health for %s", serviceID, s.service)
	}
	return s.health, nil
}

type AlwaysHappyHealthChecker struct {
	allNodes []types.NodeName
}

// creates an implementation of checker.ConsulHealthChecker that always reports
// satisfied health checks for testing purposes
func HappyHealthChecker(nodes []types.NodeName) checker.ConsulHealthChecker {
	return AlwaysHappyHealthChecker{nodes}
}

func (h AlwaysHappyHealthChecker) WatchNodeService(
	nodeName types.NodeName,
	serviceID string,
	resultCh chan<- health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	happyResult := health.Result{
		ID:     types.PodID(serviceID),
		Status: health.Passing,
	}
	for {
		select {
		case <-quitCh:
			return
		case resultCh <- happyResult:
		}
	}
}

func (h AlwaysHappyHealthChecker) Service(serviceID string) (map[types.NodeName]health.Result, error) {
	results := make(map[types.NodeName]health.Result)
	for _, node := range h.allNodes {
		results[node] = health.Result{
			ID:     types.PodID(serviceID),
			Status: health.Passing,
		}
	}
	return results, nil
}

func (h AlwaysHappyHealthChecker) WatchService(
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
	watchDelay time.Duration,
) {
	allHappy := make(map[types.NodeName]health.Result)
	for _, node := range h.allNodes {
		allHappy[node] = health.Result{
			ID:     types.PodID(serviceID),
			Status: health.Passing,
		}
	}
	for {
		select {
		case <-quitCh:
			return
		case resultCh <- allHappy:
		}
	}
}

func (h AlwaysHappyHealthChecker) WatchHealth(
	_ chan []*health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
	jitterWindow time.Duration,
) {
	panic("not implemented")
}
