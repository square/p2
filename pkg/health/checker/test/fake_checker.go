package test

import (
	"context"
	"fmt"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
)

// TODO: replication/common_setup_test.go has some things that could be moved here

type singleServiceChecker struct {
	service string
	health  map[types.NodeName]health.Result
}

// NewSingleService reports a fixed health result for a single service only
func NewSingleService(service string, health map[types.NodeName]health.Result) checker.HealthChecker {
	return &singleServiceChecker{
		service: service,
		health:  health,
	}
}

func (s singleServiceChecker) WatchPodOnNode(ctx context.Context, nodename types.NodeName, podID types.PodID, status manifest.StatusStanza) (chan health.Result, chan error) {
	resultCh := make(chan health.Result)
	errCh := make(chan error)
	result, ok := s.health[nodename]
	if !ok {
		result = health.Result{Status: health.Critical}
	}

	go func() {
		resultCh <- result
	}()

	return resultCh, errCh
}

func (s singleServiceChecker) WatchService(
	ctx context.Context,
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	watchDelay time.Duration,
	status manifest.StatusStanza,
) {
	panic("WatchService not implemented")

}

func (s singleServiceChecker) Service(serviceID string, status manifest.StatusStanza) (map[types.NodeName]health.Result, error) {
	if serviceID != s.service {
		return nil, fmt.Errorf("Wrong service %s given, I only have health for %s", serviceID, s.service)
	}
	return s.health, nil
}

type AlwaysHappyHealthChecker struct {
	allNodes []types.NodeName
}

// creates an implementation of checker.HealthChecker that always reports
// satisfied health checks for testing purposes
func HappyHealthChecker(nodes []types.NodeName) checker.HealthChecker {
	return AlwaysHappyHealthChecker{nodes}
}

func (h AlwaysHappyHealthChecker) WatchPodOnNode(
	ctx context.Context,
	nodeName types.NodeName,
	podID types.PodID,
	status manifest.StatusStanza,
) (chan health.Result, chan error) {
	resultCh := make(chan health.Result)

	happyResult := health.Result{
		ID:     podID,
		Status: health.Passing,
	}
	go func() {
		defer close(resultCh)
		for {
			select {
			case <-ctx.Done():
				return
			case resultCh <- happyResult:
			}
		}
	}()

	return resultCh, nil
}

func (h AlwaysHappyHealthChecker) Service(serviceID string, status manifest.StatusStanza) (map[types.NodeName]health.Result, error) {
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
	ctx context.Context,
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	watchDelay time.Duration,
	status manifest.StatusStanza,
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
		case <-ctx.Done():
			return
		case resultCh <- allHappy:
		}
	}
}
