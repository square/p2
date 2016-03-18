package test

import (
	"fmt"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
)

// TODO: replication/common_setup_test.go has some things that could be moved here

type singleServiceChecker struct {
	service string
	health  map[string]health.Result
}

// NewSingleService reports a fixed health result for a single service only
func NewSingleService(service string, health map[string]health.Result) checker.ConsulHealthChecker {
	return &singleServiceChecker{
		service: service,
		health:  health,
	}
}

func (s singleServiceChecker) WatchNodeService(nodename string, serviceID string, resultCh chan<- health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	panic("WatchNodeService not implemented")
}

func (s singleServiceChecker) WatchService(serviceID string, resultCh chan<- map[string]health.Result, errCh chan<- error, quitCh <-chan struct{}) {
	panic("WatchService not implemented")

}

func (s singleServiceChecker) Service(serviceID string) (map[string]health.Result, error) {
	if serviceID != s.service {
		return nil, fmt.Errorf("Wrong service %s given, I only have health for %s", serviceID, s.service)
	}
	return s.health, nil
}
