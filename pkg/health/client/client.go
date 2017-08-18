package client

import (
	"context"

	"github.com/square/p2/pkg/health"
)

type HealthRequest struct {
	Url      string
	Protocol string
}

type HealthResponse struct {
	HealthRequest
	Health health.HealthState
	Error  error
}

type HealthServiceClient interface {
	HealthCheck(req *HealthRequest) (health.HealthState, error)
	HealthMonitor(ctx context.Context, req *HealthRequest, resultChan chan *HealthResponse)
	MultiHealthMonitor(ctx context.Context, reqs []*HealthRequest, resultChan chan *HealthResponse)
}
