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

type HealthEndpointsRequest struct {
	Endpoints []string
	Protocol  string
}

type HealthEndpointsResponse struct {
	HealthResponses map[string]HealthResponse
}

type HealthServiceClient interface {
	HealthCheck(ctx context.Context, req *HealthRequest) (health.HealthState, error)
	HealthMonitor(ctx context.Context, req *HealthRequest, resultChan chan *HealthResponse) error
	HealthCheckEndpoints(ctx context.Context, req *HealthEndpointsRequest) (map[string]health.HealthState, error)
	HealthMonitorEndpoints(ctx context.Context, req *HealthEndpointsRequest, resultChan chan *HealthResponse) error
}
