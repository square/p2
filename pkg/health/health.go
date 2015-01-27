// Package health provides a generic object type for representing a service's health
// on a particlar node. It also provides an implementation of health checking based
// on the Consul Healthcheck API.
package health

import (
	"fmt"

	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kv-consul"
)

type HealthCheckState string

var (
	Passing  = HealthCheckState("passing")
	Any      = HealthCheckState("any")
	Unknown  = HealthCheckState("unknown")
	Warning  = HealthCheckState("warning")
	Critical = HealthCheckState("critical")

	NoStatusGiven = fmt.Errorf("No status given")
)

func toHealthState(v string) HealthCheckState {
	if v == "passing" {
		return Passing
	}
	if v == "any" {
		return Any
	}
	if v == "unknown" {
		return Unknown
	}
	if v == "warning" {
		return Warning
	}
	if v == "critical" {
		return Critical
	}
	return Unknown
}

type ServiceStatus struct {
	Statuses map[string]*ServiceNodeStatus `yaml:"statuses"`
}

func (s *ServiceStatus) ForNode(node string) (*ServiceNodeStatus, error) {
	for _, status := range s.Statuses {
		if status.Node == node {
			return status, nil
		}
	}
	return nil, NoStatusGiven
}

type ServiceNodeStatus struct {
	Node    string `yaml:"node"`
	Healthy bool   `yaml:"healthy"`
	Version string `yaml:"version"`
}

func (s *ServiceNodeStatus) IsCurrentVersion(version string) bool {
	return s.Version == version
}

type ConsulHealthChecker struct {
	Health *consulapi.Health
	Store  kp.Store
}

func NewConsulHealthChecker(store kp.Store, consulHealth *consulapi.Health) *ConsulHealthChecker {
	return &ConsulHealthChecker{
		Health: consulHealth,
		Store:  store,
	}
}

func (s *ConsulHealthChecker) toNodeStatus(entry consulapi.ServiceEntry) *ServiceNodeStatus {
	nodeStatus := ServiceNodeStatus{
		Node:    entry.Node.Node,
		Version: entry.Service.Tags[0],
		Healthy: true,
	}
	for _, check := range entry.Checks {
		checkPassing := toHealthState(check.Status) == Passing
		nodeStatus.Healthy = nodeStatus.Healthy && checkPassing
	}
	return &nodeStatus
}

func (s *ConsulHealthChecker) LookupHealth(serviceID string) (*ServiceStatus, error) {
	options := consulapi.QueryOptions{}
	entries, _, err := s.Health.Service(serviceID, "", false, &options)
	if err != nil {
		return nil, err
	}
	serviceStatus := ServiceStatus{
		Statuses: make(map[string]*ServiceNodeStatus),
	}
	for _, entry := range entries {
		if serviceID == entry.Service.ID {
			serviceStatus.Statuses[entry.Node.Node] = s.toNodeStatus(*entry)
		}
	}
	return &serviceStatus, nil
}

// Get a channel of service statuses watched by this health checker. This approach
// creates a watch for every lookup. A much more complex broadcast implementation might
// be possible so long as it preserves order and allows new subscribers to see messages
// that were sent before they began listening.
func (s *ConsulHealthChecker) WatchHealth(serviceID string, statusChan chan ServiceNodeStatus, statusErrChan chan error, statusQuitChan chan struct{}) {
	options := consulapi.QueryOptions{}

	healthCh := make(chan []*consulapi.ServiceEntry)
	errCh := make(chan error)
	quitCh := make(chan struct{})
	defer close(quitCh)
	go ppkv.WatchServiceHealth(s.Health, serviceID, "", false, options, healthCh, errCh, quitCh)

	for {
		select {
		case res := <-healthCh:
			for _, entry := range res {
				nodeStatus := s.toNodeStatus(*entry)
				select {
				case statusChan <- *nodeStatus:
				case <-statusQuitChan:
					return
				}
			}
		case err := <-errCh:
			select {
			case statusErrChan <- err:
			case <-statusQuitChan:
			}

			return
		case <-statusQuitChan:
			return
		}
	}
}
