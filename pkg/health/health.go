// Package health provides a generic object type for representing a service's health
// on a particlar node. It also provides an implementation of health checking based
// on the Consul Healthcheck API.
package health

import (
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type ServiceStatus struct {
	Statuses map[string]*ServiceNodeStatus `yaml:"statuses"`
}

func (s *ServiceStatus) ForNode(node string) (*ServiceNodeStatus, error) {
	for _, status := range s.Statuses {
		if status.Node == node {
			return status, nil
		}
	}
	return &ServiceNodeStatus{
		Node:    node,
		Version: "",
		Health:  Unknown,
	}, NoStatusGiven
}

type ServiceNodeStatus struct {
	Node    string      `yaml:"node"`
	Health  HealthState `yaml:"healthy"`
	Version string      `yaml:"version"`
}

func (s *ServiceNodeStatus) IsCurrentVersion(version string) bool {
	return s.Version == version
}

type ConsulHealthChecker struct {
	Health *api.Health
	Store  kp.Store
}

func NewConsulHealthChecker(opts kp.Options) *ConsulHealthChecker {
	client := kp.NewConsulClient(opts)
	return &ConsulHealthChecker{
		Health: client.Health(),
		Store:  *kp.NewStore(opts),
	}
}

func (s *ConsulHealthChecker) toNodeStatus(serviceID string, entry api.ServiceEntry) (*ServiceNodeStatus, error) {
	version := ""
	manifest, _, err := s.Store.Pod(kp.RealityPath(entry.Node.Node, serviceID))
	if err != nil && err != pods.NoCurrentManifest {
		return nil, err
	}

	if manifest != nil {
		version, err = manifest.SHA()
		if err != nil {
			return nil, err
		}
	}

	nodeStatus := ServiceNodeStatus{
		Node:    entry.Node.Node,
		Version: version,
		Health:  Passing,
	}
	for _, check := range entry.Checks {
		// degrade the health of this service to the poorest of all its associated checks
		thisHealth := ToHealthState(check.Status)
		if Compare(thisHealth, nodeStatus.Health) == -1 {
			nodeStatus.Health = thisHealth
		}
	}
	return &nodeStatus, nil
}

func (s *ConsulHealthChecker) LookupHealth(serviceID string) (*ServiceStatus, error) {
	options := api.QueryOptions{}
	entries, _, err := s.Health.Service(serviceID, "", false, &options)
	if err != nil {
		return nil, util.Errorf("/health/service failed")
	}
	serviceStatus := ServiceStatus{
		Statuses: make(map[string]*ServiceNodeStatus),
	}

	var firstErr error = nil
	for _, entry := range entries {
		if serviceID == entry.Service.ID {
			status, err := s.toNodeStatus(serviceID, *entry)
			if err != nil && firstErr == nil {
				firstErr = err
			}
			serviceStatus.Statuses[entry.Node.Node] = status
		}
	}
	return &serviceStatus, firstErr
}

// Get a channel of service statuses watched by this health checker. This approach
// creates a watch for every lookup. A much more complex broadcast implementation might
// be possible so long as it preserves order and allows new subscribers to see messages
// that were sent before they began listening.
func (s *ConsulHealthChecker) WatchHealth(serviceID string, statusCh chan<- ServiceNodeStatus, errCh chan<- error, quitCh <-chan struct{}) {
	defer close(statusCh)
	defer close(errCh)

	var curIndex uint64 = 0

	for {
		select {
		case <-quitCh:
			return
		case <-time.After(1 * time.Second):
			checks, meta, err := s.Health.Service(serviceID, "", false, &api.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				errCh <- util.Errorf("/health/service failed")
			} else {
				curIndex = meta.LastIndex
				for _, check := range checks {
					status, err := s.toNodeStatus(serviceID, *check)
					if err != nil {
						errCh <- err
					} else {
						statusCh <- *status
					}
				}
			}
		}
	}
}
