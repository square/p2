package health

import (
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util"
)

type ConsulHealthChecker struct {
	client *api.Client
}

func NewConsulHealthChecker(opts kp.Options) ConsulHealthChecker {
	return ConsulHealthChecker{
		client: kp.NewConsulClient(opts),
	}
}

type Result struct {
	ID      string
	Node    string
	Service string
	Status  HealthState
	Output  string
}

func (h ConsulHealthChecker) Service(serviceID string) (map[string][]Result, error) {
	entries, _, err := h.client.Health().Service(serviceID, "", false, nil)
	if err != nil {
		return nil, util.Errorf("/health/service failed for %q", serviceID)
	}

	ret := make(map[string][]Result)
	for _, entry := range entries {
		ret[entry.Node.Node] = make([]Result, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			ret[entry.Node.Node] = append(ret[entry.Node.Node], consulCheckToResult(*check))
		}
	}

	return ret, nil
}

func (h ConsulHealthChecker) WatchNodeService(nodename string, serviceID string, resultCh chan<- []Result, errCh chan<- error, quitCh <-chan struct{}) {
	defer close(resultCh)

	var curIndex uint64 = 0

	for {
		select {
		case <-quitCh:
			return
		case <-time.After(1 * time.Second):
			checks, meta, err := h.client.Health().Node(nodename, &api.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				errCh <- err
			} else {
				curIndex = meta.LastIndex
				out := make([]Result, 0)
				for _, check := range checks {
					outResult := consulCheckToResult(*check)
					// only retain checks if they're for this service, or for the
					// entire node
					if outResult.Service != serviceID && outResult.Service != "" {
						continue
					}
					out = append(out, outResult)
				}
				resultCh <- out
			}
		}
	}
}

func consulCheckToResult(c api.HealthCheck) Result {
	return Result{
		ID:      c.CheckID,
		Node:    c.Node,
		Service: c.ServiceID,
		Status:  HealthState(c.Status),
		Output:  c.Output,
	}
}

// Returns the poorest status of all checks in the given list, plus the check
// ID of one of those checks.
func FindWorst(results []Result) (string, HealthState) {
	ret := Passing
	retID := results[0].ID
	for _, res := range results {
		if Compare(res.Status, ret) < 0 {
			ret = res.Status
			retID = res.ID
		}
	}

	return retID, ret
}
