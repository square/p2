package health

import (
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util"
)

type ConsulHealthChecker struct {
	client      *api.Client
	consulStore kp.Store
	WaitTime    time.Duration
}

func NewConsulHealthChecker(opts kp.Options) ConsulHealthChecker {
	return ConsulHealthChecker{
		client:      kp.NewConsulClient(opts),
		consulStore: kp.NewConsulStore(opts),
		WaitTime:    1 * time.Minute,
	}
}

type Result struct {
	ID      string
	Node    string
	Service string
	Status  HealthState
	Output  string
}

func (h ConsulHealthChecker) WatchNodeService(nodename string, serviceID string, resultCh chan<- Result, errCh chan<- error, quitCh <-chan struct{}) {
	defer close(resultCh)

	var curIndex uint64 = 0

	for {
		select {
		case <-quitCh:
			return
		case <-time.After(1 * time.Second):
			checks, meta, err := h.client.Health().Node(nodename, &api.QueryOptions{
				WaitIndex: curIndex,
				WaitTime:  h.WaitTime,
			})
			if err != nil {
				errCh <- err
			}
			kvCheck, err := h.consulStore.GetHealth(nodename, serviceID)
			if err != nil {
				errCh <- err
			} else {
				curIndex = meta.LastIndex
				catalogResults := make([]Result, 0)
				for _, check := range checks {
					outResult := consulCheckToResult(*check)
					// only retain checks if they're for this service, or for the
					// entire node
					if outResult.Service != serviceID && outResult.Service != "" {
						continue
					}
					catalogResults = append(catalogResults, outResult)
				}
				kv := consulWatchToResult(kvCheck)
				catalog := findWorstResult(catalogResults)
				resultCh <- findBestResult([]Result{kv, catalog})
			}
		}
	}
}

// Service returns a map where values are individual results (keys are nodes)
// The result is selected by choosing the best of all the worst results each source
// returned. If there is one source (or other sources are unavailable) the value returned
// will just be the worst result.
func (h ConsulHealthChecker) Service(serviceID string) (map[string]Result, error) {
	catalogEntries, _, err := h.client.Health().Service(serviceID, "", false, nil)
	if err != nil {
		return nil, util.Errorf("/health/service failed for %q: %s", serviceID, err)
	}
	// return map[nodenames (string)] to kp.WatchResult
	// get health of all instances of a service with 1 query
	kvEntries, err := h.consulStore.GetServiceHealth(serviceID)
	if err != nil {
		return nil, util.Errorf("/health/service failed for %q: %s", serviceID, err)
	}

	return selectResult(catalogEntries, kvEntries)
}

func selectResult(catalogEntries []*api.ServiceEntry, kvEntries map[string]kp.WatchResult) (map[string]Result, error) {
	ret := make(map[string]Result)
	for _, entry := range catalogEntries {
		res := make([]Result, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			res = append(res, consulCheckToResult(*check))
		}
		ret[entry.Node.Node] = findWorstResult(res)
	}

	for _, kvEntry := range kvEntries {
		res := consulWatchToResult(kvEntry)
		// if kvEntry already exists for this service take the best of kv store and catalog
		if _, ok := ret[kvEntry.Node]; ok {
			ret[kvEntry.Node] = findBestResult([]Result{res, ret[kvEntry.Node]})
		} else {
			ret[kvEntry.Node] = res
		}
	}

	return ret, nil
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

func consulWatchToResult(w kp.WatchResult) Result {
	return Result{
		ID:      w.Id,
		Node:    w.Node,
		Service: w.Service,
		Status:  HealthState(w.Status),
		Output:  w.Output,
	}
}

// Returns the poorest status of all checks in the given list, plus the check
// ID of one of those checks.
func FindWorst(results []Result) (string, HealthState) {
	worst := findWorstResult(results)
	return worst.ID, worst.Status
}

func FindBest(results []Result) (string, HealthState) {
	best := findBestResult(results)
	return best.ID, best.Status
}

func findWorstResult(results []Result) Result {
	ret := Passing
	retVal := results[0]
	for _, res := range results {
		if Compare(res.Status, ret) < 0 {
			ret = res.Status
			retVal = res
		}
	}
	return retVal
}

func findBestResult(results []Result) Result {
	ret := Critical
	retVal := results[0]
	for _, res := range results {
		if Compare(res.Status, ret) >= 0 {
			ret = res.Status
			retVal = res
		}
	}
	return retVal
}
