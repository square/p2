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

func (h ConsulHealthChecker) Service(serviceID string) (map[string][][]Result, error) {
	catalogEntries, _, err := h.client.Health().Service(serviceID, "", false, nil)
	if err != nil {
		return nil, util.Errorf("/health/service failed for %q: %s", serviceID, err)
	}

	ret := make(map[string][][]Result)
	for _, entry := range catalogEntries {
		// get entry in KV store for this service/node
		kvEntry, err := h.consulStore.GetHealth(serviceID, entry.Node.Node)
		if err != nil {
			return nil, util.Errorf("/health/service failed for %q on node %s: %s", serviceID, entry.Node.Node, err)
		}

		res := make([]Result, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			res = append(res, consulCheckToResult(*check))
		}
		kvRes := []Result{consulWatchToResult(kvEntry)}
		// place worst result from catalog and result from KV store in map
		ret[entry.Node.Node] = [][]Result{kvRes, res}
	}

	return ret, nil
}

func (h ConsulHealthChecker) WatchNodeService(nodename string, serviceID string, resultCh chan<- [][]Result, errCh chan<- error, quitCh <-chan struct{}) {
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
				out := make([][]Result, 0)
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
				out = append(out, []Result{consulWatchToResult(kvCheck)})
				out = append(out, catalogResults)
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

func consulWatchToResult(w kp.WatchResult) Result {
	return Result{
		ID:      w.Id,
		Node:    w.Node,
		Service: w.Service,
		Status:  HealthState(w.Status),
		Output:  w.Output,
	}
}

// each list in results is the health results from a given source
// this method gets the worst value for each source then returns the
// best of those worst values. Its the multi-source equivalent of
// FindWorst
func GetMultisourceResult(results [][]Result) (string, HealthState) {
	id := ""
	healthRes := Critical
	for _, value := range results {
		res := findWorstResult(value)
		if Compare(res.Status, healthRes) == 1 {
			healthRes = res.Status
			id = res.ID
		}
	}
	return id, healthRes
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
