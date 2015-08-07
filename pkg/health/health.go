package health

import (
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util"
)

type ConsulHealthChecker struct {
	client      *api.Client
	health      consulHealth
	consulStore kp.Store
	WaitTime    time.Duration
}

type consulHealth interface {
	Node(string, *api.QueryOptions) ([]*api.HealthCheck, *api.QueryMeta, error)
}

func NewConsulHealthChecker(opts kp.Options) ConsulHealthChecker {
	client := kp.NewConsulClient(opts)
	return ConsulHealthChecker{
		client:      client,
		health:      client.Health(),
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

type HealthEmpty struct {
	Res string
}

func (h *HealthEmpty) Error() string {
	return h.Res
}

func (h ConsulHealthChecker) WatchNodeService(nodename string, serviceID string, resultCh chan<- Result, errCh chan<- error, quitCh <-chan struct{}) {
	defer close(resultCh)

	var curIndex uint64 = 0

	for {
		select {
		case <-quitCh:
			return
		case <-time.After(1 * time.Second):
			var err error
			var checks []*api.HealthCheck
			checks, curIndex, err = h.fetchNodeHealth(nodename, curIndex)
			if err != nil {
				errCh <- err
			}
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
			// GetHealth will fail if there are no kv results
			kvCheck, err := h.consulStore.GetHealth(nodename, serviceID)
			pickHealthResult(catalogResults, kvCheck, err, resultCh, errCh)
		}
	}
}

// if there are no health results available will place error in errCh then return
func pickHealthResult(catalogResults []Result, kvCheck kp.WatchResult, kvCheckError error, resultCh chan<- Result, errCh chan<- error) {
	if kvCheckError != nil {
		// if there are neither kv nor catalog results
		if len(catalogResults) == 0 {
			fmt.Println(kvCheckError)
			errCh <- kvCheckError
			return
		} else {
			// there are no kv results but there are catalog results
			catalogCheckResult, _ := findWorstResult(catalogResults)
			resultCh <- catalogCheckResult
		}
	} else {
		// there are kv checks
		kvCheckResult := consulWatchToResult(kvCheck)
		if len(catalogResults) == 0 {
			// there are kv results but not catalog results
			resultCh <- kvCheckResult
		} else {
			// there are both kv and catalog results
			catalogCheckResult, _ := findWorstResult(catalogResults)
			best, _ := findBestResult([]Result{kvCheckResult, catalogCheckResult})
			resultCh <- best
		}
	}
}

func (h ConsulHealthChecker) fetchNodeHealth(nodename string, curIndex uint64) ([]*api.HealthCheck, uint64, error) {
	checks, meta, err := h.health.Node(nodename, &api.QueryOptions{
		WaitIndex: curIndex,
		WaitTime:  h.WaitTime,
	})
	if err != nil {
		// return the current index, since there was an error
		return nil, curIndex, err
	}
	return checks, meta.LastIndex, nil
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

	return selectResult(catalogEntries, kvEntries), nil
}

func selectResult(catalogEntries []*api.ServiceEntry, kvEntries map[string]kp.WatchResult) map[string]Result {
	ret := make(map[string]Result)

	for _, entry := range catalogEntries {
		res := make([]Result, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			res = append(res, consulCheckToResult(*check))
		}
		val, HEErr := findWorstResult(res)
		if HEErr == nil {
			ret[entry.Node.Node] = val
		}
	}

	for _, kvEntry := range kvEntries {
		res := consulWatchToResult(kvEntry)
		// if kvEntry already exists for this service take the best of kv store and catalog
		if _, ok := ret[kvEntry.Node]; ok {
			val, HEErr := findBestResult([]Result{res, ret[kvEntry.Node]})
			if HEErr == nil {
				ret[kvEntry.Node] = val
			}
		} else {
			ret[kvEntry.Node] = res
		}
	}

	return ret
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
func FindWorst(results []Result) (string, HealthState, *HealthEmpty) {
	if len(results) == 0 {
		return "", Critical, &HealthEmpty{
			Res: "no results were passed to FindWorst",
		}
	}
	worst, HEErr := findWorstResult(results)
	if HEErr != nil {
		return "", Critical, HEErr
	}
	return worst.ID, worst.Status, HEErr
}

func FindBest(results []Result) (string, HealthState, *HealthEmpty) {
	if len(results) == 0 {
		return "", Critical, &HealthEmpty{
			Res: "no results were passed to FindBest",
		}
	}
	best, HEErr := findBestResult(results)
	if HEErr != nil {
		return "", Critical, HEErr
	}
	return best.ID, best.Status, HEErr
}

func findWorstResult(results []Result) (Result, *HealthEmpty) {
	if len(results) == 0 {
		return Result{Status: Critical}, &HealthEmpty{
			Res: "no results were passed to findWorstResult",
		}
	}
	ret := Passing
	retVal := results[0]
	for _, res := range results {
		if Compare(res.Status, ret) < 0 {
			ret = res.Status
			retVal = res
		}
	}
	return retVal, nil
}

func findBestResult(results []Result) (Result, *HealthEmpty) {
	if len(results) == 0 {
		return Result{Status: Critical}, &HealthEmpty{
			Res: "no results were passed to findBestResult",
		}
	}
	ret := Critical
	retVal := results[0]
	for _, res := range results {
		if Compare(res.Status, ret) >= 0 {
			ret = res.Status
			retVal = res
		}
	}
	return retVal, nil
}
