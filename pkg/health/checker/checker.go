package checker

import (
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util"
)

type consulHealthChecker struct {
	client      *api.Client
	health      consulHealth
	consulStore kp.Store
	WaitTime    time.Duration
}

type ConsulHealthChecker interface {
	WatchNodeService(
		nodename string,
		serviceID string,
		resultCh chan<- health.Result,
		errCh chan<- error,
		quitCh <-chan struct{},
	)
	Service(serviceID string) (map[string]health.Result, error)
}

type consulHealth interface {
	Node(string, *api.QueryOptions) ([]*api.HealthCheck, *api.QueryMeta, error)
}

func NewConsulHealthChecker(opts kp.Options) ConsulHealthChecker {
	client := kp.NewConsulClient(opts)
	return consulHealthChecker{
		client:      client,
		health:      client.Health(),
		consulStore: kp.NewConsulStore(opts),
		WaitTime:    1 * time.Minute,
	}
}

func (c consulHealthChecker) WatchNodeService(
	nodename string,
	serviceID string,
	resultCh chan<- health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	defer close(resultCh)

	var curIndex uint64 = 0

	for {
		select {
		case <-quitCh:
			return
		case <-time.After(1 * time.Second):
			var err error
			var checks []*api.HealthCheck
			checks, curIndex, err = c.fetchNodeHealth(nodename, curIndex)
			if err != nil {
				errCh <- err
			}
			catalogResults := make(health.ResultList, 0)
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
			kvCheck, err := c.consulStore.GetHealth(serviceID, nodename)
			pickHealthResult(catalogResults, kvCheck, err, resultCh, errCh)
		}
	}
}

// if there are no health results available will place error in errCh then return
func pickHealthResult(
	catalogResults health.ResultList,
	kvCheck kp.WatchResult,
	kvCheckError error,
	resultCh chan<- health.Result,
	errCh chan<- error,
) {
	if kvCheckError != nil {
		// if there are neither kv nor catalog results
		if len(catalogResults) == 0 {
			errCh <- kvCheckError
			return
		} else {
			// there are no kv results but there are catalog results
			catalogCheckResult := *catalogResults.MinValue()
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
			catalogCheckResult := *catalogResults.MinValue()
			best := health.MaxResult(kvCheckResult, catalogCheckResult)
			resultCh <- best
		}
	}
}

func (c consulHealthChecker) fetchNodeHealth(
	nodename string,
	curIndex uint64,
) ([]*api.HealthCheck, uint64, error) {
	checks, meta, err := c.health.Node(nodename, &api.QueryOptions{
		WaitIndex: curIndex,
		WaitTime:  c.WaitTime,
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
func (c consulHealthChecker) Service(serviceID string) (map[string]health.Result, error) {
	catalogEntries, _, err := c.client.Health().Service(serviceID, "", false, nil)
	if err != nil {
		return nil, util.Errorf("/health/service failed for %q: %s", serviceID, err)
	}
	// return map[nodenames (string)] to kp.WatchResult
	// get health of all instances of a service with 1 query
	kvEntries, err := c.consulStore.GetServiceHealth(serviceID)
	if err != nil {
		return nil, util.Errorf("/health/service failed for %q: %s", serviceID, err)
	}

	return selectResult(catalogEntries, kvEntries), nil
}

func selectResult(
	catalogEntries []*api.ServiceEntry,
	kvEntries map[string]kp.WatchResult,
) map[string]health.Result {
	ret := make(map[string]health.Result)

	for _, entry := range catalogEntries {
		res := make(health.ResultList, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			res = append(res, consulCheckToResult(*check))
		}
		val := res.MinValue()
		if val != nil {
			ret[entry.Node.Node] = *val
		}
	}

	for _, kvEntry := range kvEntries {
		res := consulWatchToResult(kvEntry)
		// if kvEntry already exists for this service take the best of kv store and catalog
		if _, ok := ret[kvEntry.Node]; ok {
			val := health.MaxResult(res, ret[kvEntry.Node])
			ret[kvEntry.Node] = val
		} else {
			ret[kvEntry.Node] = res
		}
	}

	return ret
}

func consulCheckToResult(c api.HealthCheck) health.Result {
	return health.Result{
		ID:      c.CheckID,
		Node:    c.Node,
		Service: c.ServiceID,
		Status:  health.ToHealthState(c.Status),
		Output:  c.Output,
	}
}

func consulWatchToResult(w kp.WatchResult) health.Result {
	return health.Result{
		ID:      w.Id,
		Node:    w.Node,
		Service: w.Service,
		Status:  health.ToHealthState(w.Status),
		Output:  w.Output,
	}
}
