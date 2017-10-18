package checker

import (
	"encoding/json"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

type ConsulHealthChecker interface {
	WatchNodeService(
		nodename types.NodeName,
		serviceID string,
		resultCh chan<- health.Result,
		errCh chan<- error,
		quitCh <-chan struct{},
	)
	WatchService(
		serviceID string,
		resultCh chan<- map[types.NodeName]health.Result,
		errCh chan<- error,
		quitCh <-chan struct{},
		watchDelay time.Duration,
	)
	WatchHealth(
		resultCh chan []*health.Result,
		errCh chan<- error,
		quitCh <-chan struct{},
		jitterWindow time.Duration,
	)
	Service(serviceID string) (map[types.NodeName]health.Result, error)
}

// Subset of consul.Store
type healthStore interface {
	GetHealth(service string, node types.NodeName) (consul.WatchResult, error)
	GetServiceHealth(service string) (map[string]consul.WatchResult, error)
}

type healthKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

type consulHealthChecker struct {
	client      consulutil.ConsulClient
	kv          healthKV
	consulStore healthStore
}
type consulHealth interface {
	Node(types.NodeName, *api.QueryOptions) ([]*api.HealthCheck, *api.QueryMeta, error)
}

func NewConsulHealthChecker(client consulutil.ConsulClient) ConsulHealthChecker {
	return consulHealthChecker{
		client:      client,
		kv:          client.KV(),
		consulStore: consul.NewConsulStore(client),
	}
}

func (c consulHealthChecker) WatchNodeService(
	nodename types.NodeName,
	serviceID string,
	resultCh chan<- health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	defer close(resultCh)

	for {
		select {
		case <-quitCh:
			return
		case <-time.After(1 * time.Second):
			kvCheck, err := c.consulStore.GetHealth(serviceID, nodename)
			if err != nil {
				errCh <- err
			} else {
				resultCh <- consulWatchToResult(kvCheck)
			}
		}
	}
}

// publishLatestHealth is not thread safe - do not start more than one of these per resultCH
func publishLatestHealth(inCh <-chan api.KVPairs, quitCh <-chan struct{}, resultCh chan []*health.Result) chan error {
	errCh := make(chan error)

	go func() {
		var listed api.KVPairs
		var ok bool
		var err error

		for {
			// We don't have a value, fetch a fresh one or skip
			select {
			case listed, ok = <-inCh:
				if !ok {
					// channel closed
					return
				}
			case <-quitCh:
				return
			}

			results := make([]*health.Result, 0, len(listed)) // allocate a new return slice for each watch
			results, err = kvpsToResult(listed)
			if err != nil {
				select {
				case errCh <- err:
					// The most recent update is an error.
					// We go back to the start in this case
					continue
				case <-quitCh:
					return
				}
			}

			// here, we prepare to write the value.
			// First we drain the resultChan of any stale health results
			select {
			case _, ok = <-resultCh:
				if !ok {
					return
				}
			default:
			}
			// Now we check the quit chan and try to write to our resultCh
			select {
			case <-quitCh:
				return
			case resultCh <- results:
			}
		}
	}()

	return errCh
}

// Watch the health tree and write the whole subtree on the chan passed by caller
// the result channel argument _must be buffered_
// Any errors are passed, best effort, over errCh
func (c consulHealthChecker) WatchHealth(
	resultCh chan []*health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
	jitterWindow time.Duration,
) {
	// closed by watchPrefix when we close quitWatch
	inCh := make(chan api.KVPairs)
	watchErrCh := make(chan error)
	go consulutil.WatchPrefix("health/", c.kv, inCh, quitCh, watchErrCh, 1*time.Second, jitterWindow)
	publishErrCh := publishLatestHealth(inCh, quitCh, resultCh)

	for {
		select {
		case <-quitCh:
			return
		case err := <-watchErrCh:
			select {
			case errCh <- err:
			case <-quitCh:
				return
			default:
			}
		case err := <-publishErrCh:
			select {
			case errCh <- err:
			case <-quitCh:
				return
			default:
			}
		}
	}
}

func (c consulHealthChecker) WatchService(
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
	watchDelay time.Duration,
) {
	defer close(resultCh)
	var curIndex uint64 = 0

	if watchDelay < time.Second {
		watchDelay = time.Second
	}

	timer := time.NewTimer(0)
	for {
		select {
		case <-quitCh:
			return
		case <-timer.C:
			timer.Reset(watchDelay)
			results, queryMeta, err := c.client.KV().List(consul.HealthPath(serviceID, "/"), &api.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				select {
				case <-quitCh:
					return
				case errCh <- consulutil.NewKVError("list", consul.HealthPath(serviceID, "/"), err):
				}
			} else {
				curIndex = queryMeta.LastIndex
				out := make(map[types.NodeName]health.Result)
				for _, result := range results {
					var next consul.WatchResult
					err = json.Unmarshal(result.Value, &next)
					if err != nil {
						select {
						case <-quitCh:
							return
						case errCh <- err:
						}
					} else {
						out[next.Node] = consulWatchToResult(next)
					}
				}
				select {
				case <-quitCh:
					return
				case resultCh <- out:
				}
			}
		}
	}
}

// Service returns a map where values are individual results (keys are nodes)
func (c consulHealthChecker) Service(serviceID string) (map[types.NodeName]health.Result, error) {
	// return map[nodenames (string)] to consul.WatchResult
	// get health of all instances of a service with 1 query
	kvEntries, err := c.consulStore.GetServiceHealth(serviceID)
	if err != nil {
		return nil, err
	}
	ret := make(map[types.NodeName]health.Result)
	for _, kvEntry := range kvEntries {
		ret[kvEntry.Node] = consulWatchToResult(kvEntry)
	}

	return ret, nil
}

func consulWatchToResult(w consul.WatchResult) health.Result {
	return health.Result{
		ID:      w.Id,
		Node:    w.Node,
		Service: w.Service,
		Status:  health.ToHealthState(w.Status),
	}
}

// Maps a list of KV Pairs into a slice of health.Results
// Halts and returns upon encountering an error
func kvpsToResult(kvs api.KVPairs) ([]*health.Result, error) {
	result := make([]*health.Result, len(kvs))
	var err error
	for i, kv := range kvs {
		tmp := &health.Result{}
		err = json.Unmarshal(kv.Value, &tmp)
		if err != nil {
			return nil, util.Errorf("Could not unmarshal health at %s: %v", kv.Key, err)
		}
		result[i] = tmp
	}

	return result, nil
}
