package checker

import (
	"encoding/json"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
)

type ConsulHealthChecker interface {
	WatchNodeService(
		nodename string,
		serviceID string,
		resultCh chan<- health.Result,
		errCh chan<- error,
		quitCh <-chan struct{},
	)
	WatchService(
		serviceID string,
		resultCh chan<- map[string]health.Result,
		errCh chan<- error,
		quitCh <-chan struct{})
	WatchHealth(
		resultCh chan []*health.Result,
		errCh chan<- error,
		quitCh <-chan struct{})
	Service(serviceID string) (map[string]health.Result, error)
}

// Subset of kp.Store
type healthStore interface {
	GetHealth(service, node string) (kp.WatchResult, error)
	GetServiceHealth(service string) (map[string]kp.WatchResult, error)
}

type healthKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

type consulHealthChecker struct {
	client      *api.Client
	kv          healthKV
	consulStore healthStore
}
type consulHealth interface {
	Node(string, *api.QueryOptions) ([]*api.HealthCheck, *api.QueryMeta, error)
}

func NewConsulHealthChecker(client *api.Client) ConsulHealthChecker {
	return consulHealthChecker{
		client:      client,
		kv:          client.KV(),
		consulStore: kp.NewConsulStore(client),
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

// Watch the health tree and write the whole subtree on the chan passed by caller
// the result channel argument _must be buffered_
func (c consulHealthChecker) WatchHealth(
	resultCh chan []*health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	defer close(resultCh)

	res := make(chan api.KVPairs)
	defer close(res)

	quitWatch := make(<-chan struct{})

	go consulutil.WatchPrefix("health/", c.kv, res, quitWatch, errCh)

	var results api.KVPairs
	for {
		select {
		case <-quitCh:
			return
		case results = <-res:
			healthResults, err := kvpsToResult(results)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			} else {
				select {
				case <-resultCh: // we drain this channel prior to attempting a write so that the freshest value is always available
				case resultCh <- healthResults:
				default:
				}
			}
		}
	}
}

func (c consulHealthChecker) WatchService(
	serviceID string,
	resultCh chan<- map[string]health.Result,
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
			results, queryMeta, err := c.client.KV().List(kp.HealthPath(serviceID, ""), &api.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				errCh <- consulutil.NewKVError("list", kp.HealthPath(serviceID, ""), err)
			} else {
				curIndex = queryMeta.LastIndex
				out := make(map[string]health.Result)
				for _, result := range results {
					var next kp.WatchResult
					err = json.Unmarshal(result.Value, &next)
					if err != nil {
						errCh <- err
					} else {
						out[next.Node] = consulWatchToResult(next)
					}
				}
				resultCh <- out
			}
		}
	}
}

// Service returns a map where values are individual results (keys are nodes)
func (c consulHealthChecker) Service(serviceID string) (map[string]health.Result, error) {
	// return map[nodenames (string)] to kp.WatchResult
	// get health of all instances of a service with 1 query
	kvEntries, err := c.consulStore.GetServiceHealth(serviceID)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]health.Result)
	for _, kvEntry := range kvEntries {
		ret[kvEntry.Node] = consulWatchToResult(kvEntry)
	}

	return ret, nil
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
