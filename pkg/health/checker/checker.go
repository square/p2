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
					// The most recent update is in error.
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
			default:
			}
			resultCh <- results
		}
	}()

	return errCh
}

// Watch the health tree and write the whole subtree on the chan passed by caller
// the result channel argument _must be buffered_
func (c consulHealthChecker) WatchHealth(
	resultCh chan []*health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	defer close(resultCh)

	// closed by watchPrefix when we close quitWatch
	inCh := make(chan api.KVPairs)
	go consulutil.WatchPrefix("health/", c.kv, inCh, quitCh, errCh)
	errCh = publishLatestHealth(inCh, quitCh, resultCh)

	var open bool
	for {
		select {
		case <-quitCh:
			return
		case _, open = <-inCh:
			if !open {
				return
			}
		case _, open = <-resultCh:
			if !open {
				return
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
