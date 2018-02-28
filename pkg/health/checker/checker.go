package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/square/p2/pkg/health"
	hclient "github.com/square/p2/pkg/health/client"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	rcfields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type HealthChecker interface {
	WatchPodOnNode(
		nodename types.NodeName,
		podID types.PodID,
		quitCh <-chan struct{},
	) (chan health.Result, chan error)
	WatchService(
		ctx context.Context,
		serviceID string,
		resultCh chan<- map[types.NodeName]health.Result,
		errCh chan<- error,
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

type ShadowTrafficHealthChecker interface {
	WatchService(
		ctx context.Context,
		serviceID string,
		resultCh chan<- map[types.NodeName]health.Result,
		errCh chan<- error,
		watchDelay time.Duration,
		useHealthService bool,
		useOnlyHealthService bool,
		status manifest.StatusStanza,
	)
	Service(
		serviceID string,
		useHealthService bool,
		status manifest.StatusStanza,
	) (map[types.NodeName]health.Result, error)
}

type HealthClient interface {
	HealthCheckEndpoints(ctx context.Context, req *hclient.HealthEndpointsRequest) (map[string]health.HealthState, error)
	HealthMonitor(ctx context.Context, req *hclient.HealthRequest, respCh chan *hclient.HealthResponse) error
	HealthMonitorEndpoints(ctx context.Context, req *hclient.HealthEndpointsRequest, respCh chan *hclient.HealthResponse) error
}

type ResourceClient interface {
	GetRCIDsForPod(pod types.PodID) ([]rcfields.ID, error)
}

type healthKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

// Subset of consul.Store
type healthStore interface {
	GetHealth(service string, node types.NodeName) (consul.WatchResult, error)
	GetServiceHealth(service string) (map[string]consul.WatchResult, error)
}

type ReplicationControllerStore interface {
	Get(id rcfields.ID) (rcfields.RC, error)
}

type LabelReader interface {
	GetMatches(klabels.Selector, labels.Type) ([]labels.Labeled, error)
}

type shadowTrafficHealthChecker struct {
	healthClient     HealthClient
	resourceClient   ResourceClient
	consulClient     consulutil.ConsulClient
	kv               healthKV
	consulStore      healthStore
	rcStore          ReplicationControllerStore
	labelReader      LabelReader
	useHealthService bool
}

func NewShadowTrafficHealthChecker(hClient HealthClient, resourceClient ResourceClient, cClient consulutil.ConsulClient, rcStore ReplicationControllerStore, labelReader LabelReader, useHealthService bool) ShadowTrafficHealthChecker {
	return shadowTrafficHealthChecker{
		healthClient:     hClient,
		resourceClient:   resourceClient,
		consulClient:     cClient,
		kv:               cClient.KV(),
		consulStore:      consul.NewConsulStore(cClient),
		rcStore:          rcStore,
		labelReader:      labelReader,
		useHealthService: useHealthService,
	}
}

type healthChecker struct {
	consulClient consulutil.ConsulClient
	kv           healthKV
	consulStore  healthStore
}

func NewHealthChecker(cClient consulutil.ConsulClient) HealthChecker {
	return healthChecker{
		consulClient: cClient,
		kv:           cClient.KV(),
		consulStore:  consul.NewConsulStore(cClient),
	}
}

func (h healthChecker) WatchPodOnNode(
	nodename types.NodeName,
	podID types.PodID,
	quitCh <-chan struct{},
) (chan health.Result, chan error) {
	resultCh := make(chan health.Result)
	errCh := make(chan error)

	key := consul.HealthPath(podID.String(), nodename)

	wsOut := make(chan *api.KVPair) // closed by WatchSingle
	wsQuit := make(chan struct{})

	go consulutil.WatchSingle(key, h.consulClient.KV(), wsOut, wsQuit, errCh)

	go func() {
		defer close(wsQuit)
		defer close(resultCh)
		defer close(errCh)
		for {
			select {
			case <-quitCh:
				return
			case kvPair := <-wsOut:
				if kvPair == nil {
					unknownRes := health.Result{
						ID:      podID,
						Node:    nodename,
						Service: podID.String(),
						Status:  health.Unknown,
					}
					select {
					case resultCh <- unknownRes:
					case <-quitCh:
						return
					}
				} else {
					res, err := kvpToResult(*kvPair)
					if err != nil {
						select {
						case errCh <- err:
						case <-quitCh:
							return
						}
					} else {
						select {
						case resultCh <- *res:
						case <-quitCh:
							return
						}
					}
				}
			}
		}
	}()

	return resultCh, errCh
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
			// First we drain the resultCh of any stale health results
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

func (h shadowTrafficHealthChecker) getStatusEndpoints(
	serviceID string,
	status manifest.StatusStanza,
) ([]string, error) {
	rcIDs, err := h.resourceClient.GetRCIDsForPod(types.PodID(serviceID))
	if err != nil {
		return nil, err
	}
	var statusEndpoints []string
	for _, rcID := range rcIDs {
		rc, err := h.rcStore.Get(rcID)
		if err != nil {
			return nil, err
		}
		// get hostnames for rc
		labeled, err := h.labelReader.GetMatches(rc.NodeSelector, labels.NODE)
		if err != nil {
			return nil, err
		}
		scheme := "https"
		if status.HTTP {
			scheme = "http"
		}
		// create status endpoints
		for _, node := range labeled {
			statusEndpoints = append(statusEndpoints, fmt.Sprintf("%s://%s:%d%s", scheme, node.ID, status.Port, status.GetPath()))
		}
	}
	return statusEndpoints, nil
}

// Watch the health tree and write the whole subtree on the chan passed by caller
// the result channel argument _must be buffered_
// Any errors are passed, best effort, over errCh
func (h healthChecker) WatchHealth(
	resultCh chan []*health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
	jitterWindow time.Duration,
) {
	// closed by watchPrefix when we close quitWatch
	inCh := make(chan api.KVPairs)
	watchErrCh := make(chan error)
	go consulutil.WatchPrefix("health/", h.kv, inCh, quitCh, watchErrCh, 1*time.Second, jitterWindow)
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

func watchConsulHealth(
	ctx context.Context,
	serviceID string,
	kv healthKV,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	watchDelay time.Duration,
) {
	if watchDelay < time.Second {
		watchDelay = time.Second
	}

	timer := time.NewTimer(0)

	var curIndex uint64 = 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(watchDelay)
			results, queryMeta, err := kv.List(consul.HealthPath(serviceID, "/"), &api.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				select {
				case <-ctx.Done():
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
						case <-ctx.Done():
							return
						case errCh <- err:
						}
					} else {
						out[next.Node] = consulWatchToResult(next)
					}
				}
				select {
				case <-ctx.Done():
					return
				case resultCh <- out:
				}
			}
		}
	}
}

func (h healthChecker) WatchService(
	ctx context.Context,
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	watchDelay time.Duration,
) {
	defer close(resultCh)
	watchConsulHealth(ctx, serviceID, h.kv, resultCh, errCh, watchDelay)
}

func statusURLToNodeName(s string) (types.NodeName, error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", util.Errorf("error parsing url '%s'", s)
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", util.Errorf("error parsing host:port '%s'", u.Host)
	}
	return types.NodeName(host), nil
}

func (h shadowTrafficHealthChecker) WatchService(
	ctx context.Context,
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	watchDelay time.Duration,
	useHealthService bool,
	useOnlyHealthService bool,
	status manifest.StatusStanza,
) {
	defer close(resultCh)

	if h.useHealthService || useHealthService || useOnlyHealthService {
		// get status endpoints for service
		statusEndpoints, err := h.getStatusEndpoints(serviceID, status)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			}
		}
		respChan := make(chan *hclient.HealthResponse, 1)
		defer close(respChan)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case resp := <-respChan:
					out := make(map[types.NodeName]health.Result)
					endpoint := resp.HealthRequest.Url
					nodeID, err := statusURLToNodeName(endpoint)
					if err != nil {
						select {
						case <-ctx.Done():
							return
						case errCh <- err:
						}
					}
					out[nodeID] = health.Result{
						ID:      types.PodID(serviceID),
						Node:    nodeID,
						Service: serviceID,
						Status:  resp.Health,
					}
					select {
					case <-ctx.Done():
						return
					case resultCh <- out:
					}
				}
			}
		}()

		go func() {
			protocol := "HTTPS"
			if status.HTTP {
				protocol = "HTTP"
			}
			healthReq := &hclient.HealthEndpointsRequest{
				Endpoints: statusEndpoints,
				Protocol:  protocol,
			}
			for {
				err := h.healthClient.HealthMonitorEndpoints(ctx, healthReq, respChan)
				if err != nil {
					select {
					case <-ctx.Done():
						return
					case errCh <- err:
					}
				}
				// retry the request after 1 second
				timer := time.NewTimer(time.Second)
				<-timer.C
			}
		}()
	}

	if !useOnlyHealthService {
		watchConsulHealth(ctx, serviceID, h.kv, resultCh, errCh, watchDelay)
	}
}

func (h healthChecker) Service(serviceID string) (map[types.NodeName]health.Result, error) {
	// return map[nodenames (string)] to consul.WatchResult
	// get health of all instances of a service with 1 query
	kvEntries, err := h.consulStore.GetServiceHealth(serviceID)
	if err != nil {
		return nil, err
	}
	ret := make(map[types.NodeName]health.Result)
	for _, kvEntry := range kvEntries {
		ret[kvEntry.Node] = consulWatchToResult(kvEntry)
	}

	return ret, nil
}

// Service returns a map where values are individual results (keys are nodes)
func (h shadowTrafficHealthChecker) Service(
	serviceID string,
	useHealthService bool,
	status manifest.StatusStanza,
) (map[types.NodeName]health.Result, error) {
	if !h.useHealthService && !useHealthService {
		// return map[nodenames (string)] to consul.WatchResult
		// get health of all instances of a service with 1 query
		kvEntries, err := h.consulStore.GetServiceHealth(serviceID)
		if err != nil {
			return nil, err
		}
		ret := make(map[types.NodeName]health.Result)
		for _, kvEntry := range kvEntries {
			ret[kvEntry.Node] = consulWatchToResult(kvEntry)
		}

		return ret, nil
	}

	// health service
	// get status endpoints for service
	statusEndpoints, err := h.getStatusEndpoints(serviceID, status)
	if err != nil {
		return nil, err
	}
	protocol := "HTTPS"
	if status.HTTP {
		protocol = "HTTP"
	}
	healthReq := &hclient.HealthEndpointsRequest{
		Endpoints: statusEndpoints,
		Protocol:  protocol,
	}
	urlToHealthStates, err := h.healthClient.HealthCheckEndpoints(context.Background(), healthReq)
	if err != nil {
		return nil, err
	}
	ret := make(map[types.NodeName]health.Result)
	for url, healthState := range urlToHealthStates {
		nodeID, err := statusURLToNodeName(url)
		if err != nil {
			return nil, err
		}
		ret[nodeID] = health.Result{
			ID:      types.PodID(serviceID),
			Node:    nodeID,
			Service: serviceID,
			Status:  healthState,
		}
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

func kvpToResult(kv api.KVPair) (*health.Result, error) {
	res := &health.Result{}
	err := json.Unmarshal(kv.Value, &res)
	if err != nil {
		return nil, util.Errorf("Could not unmarshal health at %s: %v", kv.Key, err)
	}
	return res, nil
}

// Maps a list of KV Pairs into a slice of health.Results
// Halts and returns upon encountering an error
func kvpsToResult(kvs api.KVPairs) ([]*health.Result, error) {
	result := make([]*health.Result, len(kvs))
	for i, kv := range kvs {
		res, err := kvpToResult(*kv)
		if err != nil {
			return nil, err
		}
		result[i] = res
	}

	return result, nil
}
