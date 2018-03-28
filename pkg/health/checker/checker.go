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
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

type HealthChecker interface {
	WatchPodOnNode(
		ctx context.Context,
		nodename types.NodeName,
		podID types.PodID,
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
	WatchPodOnNode(
		ctx context.Context,
		nodeID types.NodeName,
		podID types.PodID,
		useHealthService bool,
		useOnlyHealthService bool,
		status manifest.StatusStanza,
	) (chan health.Result, chan error)
	WatchService(
		ctx context.Context,
		serviceID string,
		nodeIDs []types.NodeName,
		nodeIDsCh <-chan []types.NodeName,
		resultCh chan<- map[types.NodeName]health.Result,
		errCh chan<- error,
		watchDelay time.Duration,
		useHealthService bool,
		useOnlyHealthService bool,
		status manifest.StatusStanza,
	)
	Service(
		serviceID string,
		nodeIDs []types.NodeName,
		useHealthService bool,
		status manifest.StatusStanza,
	) (map[types.NodeName]health.Result, error)
}

type HealthClient interface {
	HealthCheckEndpoints(ctx context.Context, req *hclient.HealthEndpointsRequest) (map[string]health.HealthState, error)
	HealthMonitor(ctx context.Context, req *hclient.HealthRequest, respCh chan *hclient.HealthResponse) error
}

type healthKV interface {
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

// Subset of consul.Store
type healthStore interface {
	GetHealth(service string, node types.NodeName) (consul.WatchResult, error)
	GetServiceHealth(service string) (map[string]consul.WatchResult, error)
}

type shadowTrafficHealthChecker struct {
	healthClient         HealthClient
	consulClient         consulutil.ConsulClient
	kv                   healthKV
	consulStore          healthStore
	useHealthService     bool
	useOnlyHealthService bool
}

func NewShadowTrafficHealthChecker(
	hClient HealthClient,
	cClient consulutil.ConsulClient,
	useHealthService bool,
	useOnlyHealthService bool,
) ShadowTrafficHealthChecker {
	return shadowTrafficHealthChecker{
		healthClient:         hClient,
		consulClient:         cClient,
		kv:                   cClient.KV(),
		consulStore:          consul.NewConsulStore(cClient),
		useHealthService:     useHealthService,
		useOnlyHealthService: useOnlyHealthService,
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

func (h shadowTrafficHealthChecker) WatchPodOnNode(
	ctx context.Context,
	nodeID types.NodeName,
	podID types.PodID,
	useHealthService bool,
	useOnlyHealthService bool,
	status manifest.StatusStanza,
) (chan health.Result, chan error) {
	resultCh := make(chan health.Result)
	errCh := make(chan error)
	if !useOnlyHealthService && !h.useOnlyHealthService {
		resultCh, errCh = consulWatchPodOnNode(ctx, nodeID, podID, h.kv)
	}

	if h.useOnlyHealthService || useOnlyHealthService || h.useHealthService || useHealthService {
		endpoint := nodeIDToStatusEndpoint(nodeID, status)
		protocol := "HTTPS"
		if status.HTTP {
			protocol = "HTTP"
		}
		healthReq := &hclient.HealthRequest{
			Url:      endpoint,
			Protocol: protocol,
		}
		respChan := make(chan *hclient.HealthResponse, 1)
		healthResult := health.Result{
			ID:      podID,
			Node:    nodeID,
			Service: podID.String(),
			Status:  health.Unknown,
		}
		oldResultDelay := time.Second * 2
		oldResultTimer := time.NewTimer(oldResultDelay)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case resp := <-respChan:
					endpoint := resp.HealthRequest.Url
					nodeID, err := statusURLToNodeName(endpoint)
					if err != nil {
						select {
						case <-ctx.Done():
							return
						case errCh <- err:
						}
						continue
					}
					healthResult = health.Result{
						ID:      podID,
						Node:    nodeID,
						Service: podID.String(),
						Status:  resp.Health,
					}
					select {
					case <-ctx.Done():
						return
					case resultCh <- healthResult:
					}
				case <-oldResultTimer.C:
					oldResultTimer.Reset(oldResultDelay)
					select {
					case <-ctx.Done():
						return
					case resultCh <- healthResult:
					}
				}
			}
		}()

		go func() {
			for {
				// this is a blocking call, messages will be received on the respChan in the above goroutine
				err := h.healthClient.HealthMonitor(ctx, healthReq, respChan)
				if err != nil {
					select {
					case <-ctx.Done():
						return
					case errCh <- err:
					}
				}
				// retry the request after 1 second
				<-time.NewTimer(time.Second).C
			}
		}()
	}
	return resultCh, errCh
}

func (h healthChecker) WatchPodOnNode(
	ctx context.Context,
	nodename types.NodeName,
	podID types.PodID,
) (chan health.Result, chan error) {
	return consulWatchPodOnNode(ctx, nodename, podID, h.kv)
}

func consulWatchPodOnNode(
	ctx context.Context,
	nodeID types.NodeName,
	podID types.PodID,
	kv healthKV,
) (chan health.Result, chan error) {
	resultCh := make(chan health.Result)
	errCh := make(chan error)

	key := consul.HealthPath(podID.String(), nodeID)

	wsOut := make(chan *api.KVPair) // closed by WatchSingle
	wsQuit := make(chan struct{})

	go consulutil.WatchSingle(key, kv, wsOut, wsQuit, errCh)

	go func() {
		defer close(wsQuit)
		for {
			select {
			case <-ctx.Done():
				return
			case kvPair := <-wsOut:
				if kvPair == nil {
					unknownRes := health.Result{
						ID:      podID,
						Node:    nodeID,
						Service: podID.String(),
						Status:  health.Unknown,
					}
					select {
					case resultCh <- unknownRes:
					case <-ctx.Done():
						return
					}
				} else {
					res, err := kvpToResult(*kvPair)
					if err != nil {
						select {
						case errCh <- err:
						case <-ctx.Done():
							return
						}
					} else {
						select {
						case resultCh <- *res:
						case <-ctx.Done():
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

func nodeIDToStatusEndpoint(nodeID types.NodeName, status manifest.StatusStanza) string {
	scheme := "https"
	if status.HTTP {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s:%d%s", scheme, nodeID, status.Port, status.GetPath())
}

func nodeIDsToStatusEndpoints(nodeIDs []types.NodeName, status manifest.StatusStanza) []string {
	statusEndpoints := make([]string, len(nodeIDs))

	for i, nodeID := range nodeIDs {
		statusEndpoints[i] = nodeIDToStatusEndpoint(nodeID, status)
	}
	return statusEndpoints
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
						continue
					}
					out[next.Node] = consulWatchToResult(next)
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

func healthResultsCopy(healthResults map[types.NodeName]health.Result) map[types.NodeName]health.Result {
	out := make(map[types.NodeName]health.Result)
	for nodeID, healthResult := range healthResults {
		out[nodeID] = healthResult
	}
	return out
}

func (h shadowTrafficHealthChecker) WatchService(
	ctx context.Context,
	serviceID string,
	nodeIDs []types.NodeName,
	nodeIDsCh <-chan []types.NodeName,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	watchDelay time.Duration,
	useHealthService bool,
	useOnlyHealthService bool,
	status manifest.StatusStanza,
) {
	alwaysHealthy := false
	// nodes are considered always healthy when status port is not set
	// when always healthy, WatchService ONLY sends the healthy status repeatedly on a timer set to delay
	if status.Port == 0 {
		alwaysHealthy = true
	}
	if h.useHealthService || useHealthService || useOnlyHealthService || h.useOnlyHealthService {
		respChan := make(chan *hclient.HealthResponse, len(nodeIDs))
		go func() {
			oldResultsDelay := time.Second * 5
			healthResults := make(map[types.NodeName]health.Result)
			if alwaysHealthy {
				// reduce the delay to send old results since
				// when the nodes are always healthy, these
				// are the only messages sent
				oldResultsDelay = time.Second * 2
				for _, nodeID := range nodeIDs {
					healthResults[nodeID] = health.Result{
						ID:      types.PodID(serviceID),
						Node:    nodeID,
						Service: serviceID,
						Status:  health.Passing,
					}
				}
			}
			timer := time.NewTimer(oldResultsDelay)
			for {
				select {
				case <-ctx.Done():
					return
				case resp := <-respChan:
					endpoint := resp.HealthRequest.Url
					nodeID, err := statusURLToNodeName(endpoint)
					if err != nil {
						select {
						case <-ctx.Done():
							return
						case errCh <- err:
						}
						continue
					}
					healthResults[nodeID] = health.Result{
						ID:      types.PodID(serviceID),
						Node:    nodeID,
						Service: serviceID,
						Status:  resp.Health,
					}
					// send a copy because does otherwise there's a race condition where the healthResults change before it's read
					// it doesn't matter for correctness, but the go test --race will complain
					resultsCopy := healthResultsCopy(healthResults)
					select {
					case <-ctx.Done():
						return
					case resultCh <- resultsCopy:
					}
				case nodeIDs = <-nodeIDsCh:
					if alwaysHealthy {
						for _, nodeID := range nodeIDs {
							healthResults[nodeID] = health.Result{
								ID:      types.PodID(serviceID),
								Node:    nodeID,
								Service: serviceID,
								Status:  health.Passing,
							}
						}
					}
				case <-timer.C:
					timer.Reset(oldResultsDelay)
					// send old result since health service monitors only sends updates when there is a change but rolling updates expects health status periodically
					resultsCopy := healthResultsCopy(healthResults)
					select {
					case <-ctx.Done():
						return
					case resultCh <- resultsCopy:
					}
				}
			}
		}()

		go func() {
			// app is considered always healthy when status port is not set
			// so don't create monitors to monitor status
			if alwaysHealthy {
				return
			}
			protocol := "HTTPS"
			if status.HTTP {
				protocol = "HTTP"
			}

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				statusEndpoints := nodeIDsToStatusEndpoints(nodeIDs, status)
				monitorCtx, monitorCancel := context.WithCancel(ctx)
				defer monitorCancel()
				for _, endpoint := range statusEndpoints {
					healthReq := &hclient.HealthRequest{
						Url:      endpoint,
						Protocol: protocol,
					}
					go func() {
						for {
							// this is a blocking call, messages will be received on the respChan in the above goroutine
							err := h.healthClient.HealthMonitor(monitorCtx, healthReq, respChan)
							if err != nil {
								select {
								case <-ctx.Done():
									return
								case errCh <- err:
								}
							}
							// retry the request after 1 second
							<-time.NewTimer(time.Second).C
						}
					}()
				}

				// refresh the monitors since the nodes can change
				<-time.NewTimer(time.Minute * 10).C
				monitorCancel()
			}
		}()
	}

	if !useOnlyHealthService && !h.useOnlyHealthService {
		watchConsulHealth(ctx, serviceID, h.kv, resultCh, errCh, watchDelay)
	} else {
		for {
			select {
			case <-ctx.Done():
				return
			}
		}
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
	nodeIDs []types.NodeName,
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

	statusEndpoints := nodeIDsToStatusEndpoints(nodeIDs, status)
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
