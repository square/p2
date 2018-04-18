package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"reflect"
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

type HealthCheckerFactory interface {
	New() HealthChecker
}

type HealthChecker interface {
	WatchPodOnNode(
		ctx context.Context,
		nodeID types.NodeName,
		podID types.PodID,
		status manifest.StatusStanza,
	) (chan health.Result, chan error)
	WatchService(
		ctx context.Context,
		serviceID string,
		resultCh chan<- map[types.NodeName]health.Result,
		errCh chan<- error,
		watchDelay time.Duration,
		status manifest.StatusStanza,
	)
	Service(
		serviceID string,
		status manifest.StatusStanza,
	) (map[types.NodeName]health.Result, error)
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

type consulHealthChecker struct {
	consulClient consulutil.ConsulClient
	kv           healthKV
	consulStore  healthStore
}

type HealthClient interface {
	HealthCheckEndpoints(ctx context.Context, req *hclient.HealthEndpointsRequest) (map[string]health.HealthState, error)
	HealthMonitor(ctx context.Context, req *hclient.HealthRequest, respCh chan *hclient.HealthResponse) error
}

type healthChecker struct {
	healthClient HealthClient
	serviceNodes func(serviceID string) ([]types.NodeName, error)
}

func NewConsulHealthChecker(cClient consulutil.ConsulClient) HealthChecker {
	return consulHealthChecker{
		consulClient: cClient,
		kv:           cClient.KV(),
		consulStore:  consul.NewConsulStore(cClient),
	}
}

func (c consulHealthChecker) WatchPodOnNode(
	ctx context.Context,
	nodeID types.NodeName,
	podID types.PodID,
	_ manifest.StatusStanza,
) (chan health.Result, chan error) {
	resultCh := make(chan health.Result)
	errCh := make(chan error)

	key := consul.HealthPath(podID.String(), nodeID)

	wsOut := make(chan *api.KVPair) // closed by WatchSingle
	wsQuit := make(chan struct{})

	go consulutil.WatchSingle(key, c.kv, wsOut, wsQuit, errCh)

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

func (c consulHealthChecker) WatchService(
	ctx context.Context,
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	watchDelay time.Duration,
	_ manifest.StatusStanza,
) {
	defer close(resultCh)
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
			results, queryMeta, err := c.kv.List(consul.HealthPath(serviceID, "/"), &api.QueryOptions{
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

func (c consulHealthChecker) Service(
	serviceID string,
	_ manifest.StatusStanza,
) (map[types.NodeName]health.Result, error) {
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

func NewHealthChecker(
	hClient HealthClient,
	// this function needs to return a unique set of nodeIDs
	serviceNodes func(serviceID string) ([]types.NodeName, error),
) HealthChecker {
	return healthChecker{
		healthClient: hClient,
		serviceNodes: serviceNodes,
	}
}

func (h healthChecker) WatchPodOnNode(
	ctx context.Context,
	nodeID types.NodeName,
	podID types.PodID,
	status manifest.StatusStanza,
) (chan health.Result, chan error) {
	resultCh := make(chan health.Result)
	errCh := make(chan error)

	endpoint := nodeIDToStatusEndpoint(nodeID, status)
	protocol := "HTTPS"
	if status.HTTP {
		protocol = "HTTP"
	}
	healthReq := &hclient.HealthRequest{
		Url:      endpoint,
		Protocol: protocol,
	}
	respCh := make(chan *hclient.HealthResponse, 1)
	healthResult := health.Result{
		ID:      podID,
		Node:    nodeID,
		Service: podID.String(),
		Status:  health.Unknown,
	}
	alwaysHealthyTimer := time.NewTimer(time.Second * 2)
	alwaysHealthy := status.Port == 0
	if alwaysHealthy {
		healthResult.Status = health.Passing
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case resp := <-respCh:
				// timer.Reset(time.Second * 2)
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
			case <-alwaysHealthyTimer.C:
				if !alwaysHealthy {
					continue
				}
				alwaysHealthyTimer.Reset(time.Second * 2)
				select {
				case <-ctx.Done():
					return
				case resultCh <- healthResult:
				}
			}
		}
	}()

	go func() {
		if alwaysHealthy {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			// this is a blocking call, messages will be received on the respCh in the above goroutine
			err := h.healthClient.HealthMonitor(ctx, healthReq, respCh)
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
	return resultCh, errCh
}

func (h healthChecker) WatchService(
	ctx context.Context,
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	_ time.Duration,
	status manifest.StatusStanza,
) {
	nodeIDs := []types.NodeName{}
	var err error
	for {
		nodeIDs, err = h.serviceNodes(serviceID)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
				// try again
				<-time.NewTimer(time.Second * 5).C
				continue
			}
		}
		break
	}
	// monitored the nodes for a service in case they change (i.e. node transfer)
	nodeIDsCh := make(chan []types.NodeName, 1)
	go h.serviceNodesMonitor(ctx, nodeIDsCh, errCh, serviceID, nodeIDs)

	// nodes are considered always healthy when status port is not set
	// when always healthy, WatchService ONLY sends the healthy status repeatedly on a timer set to delay
	alwaysHealthy := status.Port == 0
	monitorCtx, monitorCancel := context.WithCancel(ctx)
	defer monitorCancel()
	respCh := make(chan *hclient.HealthResponse)
	if !alwaysHealthy {
		go func() {
			statusEndpoints := nodeIDsToStatusEndpoints(nodeIDs, status)
			h.createHealthMonitors(monitorCtx, respCh, errCh, statusEndpoints, status)
		}()
	}

	healthResults := make(map[types.NodeName]health.Result)
	if alwaysHealthy {
		// reduce the delay to send old results since
		// when the nodes are always healthy, these
		// are the only messages sent
		for _, nodeID := range nodeIDs {
			healthResults[nodeID] = health.Result{
				ID:      types.PodID(serviceID),
				Node:    nodeID,
				Service: serviceID,
				Status:  health.Passing,
			}
		}
	}
	alwaysHealthyTimer := time.NewTimer(time.Second * 2)
	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-respCh:
			// the first health message from the health service is always unknown
			if resp.Health == health.Unknown {
				continue
			}
			endpoint := resp.HealthRequest.Url
			nodeID, err := statusURLToNodeName(endpoint)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case errCh <- err:
					continue
				}
			}
			healthResults[nodeID] = health.Result{
				ID:      types.PodID(serviceID),
				Node:    nodeID,
				Service: serviceID,
				Status:  resp.Health,
			}
			if len(healthResults) != len(nodeIDs) {
				continue
			}
			// send a copy because does otherwise there's a race condition where the health results could be read and written at the same time
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
				continue
			}
			monitorCancel()
			monitorCtx, monitorCancel = context.WithCancel(ctx)
			defer monitorCancel()
			go func() {
				statusEndpoints := nodeIDsToStatusEndpoints(nodeIDs, status)
				h.createHealthMonitors(monitorCtx, respCh, errCh, statusEndpoints, status)
			}()
		case <-alwaysHealthyTimer.C:
			alwaysHealthyTimer.Reset(time.Second * 2)
			select {
			case <-ctx.Done():
				return
			case resultCh <- healthResults:
			}
		}
	}

}

func (h healthChecker) Service(
	serviceID string,
	status manifest.StatusStanza,
) (map[types.NodeName]health.Result, error) {
	nodeIDs, err := h.serviceNodes(serviceID)
	if err != nil {
		return nil, err
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

// helper function for monitor nodeIDs of a service
// takes oldNodeIDs and only sends a message over the
// nodeIDsCh if there's a diff
func (h healthChecker) serviceNodesMonitor(
	ctx context.Context,
	nodeIDsCh chan []types.NodeName,
	errCh chan<- error,
	serviceID string,
	oldNodeIDs []types.NodeName,
) {
	tick := time.Tick(time.Minute * 5)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			newNodeIDs, err := h.serviceNodes(serviceID)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case errCh <- err:
					continue
				}
			}
			if !reflect.DeepEqual(oldNodeIDs, newNodeIDs) {
				oldNodeIDs = newNodeIDs
				select {
				case <-ctx.Done():
					return
				case nodeIDsCh <- newNodeIDs:
				}
			}
		}
	}
}

func (h healthChecker) createHealthMonitors(
	ctx context.Context,
	respCh chan *hclient.HealthResponse,
	errCh chan<- error,
	statusEndpoints []string,
	status manifest.StatusStanza,
) {
	protocol := "HTTPS"
	if status.HTTP {
		protocol = "HTTP"
	}

	for _, endpoint := range statusEndpoints {
		healthReq := &hclient.HealthRequest{
			Url:      endpoint,
			Protocol: protocol,
		}
		go func() {
			for {
				// this is a blocking call, messages will be received on the respCh
				err := h.healthClient.HealthMonitor(ctx, healthReq, respCh)
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
}

func healthResultsCopy(healthResults map[types.NodeName]health.Result) map[types.NodeName]health.Result {
	out := make(map[types.NodeName]health.Result)
	for nodeID, healthResult := range healthResults {
		out[nodeID] = healthResult
	}
	return out
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
