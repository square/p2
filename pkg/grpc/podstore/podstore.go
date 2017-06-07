package podstore

import (
	"time"

	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/podstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"

	"github.com/hashicorp/consul/api"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type store struct {
	scheduler      Scheduler
	podStatusStore PodStatusStore
	consulClient   consulutil.ConsulClient
}

var _ podstore_protos.P2PodStoreServer = store{}

type Scheduler interface {
	Schedule(manifest manifest.Manifest, node types.NodeName) (key types.PodUniqueKey, err error)
	Unschedule(key types.PodUniqueKey) error
}

type PodStatusStore interface {
	Get(key types.PodUniqueKey) (podstatus.PodStatus, *api.QueryMeta, error)
	Delete(podUniqueKey types.PodUniqueKey) error
	WaitForStatus(key types.PodUniqueKey, waitIndex uint64) (podstatus.PodStatus, *api.QueryMeta, error)
	List() (map[types.PodUniqueKey]podstatus.PodStatus, error)
	MutateStatus(ctx context.Context, key types.PodUniqueKey, mutator func(podstatus.PodStatus) (podstatus.PodStatus, error)) error
}

func NewServer(scheduler Scheduler, podStatusStore PodStatusStore, consulClient consulutil.ConsulClient) store {
	return store{
		scheduler:      scheduler,
		podStatusStore: podStatusStore,
		consulClient:   consulClient,
	}
}

func (s store) SchedulePod(_ context.Context, req *podstore_protos.SchedulePodRequest) (*podstore_protos.SchedulePodResponse, error) {
	if req.NodeName == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "node_name must be provided")
	}

	if req.Manifest == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "manifest must be provided")
	}

	manifest, err := manifest.FromBytes([]byte(req.Manifest))
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "could not parse passed manifest: %s", err)
	}

	podUniqueKey, err := s.scheduler.Schedule(manifest, types.NodeName(req.NodeName))
	if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "could not schedule pod: %s", err)
	}

	resp := &podstore_protos.SchedulePodResponse{
		PodUniqueKey: podUniqueKey.String(),
	}
	return resp, nil
}

func (s store) UnschedulePod(_ context.Context, req *podstore_protos.UnschedulePodRequest) (*podstore_protos.UnschedulePodResponse, error) {
	podUniqueKeyStr := req.GetPodUniqueKey()
	if podUniqueKeyStr == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "pod_unique_key must be provided")
	}

	podUniqueKey, err := types.ToPodUniqueKey(podUniqueKeyStr)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "pod_unique_key of %q is invalid", podUniqueKeyStr)
	}

	err = s.scheduler.Unschedule(podUniqueKey)
	if podstore.IsNoPod(err) {
		return nil, grpc.Errorf(codes.NotFound, "no pod with pod_unique_key of %q found", podUniqueKey)
	} else if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "error unscheduling pod: %s", err)
	}
	return &podstore_protos.UnschedulePodResponse{}, nil
}

// Represents the return values of WaitForStatus on the podstore. This is
// useful so the results can be passed on a channel so we can wait for
// cancellation on the main goroutine
type podStatusResult struct {
	status podstatus.PodStatus
	err    error
}

func (s store) WatchPodStatus(req *podstore_protos.WatchPodStatusRequest, stream podstore_protos.P2PodStore_WatchPodStatusServer) error {
	if req.StatusNamespace != consul.PreparerPodStatusNamespace.String() {
		// Today this is the only namespace so we just make sure it doesn't diverge from expected
		return grpc.Errorf(codes.InvalidArgument, "%q is not an understood namespace, must be %q", req.StatusNamespace, consul.PreparerPodStatusNamespace)
	}

	podUniqueKey, err := types.ToPodUniqueKey(req.PodUniqueKey)
	if err == types.InvalidUUID {
		return grpc.Errorf(codes.InvalidArgument, "%q does not parse as pod unique key (uuid)", req.PodUniqueKey)
	} else if err != nil {
		return grpc.Errorf(codes.Unavailable, err.Error())
	}

	clientCancel := stream.Context().Done()

	var waitIndex uint64
	// Do one consistent fetch from consul to ensure we don't return any
	// stale results. From then on we'll use watches using the index we got
	// from the Get()
	status, queryMeta, err := s.podStatusStore.Get(podUniqueKey)
	switch {
	case statusstore.IsNoStatus(err) && req.WaitForExists:
		// The client has asked to not be sent 404s, just wait for the record
		// to exist.  Don't send a value, just update the wait index to use
		// for the next watch.
		waitIndex = queryMeta.LastIndex
	case err == nil:
		// send the value we got
		waitIndex = queryMeta.LastIndex
		select {
		case <-clientCancel:
			return nil
		default:
			if err != nil {
				return convertStatusStoreError(err)
			}
			resp := PodStatusToResp(status)

			err = stream.Send(resp)
			if err != nil {
				return err
			}
		}
	default:
		return grpc.Errorf(codes.Unavailable, "error fetching first result from consul: %s", err)
	}

	podStatusResultCh := make(chan podStatusResult)
	innerQuit := make(chan struct{})
	defer close(innerQuit)
	go func() {
		defer close(podStatusResultCh)
		for {
			status, queryMeta, err := s.podStatusStore.WaitForStatus(podUniqueKey, waitIndex)

			if queryMeta != nil {
				waitIndex = queryMeta.LastIndex
			}

			if statusstore.IsNoStatus(err) && req.WaitForExists {
				// the client wants 404 to be ignored, start
				// the watch again with our new index
				continue
			}
			select {
			case podStatusResultCh <- podStatusResult{
				status: status,
				err:    err,
			}:
				if err != nil {
					return
				}

			case <-innerQuit:
				// Client canceled
				return
			}
		}
	}()

	for {
		select {
		case <-clientCancel:
			return nil
		case result := <-podStatusResultCh:
			if result.err != nil {
				return convertStatusStoreError(result.err)
			}

			resp := PodStatusToResp(result.status)

			err = stream.Send(resp)
			if err != nil {
				return err
			}
		}
	}
}

func (s store) ListPodStatus(_ context.Context, req *podstore_protos.ListPodStatusRequest) (*podstore_protos.ListPodStatusResponse, error) {
	if req.StatusNamespace != consul.PreparerPodStatusNamespace.String() {
		// Today this is the only namespace so we just make sure it doesn't diverge from expected
		return nil, grpc.Errorf(codes.InvalidArgument, "%q is not an understood namespace, must be %q", req.StatusNamespace, consul.PreparerPodStatusNamespace)
	}

	statusMap, err := s.podStatusStore.List()
	if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "error listing pod status: %s", err)
	}

	ret := make(map[string]*podstore_protos.PodStatusResponse)
	for podUniqueKey, status := range statusMap {
		ret[podUniqueKey.String()] = PodStatusToResp(status)
	}

	return &podstore_protos.ListPodStatusResponse{
		PodStatuses: ret,
	}, nil
}

func (s store) DeletePodStatus(_ context.Context, req *podstore_protos.DeletePodStatusRequest) (*podstore_protos.DeletePodStatusResponse, error) {
	podUniqueKey, err := types.ToPodUniqueKey(req.PodUniqueKey)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "could not convert %s to a pod unique key: %s", req.PodUniqueKey, err)
	}

	err = s.podStatusStore.Delete(podUniqueKey)
	if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "error deleting pod status for %s: %s", podUniqueKey, err)
	}

	return &podstore_protos.DeletePodStatusResponse{}, nil
}

func (s store) MarkPodFailed(ctx context.Context, req *podstore_protos.MarkPodFailedRequest) (*podstore_protos.MarkPodFailedResponse, error) {
	podUniqueKey, err := types.ToPodUniqueKey(req.PodUniqueKey)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "could not convert %s to a pod unique key: %s", req.PodUniqueKey, err)
	}

	mutator := func(podStatus podstatus.PodStatus) (podstatus.PodStatus, error) {
		podStatus.PodStatus = podstatus.PodFailed
		return podStatus, nil
	}

	// we don't really need the CAS properties of MutateStatus but there
	// should be only one system trying to write the status record so it
	// doesn't hurt.
	trxctx, cancelFunc := transaction.New(ctx)
	err = s.podStatusStore.MutateStatus(trxctx, podUniqueKey, mutator)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "failed to construct a consul transaction")
	}
	err = transaction.Commit(trxctx, cancelFunc, s.consulClient.KV())
	if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "could not update pod %s to failed: %s", podUniqueKey, err)
	}

	return &podstore_protos.MarkPodFailedResponse{}, nil
}

// converts an error returned by the status store to an appropriate grpc error.
func convertStatusStoreError(err error) error {
	if statusstore.IsNoStatus(err) {
		return grpc.Errorf(codes.NotFound, err.Error())
	} else if err != nil {
		return grpc.Errorf(codes.Unavailable, err.Error())
	}

	return nil
}

func PodStatusToResp(podStatus podstatus.PodStatus) *podstore_protos.PodStatusResponse {
	var processStatuses []*podstore_protos.ProcessStatus

	for _, processStatus := range podStatus.ProcessStatuses {
		processStatuses = append(processStatuses, &podstore_protos.ProcessStatus{
			LaunchableId: processStatus.LaunchableID.String(),
			EntryPoint:   processStatus.EntryPoint,
			LastExit: &podstore_protos.ExitStatus{
				ExitTime:   processStatus.LastExit.ExitTime.Unix(),
				ExitCode:   int64(processStatus.LastExit.ExitCode),
				ExitStatus: int64(processStatus.LastExit.ExitStatus),
			},
		})
	}

	return &podstore_protos.PodStatusResponse{
		Manifest:        podStatus.Manifest,
		PodState:        podStatus.PodStatus.String(),
		ProcessStatuses: processStatuses,
	}
}

func PodStatusResponseToPodStatus(resp podstore_protos.PodStatusResponse) podstatus.PodStatus {
	var ret podstatus.PodStatus
	ret.PodStatus = podstatus.PodState(resp.PodState)
	ret.Manifest = resp.Manifest

	for _, rawProcessStatus := range resp.ProcessStatuses {
		processStatus := podstatus.ProcessStatus{
			LaunchableID: launch.LaunchableID(rawProcessStatus.LaunchableId),
			EntryPoint:   rawProcessStatus.EntryPoint,
		}
		if rawProcessStatus.LastExit != nil {
			processStatus.LastExit = &podstatus.ExitStatus{
				ExitTime:   time.Unix(rawProcessStatus.LastExit.ExitTime, 0),
				ExitCode:   int(rawProcessStatus.LastExit.ExitCode),
				ExitStatus: int(rawProcessStatus.LastExit.ExitStatus),
			}
		}
		ret.ProcessStatuses = append(ret.ProcessStatuses, processStatus)
	}

	return ret
}
