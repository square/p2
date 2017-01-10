package podstore

import (
	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/types"

	"github.com/hashicorp/consul/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type store struct {
	scheduler      Scheduler
	podStatusStore PodStatusStore
}

var _ podstore_protos.P2PodStoreServer = store{}

type Scheduler interface {
	Schedule(manifest manifest.Manifest, node types.NodeName) (key types.PodUniqueKey, err error)
}

type PodStatusStore interface {
	WaitForStatus(key types.PodUniqueKey, waitIndex uint64) (podstatus.PodStatus, *api.QueryMeta, error)
}

func NewServer(scheduler Scheduler, podStatusStore PodStatusStore) store {
	return store{
		scheduler:      scheduler,
		podStatusStore: podStatusStore,
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

// Represents the return values of WaitForStatus on the podstore. This is
// useful so the results can be passed on a channel so we can wait for
// cancellation on the main goroutine
type podStatusResult struct {
	status    podstatus.PodStatus
	queryMeta *api.QueryMeta
	err       error
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

	podStatusResultCh := make(chan podStatusResult)
	innerQuit := make(chan struct{})
	defer close(podStatusResultCh)
	waitIndex := req.WaitIndex
	go func() {
		for {
			status, queryMeta, err := s.podStatusStore.WaitForStatus(podUniqueKey, waitIndex)
			select {
			case podStatusResultCh <- podStatusResult{
				status:    status,
				queryMeta: queryMeta,
				err:       err,
			}:
				if err != nil {
					return
				}

				if queryMeta != nil {
					waitIndex = queryMeta.LastIndex
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
			close(innerQuit)
			return nil
		case result := <-podStatusResultCh:
			resp, err := podStatusResultToResp(result)
			if err != nil {
				return err
			}

			err = stream.Send(resp)
			if err != nil {
				return err
			}
		}
	}
}

func podStatusResultToResp(result podStatusResult) (*podstore_protos.PodStatusResponse, error) {
	if statusstore.IsNoStatus(result.err) {
		return nil, grpc.Errorf(codes.NotFound, result.err.Error())
	} else if result.err != nil {
		return nil, grpc.Errorf(codes.Unavailable, result.err.Error())
	}

	var processStatuses []*podstore_protos.ProcessStatus

	for _, processStatus := range result.status.ProcessStatuses {
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
		Manifest:        result.status.Manifest,
		PodState:        result.status.PodStatus.String(),
		ProcessStatuses: processStatuses,
		LastIndex:       result.queryMeta.LastIndex,
	}, nil
}
