package podstore

import (
	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type store struct {
	innerStore innerStore
}

var _ podstore_protos.P2PodStoreServer = store{}

type innerStore interface {
	Schedule(manifest manifest.Manifest, node types.NodeName) (key types.PodUniqueKey, err error)
}

func NewServer(innerStore innerStore) store {
	return store{
		innerStore: innerStore,
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

	podUniqueKey, err := s.innerStore.Schedule(manifest, types.NodeName(req.NodeName))
	if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "could not schedule pod: %s", err)
	}

	resp := &podstore_protos.SchedulePodResponse{
		PodUniqueKey: podUniqueKey.String(),
	}
	return resp, nil
}

func (s store) WatchPodStatus(*podstore_protos.WatchPodStatusRequest, podstore_protos.P2PodStore_WatchPodStatusServer) error {
	panic("not implemented")
}
