package daemonsetstore

import (
	daemonsetstore_protos "github.com/square/p2/pkg/grpc/daemonsetstore/protos"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type Store struct {
}

var _ daemonsetstore_protos.P2DaemonSetStoreServer = Store{}

func (s Store) ListDaemonSets(context.Context, *daemonsetstore_protos.ListDaemonSetsRequest) (*daemonsetstore_protos.ListDaemonSetsResponse, error) {
	return nil, grpc.Errorf(codes.Unimplemented, "ListDaemonSets not implemented")
}

func (s Store) DisableDaemonSet(context.Context, *daemonsetstore_protos.DisableDaemonSetRequest) (*daemonsetstore_protos.DisableDaemonSetResponse, error) {
	return nil, grpc.Errorf(codes.Unimplemented, "DisableDaemonSet not implemented")
}

func (s Store) WatchDaemonSets(*daemonsetstore_protos.WatchDaemonSetsRequest, daemonsetstore_protos.P2DaemonSetStore_WatchDaemonSetsServer) error {
	return grpc.Errorf(codes.Unimplemented, "WatchDaemonSets not implemented")
}
