package daemonsetstore

import (
	"github.com/square/p2/pkg/ds/fields"
	daemonsetstore_protos "github.com/square/p2/pkg/grpc/daemonsetstore/protos"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/util"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type ConsulStore interface {
	List() ([]fields.DaemonSet, error)
	Disable(id fields.ID) (fields.DaemonSet, error)
}

type Store struct {
	consulStore ConsulStore
}

func NewServer(consulStore ConsulStore) Store {
	return Store{
		consulStore: consulStore,
	}
}

var _ daemonsetstore_protos.P2DaemonSetStoreServer = Store{}

func (s Store) ListDaemonSets(_ context.Context, _ *daemonsetstore_protos.ListDaemonSetsRequest) (*daemonsetstore_protos.ListDaemonSetsResponse, error) {
	daemonSets, err := s.consulStore.List()
	if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "error listing daemon sets: %s", err)
	}

	ret := new(daemonsetstore_protos.ListDaemonSetsResponse)
	ret.DaemonSets = make([]*daemonsetstore_protos.DaemonSet, len(daemonSets))
	for i, ds := range daemonSets {
		proto, err := rawDSToProtoDS(ds)
		if err != nil {
			return nil, grpc.Errorf(codes.Unavailable, err.Error())
		}
		ret.DaemonSets[i] = proto
	}

	return ret, nil
}

func (s Store) DisableDaemonSet(_ context.Context, req *daemonsetstore_protos.DisableDaemonSetRequest) (*daemonsetstore_protos.DisableDaemonSetResponse, error) {
	id, err := fields.ToDaemonSetID(req.DaemonSetId)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	_, err = s.consulStore.Disable(id)
	if err != nil {
		if err == dsstore.NoDaemonSet {
			return nil, grpc.Errorf(codes.NotFound, "no daemon set with id %s was found", id)
		}

		return nil, grpc.Errorf(codes.Unavailable, "could not disable daemon set %s: %s", id, err)
	}

	return &daemonsetstore_protos.DisableDaemonSetResponse{}, nil
}

func (s Store) WatchDaemonSets(*daemonsetstore_protos.WatchDaemonSetsRequest, daemonsetstore_protos.P2DaemonSetStore_WatchDaemonSetsServer) error {
	return grpc.Errorf(codes.Unimplemented, "WatchDaemonSets not implemented")
}

func rawDSToProtoDS(rawDS fields.DaemonSet) (*daemonsetstore_protos.DaemonSet, error) {
	manifest, err := rawDS.Manifest.Marshal()
	if err != nil {
		return nil, util.Errorf("could not convert daemon set %s's manifest to string for daemon set proto: %s", rawDS.ID, err)
	}

	return &daemonsetstore_protos.DaemonSet{
		Id:           rawDS.ID.String(),
		Disabled:     rawDS.Disabled,
		Manifest:     string(manifest),
		MinHealth:    int64(rawDS.MinHealth),
		Name:         rawDS.Name.String(),
		NodeSelector: rawDS.NodeSelector.String(),
		PodId:        rawDS.PodID.String(),
		Timeout:      rawDS.Timeout.Nanoseconds(),
	}, nil
}
