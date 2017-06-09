package client

import (
	"context"

	"github.com/square/p2/pkg/ds"
	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/grpc/daemonsetstore"
	daemonsetstore_protos "github.com/square/p2/pkg/grpc/daemonsetstore/protos"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type Client struct {
	client daemonsetstore_protos.P2DaemonSetStoreClient
	logger logging.Logger
}

// assert that this client satisfies the interface used by the daemon set farm
// so it may be dropped in
var _ ds.DaemonSetStore = Client{}

func (c Client) List() ([]fields.DaemonSet, error) {
	resp, err := c.client.ListDaemonSets(context.Background(), &daemonsetstore_protos.ListDaemonSetsRequest{})
	if err != nil {
		return nil, util.Errorf("list daemon sets grpc failed: %s", err)
	}

	out := make([]fields.DaemonSet, len(resp.DaemonSets))
	for i, dsProto := range resp.DaemonSets {
		ds, err := daemonsetstore.ProtoDSToRawDS(dsProto)
		if err != nil {
			return nil, util.Errorf("list daemon sets grpc failed: %s", err)
		}

		out[i] = ds
	}

	return out, nil
}

func (c Client) Disable(id fields.ID) (fields.DaemonSet, error) {
	resp, err := c.client.DisableDaemonSet(context.Background(), &daemonsetstore_protos.DisableDaemonSetRequest{
		DaemonSetId: id.String(),
	})
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("disable daemon set grpc for %s failed: %s", id, err)
	}

	ds, err := daemonsetstore.ProtoDSToRawDS(resp.DaemonSet)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("disabling succeeded but could not convert from grpc proto type to DaemonSet: %s", err)
	}

	return ds, nil
}

// TODO: pass a context here instead of a <-chan struct{}. It's like this so it matches the consul
// implementation
func (c Client) Watch(quitCh <-chan struct{}) <-chan dsstore.WatchedDaemonSets {
	outerCtx, cancel := context.WithCancel(context.Background())
	go func() {
		<-quitCh
		cancel()
	}()

	var watchClient daemonsetstore_protos.P2DaemonSetStore_WatchDaemonSetsClient
	var err error
	outCh := make(chan dsstore.WatchedDaemonSets)

	go func() {
		for {
			innerCtx, innerCancel := context.WithCancel(outerCtx)
			defer innerCancel()
			watchClient, err = c.client.WatchDaemonSets(innerCtx, &daemonsetstore_protos.WatchDaemonSetsRequest{})
			if err != nil {
				innerCancel()
				c.logger.WithError(err).Errorln("WatchDaemonSets rpc failed, will retry")
				continue
			}

			var ret dsstore.WatchedDaemonSets
			resp, err := watchClient.Recv()
			if grpc.Code(err) == codes.Canceled {
				c.logger.Infoln("daemonsetstore grpc client: terminating Watch()")
				// This just means quitCh fired and the RPC was canceled as expected
				return
			} else if err != nil {
				ret = dsstore.WatchedDaemonSets{
					Err: err,
				}
			} else {
				ret = watchRespProtoToWatchedDaemonSets(resp)
			}

			select {
			case outCh <- ret:
			case <-outerCtx.Done():
				return
			}
		}
	}()

	return outCh
}

func watchRespProtoToWatchedDaemonSets(resp *daemonsetstore_protos.WatchDaemonSetsResponse) dsstore.WatchedDaemonSets {
	var ret dsstore.WatchedDaemonSets
	for _, dsProto := range resp.Created {
		ds, err := daemonsetstore.ProtoDSToRawDS(dsProto)
		if err != nil {
			return dsstore.WatchedDaemonSets{
				Err: err,
			}
		}
		ret.Created = append(ret.Created, &ds)
	}
	for _, dsProto := range resp.Updated {
		ds, err := daemonsetstore.ProtoDSToRawDS(dsProto)
		if err != nil {
			return dsstore.WatchedDaemonSets{
				Err: err,
			}
		}
		ret.Updated = append(ret.Updated, &ds)
	}
	for _, dsProto := range resp.Deleted {
		ds, err := daemonsetstore.ProtoDSToRawDS(dsProto)
		if err != nil {
			return dsstore.WatchedDaemonSets{
				Err: err,
			}
		}
		ret.Deleted = append(ret.Deleted, &ds)
	}

	return ret
}
