package client

import (
	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Client struct {
	client podstore_protos.P2PodStoreClient
	logger logging.Logger
}

func New(conn *grpc.ClientConn, logger logging.Logger) (Client, error) {
	return Client{
		client: podstore_protos.NewP2PodStoreClient(conn),
		logger: logger,
	}, nil
}

// matches podstore.consulStore signature
func (c Client) Schedule(manifest manifest.Manifest, node types.NodeName) (types.PodUniqueKey, error) {
	manifestBytes, err := manifest.Marshal()
	if err != nil {
		return "", util.Errorf("Could not marshal manifest: %s", err)
	}

	req := &podstore_protos.SchedulePodRequest{
		NodeName: node.String(),
		Manifest: string(manifestBytes),
	}

	resp, err := c.client.SchedulePod(context.Background(), req)
	if err != nil {
		return "", util.Errorf("Could not schedule pod: %s", err)
	}

	return types.PodUniqueKey(resp.PodUniqueKey), nil
}

type PodStatusResult struct {
	PodStatus *podstore_protos.PodStatusResponse `json:"pod_status"`
	Error     error                              `json:"-"`
}

func (c Client) WatchStatus(ctx context.Context, podUniqueKey types.PodUniqueKey, waitForExists bool) (<-chan PodStatusResult, error) {
	stream, err := c.client.WatchPodStatus(ctx, &podstore_protos.WatchPodStatusRequest{
		PodUniqueKey:    podUniqueKey.String(),
		StatusNamespace: consul.PreparerPodStatusNamespace.String(),
		WaitForExists:   waitForExists,
	})
	if err != nil {
		return nil, err
	}

	outCh := make(chan PodStatusResult)
	go func() {
		defer close(outCh)
		for {
			status, err := stream.Recv()
			select {
			case <-ctx.Done():
				return
			case outCh <- PodStatusResult{
				PodStatus: status,
				Error:     err,
			}:
			}
		}
	}()
	return outCh, nil
}
