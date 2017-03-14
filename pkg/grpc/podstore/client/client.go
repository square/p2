package client

import (
	"github.com/square/p2/pkg/grpc/podstore"
	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
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

// matches podstore.consulStore signature so no context.Context argument
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

func (c Client) UnschedulePod(ctx context.Context, podUniqueKey types.PodUniqueKey) error {
	_, err := c.client.UnschedulePod(ctx, &podstore_protos.UnschedulePodRequest{
		PodUniqueKey: podUniqueKey.String(),
	})
	return err
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

func (c Client) DeletePodStatus(ctx context.Context, podUniqueKey types.PodUniqueKey) error {
	req := &podstore_protos.DeletePodStatusRequest{
		PodUniqueKey: podUniqueKey.String(),
	}

	_, err := c.client.DeletePodStatus(ctx, req)
	return err
}

func (c Client) ListPodStatus(ctx context.Context) (map[types.PodUniqueKey]podstatus.PodStatus, error) {
	resp, err := c.client.ListPodStatus(ctx, &podstore_protos.ListPodStatusRequest{
		// TODO: the whole podstatus.PodStatus type is coupled to the preparer's notion
		// of pod status, so we ought to just make this namespace a constant in the
		// pod status store
		StatusNamespace: consul.PreparerPodStatusNamespace.String(),
	})
	if err != nil {
		return nil, err
	}

	ret := make(map[types.PodUniqueKey]podstatus.PodStatus)
	for podUniqueKeyStr, rawStatus := range resp.PodStatuses {
		podUniqueKey, err := types.ToPodUniqueKey(podUniqueKeyStr)
		if err != nil {
			return nil, err
		}
		ret[podUniqueKey] = podstore.PodStatusResponseToPodStatus(*rawStatus)
	}

	return ret, nil
}

func (c Client) MarkPodFailed(podUniqueKey types.PodUniqueKey) error {
	_, err := c.client.MarkPodFailed(context.Background(), &podstore_protos.MarkPodFailedRequest{
		PodUniqueKey: podUniqueKey.String(),
	})
	return err
}
