package client

import (
	"time"

	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Client struct {
	// Kept around just to close it
	conn   *grpc.ClientConn
	client podstore_protos.P2PodStoreClient
	logger logging.Logger
}

func New(grpcAddress string, creds credentials.TransportCredentials, logger logging.Logger) (Client, error) {
	dialOptions := []grpc.DialOption{grpc.WithBlock(), grpc.WithTimeout(5 * time.Second)}
	if creds != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(grpcAddress, dialOptions...)
	if err != nil {
		return Client{}, err
	}

	return Client{
		conn:   conn,
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

func (c Client) Close() {
	_ = c.conn.Close()
}
