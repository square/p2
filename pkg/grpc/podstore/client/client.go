package client

import (
	"crypto/tls"
	"time"

	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
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

func New(grpcAddress string, tlsConfig *tls.Config, logger logging.Logger) (Client, error) {
	dialOptions := []grpc.DialOption{grpc.WithBlock(), grpc.WithTimeout(5 * time.Second)}
	if tlsConfig != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
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

func (c Client) Close() {
	_ = c.conn.Close()
}
