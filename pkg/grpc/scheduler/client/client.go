package client

import (
	"context"

	scheduler_protos "github.com/square/p2/pkg/grpc/scheduler/protos"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	"google.golang.org/grpc"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type Client struct {
	schedulerClient scheduler_protos.P2SchedulerClient
}

func NewClient(conn *grpc.ClientConn) Client {
	return Client{
		schedulerClient: scheduler_protos.NewP2SchedulerClient(conn),
	}
}

func (c *Client) EligibleNodes(man manifest.Manifest, sel klabels.Selector) ([]types.NodeName, error) {
	manifestStr, err := man.Marshal()
	if err != nil {
		return nil, util.Errorf("could not marshal manifest for EligibleNodes gRPC request: %s", err)
	}

	req := &scheduler_protos.EligibleNodesRequest{
		Manifest:     string(manifestStr),
		NodeSelector: sel.String(),
	}

	resp, err := c.schedulerClient.EligibleNodes(context.TODO(), req)
	if err != nil {
		return nil, util.Errorf("EligibleNodes gRPC call failed: %s", err)
	}

	ret := make([]types.NodeName, len(resp.EligibleNodes))
	for i, node := range resp.EligibleNodes {
		ret[i] = types.NodeName(node)
	}

	return ret, nil
}

func (c *Client) AllocateNodes(manifest manifest.Manifest, nodeSelector klabels.Selector, nodesRequested int) ([]types.NodeName, error) {
	return nil, util.Errorf("AllocateNodes() not yet implemented")
}

func (c *Client) DeallocateNodes(nodes []types.NodeName) error {
	return util.Errorf("DeallocateNodes() not yet implemented")
}
