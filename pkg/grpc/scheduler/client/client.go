package client

import (
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"
)

type Client struct {
}

func (c *Client) EligibleNodes(manifest.Manifest, klabels.Selector) ([]types.NodeName, error) {
	return nil, util.Errorf("EligibleNodes() not yet implemented")
}

func (c *Client) AllocateNodes(manifest manifest.Manifest, nodeSelector klabels.Selector, nodesRequested int) ([]types.NodeName, error) {
	return nil, util.Errorf("AllocateNodes() not yet implemented")
}

func (c *Client) DeallocateNodes(nodes []types.NodeName) error {
	return util.Errorf("DeallocateNodes() not yet implemented")
}
