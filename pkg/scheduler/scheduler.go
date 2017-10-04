package scheduler

import (
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

type NodeLabeler interface {
	GetMatches(klabels.Selector, labels.Type) ([]labels.Labeled, error)
}

type ApplicatorScheduler struct {
	applicator NodeLabeler
}

// ApplicatorSchedulers simply return the results of node label selector.
// The manifest is ignored.
func NewApplicatorScheduler(applicator NodeLabeler) *ApplicatorScheduler {
	return &ApplicatorScheduler{applicator: applicator}
}

func (sel *ApplicatorScheduler) EligibleNodes(_ manifest.Manifest, selector klabels.Selector) ([]types.NodeName, error) {
	nodes, err := sel.applicator.GetMatches(selector, labels.NODE)
	if err != nil {
		return nil, err
	}

	result := make([]types.NodeName, len(nodes))
	for i, node := range nodes {
		result[i] = types.NodeName(node.ID)
	}
	return result, nil
}

func (sel *ApplicatorScheduler) AllocateNodes(manifest.Manifest, klabels.Selector, int) ([]types.NodeName, error) {
	return nil, util.Errorf("AllocateNodes() not yet implemented")
}
