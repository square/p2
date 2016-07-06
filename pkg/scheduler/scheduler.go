package scheduler

import (
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
)

// A Scheduler decides what nodes are appropriate for a pod to run on.
// It potentially takes into account considerations such as existing load on the nodes,
// label selectors, and more.
type Scheduler interface {
	EligibleNodes(manifest.Manifest, klabels.Selector) ([]types.NodeName, error)
}

type applicatorScheduler struct {
	applicator labels.Applicator
}

// ApplicatorSchedulers simply return the results of node label selector.
// The manifest is ignored.
func NewApplicatorScheduler(applicator labels.Applicator) *applicatorScheduler {
	return &applicatorScheduler{applicator: applicator}
}

func (sel *applicatorScheduler) EligibleNodes(_ manifest.Manifest, selector klabels.Selector) ([]types.NodeName, error) {
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
