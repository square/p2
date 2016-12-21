package scheduler

import (
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/types"
)

type NodeLabeler interface {
	GetMatches(klabels.Selector, labels.Type, bool) ([]labels.Labeled, error)
}

// A Scheduler decides what nodes are appropriate for a pod to run on.
// It potentially takes into account considerations such as existing load on the nodes,
// label selectors, and more.
type Scheduler interface {
	EligibleNodes(store.Manifest, klabels.Selector) ([]types.NodeName, error)
}

type applicatorScheduler struct {
	applicator NodeLabeler
}

// ApplicatorSchedulers simply return the results of node label selector.
// The manifest is ignored.
func NewApplicatorScheduler(applicator NodeLabeler) *applicatorScheduler {
	return &applicatorScheduler{applicator: applicator}
}

func (sel *applicatorScheduler) EligibleNodes(_ store.Manifest, selector klabels.Selector) ([]types.NodeName, error) {
	nodes, err := sel.applicator.GetMatches(selector, labels.NODE, false)
	if err != nil {
		return nil, err
	}

	result := make([]types.NodeName, len(nodes))
	for i, node := range nodes {
		result[i] = types.NodeName(node.ID)
	}
	return result, nil
}
