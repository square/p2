package rc

import (
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/pods"
)

// A Scheduler decides what nodes are appropriate for a pod to run on.
// It potentially takes into account considerations such as existing load on the nodes,
// label selectors, and more.
type Scheduler interface {
	EligibleNodes(pods.Manifest, labels.Selector) ([]string, error)
}

type applicatorScheduler struct {
	applicator labels.Applicator
}

// ApplicatorSchedulers simply return the results of node label selector.
// The manifest is ignored.
func NewApplicatorScheduler(applicator labels.Applicator) *applicatorScheduler {
	return &applicatorScheduler{applicator: applicator}
}

func (sel *applicatorScheduler) EligibleNodes(_ pods.Manifest, selector labels.Selector) ([]string, error) {
	nodes, err := sel.applicator.GetMatches(selector, labels.NODE)
	if err != nil {
		return []string{}, err
	}

	result := make([]string, len(nodes))
	for i, node := range nodes {
		result[i] = node.ID
	}
	return result, nil
}
