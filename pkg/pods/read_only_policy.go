package pods

import "github.com/square/p2/pkg/types"

type ReadOnlyPolicy struct {
	defaultReadOnly bool
	whitelist       []types.PodID
	blacklist       []types.PodID
}

func NewReadOnlyPolicy(defaultReadOnly bool, whitelist []types.PodID, blacklist []types.PodID) ReadOnlyPolicy {
	return ReadOnlyPolicy{
		defaultReadOnly: defaultReadOnly,
		whitelist:       whitelist,
		blacklist:       blacklist,
	}
}

func (p *ReadOnlyPolicy) IsReadOnly(pod types.PodID) bool {
	for _, v := range p.blacklist {
		if v == pod {
			return false
		}
	}

	for _, v := range p.whitelist {
		if v == pod {
			return true
		}
	}

	return p.defaultReadOnly
}
