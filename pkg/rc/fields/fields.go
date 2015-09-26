package fields

import (
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/pods"
)

type ID string

func (id ID) String() string {
	return string(id)
}

type RC struct {
	Id              ID
	Manifest        pods.Manifest
	NodeSelector    labels.Selector
	PodLabels       labels.Set
	ReplicasDesired int
	Disabled        bool
}
