// Package allocation provides a generic interface for specifiying where certain things run.
// It is a simple, dumb and lightweight way to facilitate scheduling.
package allocation

import (
	"github.com/square/p2/pkg/pods"
)

// Allocation wraps a list of Nodes in the cluster that will be the hosts for a particular
// pod. Nodes contain metadata
type Allocation struct {
	Nodes []Node
}

type Node struct {
	Name string
}

// Requests describe the manifest and number of replicas of the running pod needed to satisfy
// the allocation. Implementators of the Allocator interface accept these to determine the
// allocation.
type Request struct {
	Manifest pods.PodManifest
	Replicas int
}

type Allocator interface {
	Allocate(Request) Allocation
}

func NewAllocation(nodes ...string) Allocation {
	allocationNodes := make([]Node, len(nodes))
	for i, node := range nodes {
		allocationNodes[i] = Node{node}
	}
	return Allocation{allocationNodes}
}

func (n Node) Valid() bool {
	return n.Name != ""
}
