package allocation

import (
	"github.com/square/p2/pkg/pods"
)

type Request struct {
	Manifest pods.PodManifest
	Replicas int
}

type Allocator interface {
	Allocate(Request) Allocation
}

type Predicate func(Allocation) Allocation

type Allocation struct {
	Nodes []Node
}

func IsMasterNode(a Allocation) Allocation {
	if !a.MasterNode().Valid() {
		return Allocation{}
	}
	return Allocation{[]Node{a.MasterNode()}}
}

func NewAllocation(nodes ...string) Allocation {
	allocationNodes := make([]Node, len(nodes))
	for i, node := range nodes {
		allocationNodes[i] = Node{node, false}
	}
	return Allocation{allocationNodes}
}

func (a *Allocation) MasterNode() Node {
	for _, node := range a.Nodes {
		if node.IsMaster {
			return node
		}
	}
	return Node{}
}

type Node struct {
	Name     string
	IsMaster bool
}

func (n Node) Valid() bool {
	return n.Name != ""
}
