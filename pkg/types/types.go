// Package for declaring types that will be used by various other packages. This is useful
// for preventing import cycles. For example, pkg/pods depends on pkg/auth. If both
// wish to use pods.ID, an import cycle is created.
package types

import (
	"k8s.io/kubernetes/pkg/util/sets"
)

type NodeName string
type PodID string

func (n NodeName) String() string {
	return string(n)
}

func (p PodID) String() string {
	return string(p)
}

type PodLocation struct {
	Node  NodeName
	PodID PodID
}
type PodLocations []PodLocation

// Nodes returns a list of just the locations' nodes.
func (l PodLocations) Nodes() []NodeName {
	nodes := make([]NodeName, len(l))
	for i, pod := range l {
		nodes[i] = pod.Node
	}
	return nodes
}

// Wraps sets.String to provide the functionality of a set when dealing with
// the NodeName type (which is a string)
type NodeSet struct {
	sets.String
}

func NewNodeSet(nodes ...NodeName) NodeSet {
	nodeStrings := make([]string, len(nodes))
	for i, node := range nodes {
		nodeStrings[i] = node.String()
	}

	return NodeSet{
		String: sets.NewString(nodeStrings...),
	}
}

func (n NodeSet) ListNodes() []NodeName {
	strings := n.List()
	nodes := make([]NodeName, len(strings))
	for i, str := range strings {
		nodes[i] = NodeName(str)
	}
	return nodes
}

func (n NodeSet) InsertNode(node NodeName) {
	n.String.Insert(node.String())
}

func (n NodeSet) DeleteNode(node NodeName) {
	n.String.Delete(node.String())
}

func (n NodeSet) Difference(other NodeSet) NodeSet {
	diff := n.String.Difference(other.String)
	return NodeSet{
		String: diff,
	}
}

func (n NodeSet) Equal(other NodeSet) bool {
	return n.String.Equal(other.String)
}

func (n NodeSet) PopAny() (NodeName, bool) {
	node, ok := n.String.PopAny()
	return NodeName(node), ok
}
