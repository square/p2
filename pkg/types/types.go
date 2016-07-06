// Package for declaring types that will be used by various other packages. This is useful
// for preventing import cycles. For example, pkg/pods depends on pkg/auth. If both
// wish to use pods.ID, an import cycle is created.
package types

import (
	"github.com/square/p2/pkg/cgroups"

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

type LaunchableVersion struct {
	ID   string            `yaml:"id"`
	Tags map[string]string `yaml:"tags"`
}

type LaunchableStanza struct {
	LaunchableType          string            `yaml:"launchable_type"`
	LaunchableId            string            `yaml:"launchable_id"`
	DigestLocation          string            `yaml:"digest_location,omitempty"`
	DigestSignatureLocation string            `yaml:"digest_signature_location,omitempty"`
	RestartTimeout          string            `yaml:"restart_timeout,omitempty"`
	CgroupConfig            cgroups.Config    `yaml:"cgroup,omitempty"`
	Env                     map[string]string `yaml:"env,omitempty"`

	// The URL from which the launchable can be downloaded. May not be used
	// in conjunction with Version
	Location string `yaml:"location"`

	// An alternative to using Location to inform artifact downloading. Version information
	// can be used to query a configured artifact registry which will provide the artifact
	// URL. Version may not be used in conjunction with Location
	Version LaunchableVersion `yaml:"version,omitempty"`
}
