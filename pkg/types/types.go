// Package for declaring types that will be used by various other packages. This is useful
// for preventing import cycles. For example, pkg/pods depends on pkg/auth. If both
// wish to use pods.ID, an import cycle is created.
package types

type NodeName string
type PodID string

func (n NodeName) String() string {
	return string(n)
}

func (p PodID) String() string {
	return string(p)
}
