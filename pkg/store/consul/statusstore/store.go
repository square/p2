package statusstore

import (
	"github.com/hashicorp/consul/api"
)

// The root of the status tree (e.g. in Consul)
const statusTree string = "status"

// The resource type being labeled. See the constants below
type ResourceType string

func (t ResourceType) String() string { return string(t) }

// Should this be collapsed with label types and "tree" names? this stuff is
// all over the place but sometimes has subtle differences
const (
	PC  = ResourceType("pod_clusters")
	POD = ResourceType("pods")
)

// Unfortunately each ResourceType will carry along with it a different "ID"
// type def, e.g. types.PodID. Go isn't flexible enough to handle this so
// we require all callers to cast their ID type to ResourceID and back when
// interacting with the status store.
type ResourceID string

func (id ResourceID) String() string { return string(id) }

// The resource type isn't enough to namespace a status. For example, there
// might be two different implementations of pcstore.ConcreteSyncer that wish
// to record independent status structs for the same pod cluster without
// interfering with eachother.
type Namespace string

func (n Namespace) String() string { return string(n) }

// The store doesn't care what status looks like, it will be
// implementation-specific (e.g. some structured JSON).
type Status []byte

func (s Status) Bytes() []byte { return []byte(s) }

type Store interface {
	// Set the status for a particular resource specified by ResourceType and ID,
	// namespaced by a Namespace string
	SetStatus(t ResourceType, id ResourceID, namespace Namespace, status Status) error

	// Like SetStatus(), but compare-and-swap value at a specified ModifyIndex (see consul docs)
	CASStatus(t ResourceType, id ResourceID, namespace Namespace, status Status, modifyIndex uint64) error

	// Get the status for a particular resource specified by ResourceType and ID,
	// namespaced by a Namespace string
	GetStatus(t ResourceType, id ResourceID, namespace Namespace) (Status, *api.QueryMeta, error)

	// Like GetStatus(), but doesn't return status until waitIndex has been surpassed in consul
	WatchStatus(t ResourceType, id ResourceID, namespace Namespace, waitIndex uint64) (Status, *api.QueryMeta, error)

	// Delete the status entry for a resource that has been deleted once the
	// deletion has been processed
	DeleteStatus(t ResourceType, id ResourceID, namespace Namespace) error

	// Get the status for all namespaces for a particular resource specified
	// by ResourceType and ID
	GetAllStatusForResource(t ResourceType, id ResourceID) (map[Namespace]Status, error)

	// Get the statuses for all resources of a given type. Returns a map of
	// resource ID to map[Namespace]Status
	GetAllStatusForResourceType(t ResourceType) (map[ResourceID]map[Namespace]Status, error)
}
