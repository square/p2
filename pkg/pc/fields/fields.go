package fields

import (
	"github.com/square/p2/pkg/types"

	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

type ID string
type AvailabilityZone string
type ClusterName string
type Annotations map[string]interface{}

func (id ID) String() string {
	return string(id)
}

func (az AvailabilityZone) String() string {
	return string(az)
}

func (cn ClusterName) String() string {
	return string(cn)
}

type PodCluster struct {
	// GUID for this cluster
	ID ID `json:"id"`

	// The ID of the pods that the cluster contains
	PodID types.PodID `json:"pod_id"`

	// Represents a region the pod cluster inhabits. P2 doesn't use this
	// value but it is useful for implementations that care about
	// geographical location of pod clusters
	AvailabilityZone AvailabilityZone `json:"availability_zone"`

	// Human-readable name for the pod cluster. Must be unique within a
	// (PodID, AvaialibilityZone) space
	Name ClusterName `json:"name"`

	// Selector to identify the pods that are members of this pod cluster
	PodSelector labels.Selector `json:"pod_selector"`

	// Free-form annotations for implementation-specific information on top
	// of pod clusters
	Annotations Annotations `json:"annotations"`
}
