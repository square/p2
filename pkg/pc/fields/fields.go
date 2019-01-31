package fields

import (
	"encoding/json"
	"reflect"

	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/types"

	"k8s.io/kubernetes/pkg/labels"
)

// Types stored in the actual pod cluster document
type ID string
type AvailabilityZone string
type ClusterName string
type Annotations map[string]interface{}
type MinHealthPercentage int

// label keys used by pod selector
const (
	AvailabilityZoneLabel = types.AvailabilityZoneLabel
	ClusterNameLabel      = types.ClusterNameLabel
	PodIDLabel            = types.PodIDLabel
)

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
	ID ID

	// The ID of the pods that the cluster contains
	PodID types.PodID

	// Represents a region the pod cluster inhabits. P2 doesn't use this
	// value but it is useful for implementations that care about
	// geographical location of pod clusters
	AvailabilityZone AvailabilityZone

	// Human-readable name for the pod cluster. Must be unique within a
	// (PodID, AvailabilityZone) space
	Name ClusterName

	// Selector to identify the pods that are members of this pod cluster
	PodSelector labels.Selector

	// AllocationStrategy tweaks certain characteristic about how pods
	// within this cluster are managed. For example the "static" strategy will
	// never transfer a pod from one node to another without human
	// intervention whereas the "dynamic" strategy will
	AllocationStrategy rc_fields.Strategy

	// Free-form annotations for implementation-specific information on top
	// of pod clusters
	Annotations Annotations

	// Minimum health percentage that this pod cluster should have
	MinHealthPercentage MinHealthPercentage
}

func (pc *PodCluster) Equals(other *PodCluster) bool {
	if pc == nil && other == nil {
		return true
	} else if other == nil || pc == nil {
		return false
	}
	if pc.Name != other.Name ||
		pc.PodID != other.PodID ||
		pc.ID != other.ID ||
		pc.AvailabilityZone != other.AvailabilityZone {
		return false
	}
	if pc.PodSelector != nil && other.PodSelector == nil ||
		pc.PodSelector == nil && other.PodSelector != nil {
		return false
	}
	if pc.PodSelector != nil && other.PodSelector != nil &&
		pc.PodSelector.String() != other.PodSelector.String() {
		return false
	}
	if pc.AllocationStrategy != other.AllocationStrategy {
		return false
	}
	return reflect.DeepEqual(pc.Annotations, other.Annotations)
}

// Unfortunately due to weirdness of marshaling label selectors, we have to
// implement it ourselves. RawPodCluster mimics PodCluster but has a string
// type for PodSelector instead of labels.Selector
type RawPodCluster struct {
	ID                  ID                  `json:"id"`
	PodID               types.PodID         `json:"pod_id"`
	AvailabilityZone    AvailabilityZone    `json:"availability_zone"`
	Name                ClusterName         `json:"name"`
	PodSelector         string              `json:"pod_selector"`
	Annotations         Annotations         `json:"annotations"`
	AllocationStrategy  rc_fields.Strategy  `json:"allocation_strategy"`
	MinHealthPercentage MinHealthPercentage `json:"min_health_percentage"`
}

// MarshalJSON implements the json.Marshaler interface for serializing the
// PodCluster to JSON format.
//
// The PodCluster struct contains a labels.Selector interface, and unmarshaling
// into a nil, non-empty interface is impossible (unless the value is a JSON
// null), because the unmarshaler doesn't know what structure to allocate
// there. Since we don't own labels.Selector, we have to implement the json
// marshaling here to wrap around the interface value
func (pc PodCluster) MarshalJSON() ([]byte, error) {
	return json.Marshal(pc.ToRaw())
}

// Converts a pod cluster to a type that will marshal cleanly to JSON.
func (pc PodCluster) ToRaw() RawPodCluster {
	var podSel string
	if pc.PodSelector != nil {
		podSel = pc.PodSelector.String()
	}

	return RawPodCluster{
		ID:                  pc.ID,
		PodID:               pc.PodID,
		AvailabilityZone:    pc.AvailabilityZone,
		Name:                pc.Name,
		PodSelector:         podSel,
		Annotations:         pc.Annotations,
		AllocationStrategy:  pc.AllocationStrategy,
		MinHealthPercentage: pc.MinHealthPercentage,
	}
}

var _ json.Marshaler = PodCluster{}

// UnmarshalJSON implements the json.Unmarshaler interface for deserializing the JSON
// representation of an PodCluster.
func (pc *PodCluster) UnmarshalJSON(b []byte) error {
	var rawPC RawPodCluster
	if err := json.Unmarshal(b, &rawPC); err != nil {
		return err
	}

	podSel, err := labels.Parse(rawPC.PodSelector)
	if err != nil {
		return err
	}

	*pc = PodCluster{
		ID:                  rawPC.ID,
		PodID:               rawPC.PodID,
		AvailabilityZone:    rawPC.AvailabilityZone,
		Name:                rawPC.Name,
		PodSelector:         podSel,
		Annotations:         rawPC.Annotations,
		AllocationStrategy:  rawPC.AllocationStrategy,
		MinHealthPercentage: rawPC.MinHealthPercentage,
	}
	return nil
}

var _ json.Unmarshaler = &PodCluster{}
