package store

import (
	"encoding/json"
	"time"

	"k8s.io/kubernetes/pkg/labels"
)

// DaemonSetID is a named type alias for DaemonSet IDs
type DaemonSetID string

func (id DaemonSetID) String() string {
	return string(id)
}

// DaemonSetName provides a way to differentiate daemon sets that have the same
// PodID
type DaemonSetName string

// DaemonSet holds the runtime state of a Daemon Set as saved in Consul.
type DaemonSet struct {
	// UUID for this DaemonSet
	ID DaemonSetID

	// When disabled, this DaemonSet will not make any scheduling changes
	Disabled bool

	// The pod manifest that should be scheduled on nodes
	Manifest Manifest

	// Minimum health for nodes when scheduling
	MinHealth int

	// DaemonSet's environment name
	Name DaemonSetName

	// Defines the set of nodes on which the manifest can be scheduled
	NodeSelector labels.Selector

	// PodID to deploy
	PodID PodID

	Timeout time.Duration
}

// RawDaemonSet defines the JSON format used to store data into Consul
type RawDaemonSet struct {
	ID           DaemonSetID   `json:"id"`
	Disabled     bool          `json:"disabled"`
	Manifest     string        `json:"manifest"`
	MinHealth    int           `json:"min_health"`
	Name         DaemonSetName `json:"cluster_name"`
	NodeSelector string        `json:"node_selector"`
	PodID        PodID         `json:"pod_id"`
	Timeout      time.Duration `json:"timeout"`
}

// MarshalJSON implements the json.Marshaler interface for serializing the DS
// to JSON format. Lets json interface know how to marshal a DaemonSet
// Can be called using json.Marshal
func (ds DaemonSet) MarshalJSON() ([]byte, error) {
	rawDS, err := ds.ToRaw()
	if err != nil {
		return nil, err
	}
	return json.Marshal(rawDS)
}

// Converts a pod cluster to a type that will marshal cleanly to JSON.
func (ds *DaemonSet) ToRaw() (RawDaemonSet, error) {
	var manifest []byte
	var err error
	if ds.Manifest != nil {
		manifest, err = ds.Manifest.Marshal()
		if err != nil {
			return RawDaemonSet{}, err
		}
	}

	var nodeSelector string
	if ds.NodeSelector != nil {
		nodeSelector = ds.NodeSelector.String()
	}

	return RawDaemonSet{
		ID:           ds.ID,
		Disabled:     ds.Disabled,
		Manifest:     string(manifest),
		MinHealth:    ds.MinHealth,
		Name:         ds.Name,
		NodeSelector: nodeSelector,
		PodID:        ds.PodID,
		Timeout:      ds.Timeout,
	}, nil
}

// Assert DaemonSet.MashalJSON is implemented in json.Marshaler
var _ json.Marshaler = DaemonSet{}

// UnmarshalJSON implements the json.Unmarshaler interface for deserializing the JSON
// representation of an DS. Lets json interface know how to unmarshal a DaemonSet
// Can be called using json.Unmarshal
func (ds *DaemonSet) UnmarshalJSON(b []byte) error {
	var rawDS RawDaemonSet
	if err := json.Unmarshal(b, &rawDS); err != nil {
		return err
	}

	var podManifest Manifest
	if rawDS.Manifest != "" {
		var err error
		podManifest, err = FromBytes([]byte(rawDS.Manifest))
		if err != nil {
			return err
		}
	}

	nodeSelector, err := labels.Parse(rawDS.NodeSelector)
	if err != nil {
		return err
	}

	*ds = DaemonSet{
		ID:           rawDS.ID,
		Disabled:     rawDS.Disabled,
		Manifest:     podManifest,
		MinHealth:    rawDS.MinHealth,
		Name:         rawDS.Name,
		NodeSelector: nodeSelector,
		PodID:        rawDS.PodID,
		Timeout:      rawDS.Timeout,
	}
	return nil
}

// Assert DaemonSet.UnmarshalJSON is implemented in json.Unmarshaler
var _ json.Unmarshaler = &DaemonSet{}
