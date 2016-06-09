package fields

import (
	"encoding/json"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
)

// ID is a named type alias for DaemonSet IDs
type ID string

func (id ID) String() string {
	return string(id)
}

// Cluster name is where the DaemonSet lives on
type ClusterName string

// DaemonSet holds the runtime state of a Daemon Set as saved in Consul.
type DaemonSet struct {
	// UUID for this DaemonSet
	ID ID

	// When disabled, this DaemonSet will not make any scheduling changes
	Disabled bool

	// The pod manifest that should be scheduled on nodes
	Manifest pods.Manifest

	// Minimum health for nodes when scheduling
	MinHealth int

	// DaemonSet's environment name
	Name ClusterName

	// Defines the set of nodes on which the manifest can be scheduled
	NodeSelector labels.Selector

	// PodID to deploy
	PodID types.PodID
}

// RawDaemonSet defines the JSON format used to store data into Consul
type RawDaemonSet struct {
	ID           ID          `json:"id"`
	Disabled     bool        `json:"disabled"`
	Manifest     string      `json:"manifest"`
	MinHealth    int         `json:"min_health"`
	Name         ClusterName `json:"cluster_name"`
	NodeSelector string      `json:"node_selector"`
	PodID        types.PodID `json:"pod_id"`
}

// MarshalJSON implements the json.Marshaler interface for serializing the DS
// to JSON format. Lets json interface know how to marshal a DaemonSet
// Can be called using json.Marshal
func (ds DaemonSet) MarshalJSON() ([]byte, error) {
	var manifest []byte
	var err error
	if ds.Manifest != nil {
		manifest, err = ds.Manifest.Marshal()
		if err != nil {
			return nil, err
		}
	}

	var nodeSelector string
	if ds.NodeSelector != nil {
		nodeSelector = ds.NodeSelector.String()
	}

	return json.Marshal(RawDaemonSet{
		ID:           ds.ID,
		Disabled:     ds.Disabled,
		Manifest:     string(manifest),
		MinHealth:    ds.MinHealth,
		Name:         ds.Name,
		NodeSelector: nodeSelector,
		PodID:        ds.PodID,
	})
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

	podManifest, err := pods.ManifestFromBytes([]byte(rawDS.Manifest))
	if err != nil {
		return err
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
	}
	return nil
}

// Assert DaemonSet.UnmarshalJSON is implemented in json.Unmarshaler
var _ json.Unmarshaler = &DaemonSet{}
