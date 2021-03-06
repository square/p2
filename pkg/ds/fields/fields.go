package fields

import (
	"encoding/json"
	"time"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/gofrs/uuid"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

// ID is a named type alias for DaemonSet IDs
type ID string

func (id ID) String() string {
	return string(id)
}

func ToDaemonSetID(id string) (ID, error) {
	if _, err := uuid.FromString(id); err != nil {
		return "", util.Errorf("%v", err)
	}
	return ID(id), nil
}

// Cluster name is where the DaemonSet lives on
type ClusterName string

func (cn ClusterName) String() string {
	return string(cn)
}

// DaemonSet holds the runtime state of a Daemon Set as saved in Consul.
type DaemonSet struct {
	// UUID for this DaemonSet
	ID ID

	// When disabled, this DaemonSet will not make any scheduling changes
	Disabled bool

	// The pod manifest that should be scheduled on nodes
	Manifest manifest.Manifest

	// Minimum health for nodes when scheduling
	MinHealth int

	// DaemonSet's environment name
	Name ClusterName

	// Defines the set of nodes on which the manifest can be scheduled
	NodeSelector labels.Selector

	// PodID to deploy
	PodID types.PodID

	Timeout time.Duration
}

// RawDaemonSet defines the JSON format used to store data into Consul
type RawDaemonSet struct {
	ID           ID            `json:"id"`
	Disabled     bool          `json:"disabled"`
	Manifest     string        `json:"manifest"`
	MinHealth    int           `json:"min_health"`
	Name         ClusterName   `json:"cluster_name"`
	NodeSelector string        `json:"node_selector"`
	PodID        types.PodID   `json:"pod_id"`
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

	var podManifest manifest.Manifest
	if rawDS.Manifest != "" {
		var err error
		podManifest, err = manifest.FromBytes([]byte(rawDS.Manifest))
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
