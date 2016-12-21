package store

import (
	"encoding/json"
	"sort"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/manifest"
)

// ReplicationControllerID represents the uuid identifying a
// ReplicationController. This is preferred to the raw string format so that
// Go will typecheck its uses.
type ReplicationControllerID string

// String implements fmt.Stringer
func (id ReplicationControllerID) String() string {
	return string(id)
}

// ReplicationController holds the runtime state of a ReplicationController Controller.
type ReplicationController struct {
	// GUID for this controller
	ID ReplicationControllerID

	// The pod manifest that should be scheduled on nodes
	Manifest manifest.Manifest

	// Defines the set of nodes on which the manifest can be scheduled
	NodeSelector labels.Selector

	// A set of labels that will be added to every pod scheduled by this controller
	PodLabels labels.Set

	// The desired number of instances of the manifest that should be scheduled
	ReplicasDesired int

	// When disabled, this controller will not make any scheduling changes
	Disabled bool
}

// RawReplicationController defines the JSON format used to serialize a replication controller. It
// should only be used while (de-)serializing the ReplicationController state to/from a datastore.
// Prefer using the "ReplicationController" when possible.
type RawReplicationController struct {
	ID              ReplicationControllerID `json:"id"`
	Manifest        string                  `json:"manifest"`
	NodeSelector    string                  `json:"node_selector"`
	PodLabels       labels.Set              `json:"pod_labels"`
	ReplicasDesired int                     `json:"replicas_desired"`
	Disabled        bool                    `json:"disabled"`
}

// MarshalJSON implements the json.Marshaler interface for serializing the ReplicationController to JSON
// format.
//
// The ReplicationController struct contains interfaces (manifest.Manifest, labels.Selector), and
// unmarshaling into a nil, non-empty interface is impossible (unless the value
// is a JSON null), because the unmarshaler doesn't know what structure to
// allocate there
// we own manifest.Manifest, but we don't own labels.Selector, so we have to
// implement the json marshaling here to wrap around the interface values
func (rc ReplicationController) MarshalJSON() ([]byte, error) {
	rawRC, err := rc.ToRaw()
	if err != nil {
		return nil, err
	}

	return json.Marshal(rawRC)
}

// Converts an ReplicationController to a type that will marshal cleanly as JSON
func (rc ReplicationController) ToRaw() (RawReplicationController, error) {
	var manifest []byte
	var err error
	if rc.Manifest != nil {
		manifest, err = rc.Manifest.Marshal()
		if err != nil {
			return RawReplicationController{}, err
		}
	}

	var nodeSel string
	if rc.NodeSelector != nil {
		nodeSel = rc.NodeSelector.String()
	}

	return RawReplicationController{
		ID:              rc.ID,
		Manifest:        string(manifest),
		NodeSelector:    nodeSel,
		PodLabels:       rc.PodLabels,
		ReplicasDesired: rc.ReplicasDesired,
		Disabled:        rc.Disabled,
	}, nil
}

var _ json.Marshaler = ReplicationController{}

// UnmarshalJSON implements the json.Unmarshaler interface for deserializing the JSON
// representation of an ReplicationController.
func (rc *ReplicationController) UnmarshalJSON(b []byte) error {
	var rawRC RawReplicationController
	if err := json.Unmarshal(b, &rawRC); err != nil {
		return err
	}

	var m manifest.Manifest
	if rawRC.Manifest != "" {
		var err error
		m, err = manifest.FromBytes([]byte(rawRC.Manifest))
		if err != nil {
			return err
		}
	}

	nodeSel, err := labels.Parse(rawRC.NodeSelector)
	if err != nil {
		return err
	}

	*rc = ReplicationController{
		ID:              rawRC.ID,
		Manifest:        m,
		NodeSelector:    nodeSel,
		PodLabels:       rawRC.PodLabels,
		ReplicasDesired: rawRC.ReplicasDesired,
		Disabled:        rawRC.Disabled,
	}
	return nil
}

var _ json.Unmarshaler = &ReplicationController{}

// Implements sort.Interface to make a list of ids sortable lexicographically
type ReplicationControllerIDs []ReplicationControllerID

var _ sort.Interface = make(ReplicationControllerIDs, 0)

func (ids ReplicationControllerIDs) Len() int {
	return len(ids)
}

func (ids ReplicationControllerIDs) Less(i, j int) bool {
	return ids[i] < ids[j]
}

func (ids ReplicationControllerIDs) Swap(i, j int) {
	ids[i], ids[j] = ids[j], ids[i]
}
