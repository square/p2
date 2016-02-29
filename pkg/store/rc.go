package store

import (
	"encoding/json"

	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

type ReplicationControllerID string

// RC holds the runtime state of a Replication Controller as saved in Consul.
type ReplicationController struct {
	// GUID for this controller
	ID ReplicationControllerID

	// The pod manifest that should be scheduled on nodes
	Manifest Manifest

	// Defines the set of nodes on which the manifest can be scheduled
	NodeSelector labels.Selector

	// A set of labels that will be added to every pod scheduled by this controller
	PodLabels labels.Set

	// The desired number of instances of the manifest that should be scheduled
	ReplicasDesired int

	// When disabled, this controller will not make any scheduling changes
	Disabled bool
}

// RawRC defines the JSON format used to store data into Consul. It should only be used
// while (de-)serializing the RC state. Prefer using the "ReplicationController" when
// possible.
type RawRC struct {
	ID              ReplicationControllerID `json:"id"`
	Manifest        string                  `json:"manifest"`
	NodeSelector    string                  `json:"node_selector"`
	PodLabels       labels.Set              `json:"pod_labels"`
	ReplicasDesired int                     `json:"replicas_desired"`
	Disabled        bool                    `json:"disabled"`
}

// MarshalJSON implements the json.Marshaler interface for serializing the RC to JSON
// format.
//
// The RC struct contains interfaces (pods.Manifest, labels.Selector), and unmarshaling
// into a nil, non-empty interface is impossible (unless the value is a JSON null),
// because the unmarshaler doesn't know what structure to allocate there.  We own
// Manifest, but we don't own labels.Selector, so we have to implement the JSON marshaling
// here to wrap around the interface values.
func (rc *ReplicationController) MarshalJSON() ([]byte, error) {
	var manifest []byte
	var err error
	if rc.Manifest != nil {
		manifest, err = rc.Manifest.Marshal()
		if err != nil {
			return nil, err
		}
	}

	var nodeSel string
	if rc.NodeSelector != nil {
		nodeSel = rc.NodeSelector.String()
	}

	return json.Marshal(RawRC{
		ID:              rc.ID,
		Manifest:        string(manifest),
		NodeSelector:    nodeSel,
		PodLabels:       rc.PodLabels,
		ReplicasDesired: rc.ReplicasDesired,
		Disabled:        rc.Disabled,
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface for deserializing the JSON
// representation of an RC.
func (rc *ReplicationController) UnmarshalJSON(b []byte) error {
	var rawRC RawRC
	if err := json.Unmarshal(b, &rawRC); err != nil {
		return err
	}

	m, err := ManifestFromBytes([]byte(rawRC.Manifest))
	if err != nil {
		return err
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
