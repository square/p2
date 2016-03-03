package fields

import (
	"encoding/json"
	"sort"

	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/pods"
)

// ID is a named type alias for Resource Controller IDs. This is preferred to the raw
// string format so that Go will typecheck its uses.
type ID string

// String implements fmt.Stringer
func (id ID) String() string {
	return string(id)
}

// RC holds the runtime state of a Resource Controller as saved in Consul.
type RC struct {
	// GUID for this controller
	ID ID

	// The pod manifest that should be scheduled on nodes
	Manifest pods.Manifest

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
// while (de-)serializing the RC state. Prefer using the "RC" when possible.
type RawRC struct {
	ID              ID         `json:"id"`
	Manifest        string     `json:"manifest"`
	NodeSelector    string     `json:"node_selector"`
	PodLabels       labels.Set `json:"pod_labels"`
	ReplicasDesired int        `json:"replicas_desired"`
	Disabled        bool       `json:"disabled"`
}

// MarshalJSON implements the json.Marshaler interface for serializing the RC to JSON
// format.
//
// The RC struct contains interfaces (pods.Manifest, labels.Selector), and
// unmarshaling into a nil, non-empty interface is impossible (unless the value
// is a JSON null), because the unmarshaler doesn't know what structure to
// allocate there
// we own pods.Manifest, but we don't own labels.Selector, so we have to
// implement the json marshaling here to wrap around the interface values
func (rc RC) MarshalJSON() ([]byte, error) {
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

var _ json.Marshaler = RC{}

// UnmarshalJSON implements the json.Unmarshaler interface for deserializing the JSON
// representation of an RC.
func (rc *RC) UnmarshalJSON(b []byte) error {
	var rawRC RawRC
	if err := json.Unmarshal(b, &rawRC); err != nil {
		return err
	}

	m, err := pods.ManifestFromBytes([]byte(rawRC.Manifest))
	if err != nil {
		return err
	}

	nodeSel, err := labels.Parse(rawRC.NodeSelector)
	if err != nil {
		return err
	}

	*rc = RC{
		ID:              rawRC.ID,
		Manifest:        m,
		NodeSelector:    nodeSel,
		PodLabels:       rawRC.PodLabels,
		ReplicasDesired: rawRC.ReplicasDesired,
		Disabled:        rawRC.Disabled,
	}
	return nil
}

var _ json.Unmarshaler = &RC{}

// Implements sort.Interface to make a list of ids sortable lexicographically
type IDs []ID

var _ sort.Interface = make(IDs, 0)

func (ids IDs) Len() int {
	return len(ids)
}

func (ids IDs) Less(i, j int) bool {
	return ids[i] < ids[j]
}

func (ids IDs) Swap(i, j int) {
	ids[i], ids[j] = ids[j], ids[i]
}
