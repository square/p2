package fields

import (
	"encoding/json"
	"sort"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/gofrs/uuid"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/util"
)

// ID is a named type alias for Resource Controller IDs. This is preferred to the raw
// string format so that Go will typecheck its uses.
type ID string

// String implements fmt.Stringer
func (id ID) String() string {
	return string(id)
}

func ToRCID(rcID string) (ID, error) {
	rcUUID, err := uuid.FromString(rcID)
	if err != nil {
		return "", util.Errorf("%v", err)
	}

	return ID(rcUUID.String()), nil
}

// Strategy is a type alias used for node allocation strategies.
type Strategy string

func (s Strategy) String() string { return string(s) }

const (
	// DynamicStrategy is used for dynamic node allocation to one of many
	// replaceable nodes, while StaticStrategy is used when a manifest
	// must be scheduled on specific nodes
	DynamicStrategy = Strategy("dynamic_strategy")
	StaticStrategy  = Strategy("static_strategy")
)

// RC holds the runtime state of a Resource Controller as saved in Consul.
type RC struct {
	// GUID for this controller
	ID ID

	// The pod manifest that should be scheduled on nodes
	Manifest manifest.Manifest

	// Defines the set of nodes on which the manifest can be scheduled
	NodeSelector labels.Selector

	// A set of labels that will be added to every pod scheduled by this controller.
	PodLabels labels.Set

	// The desired number of instances of the manifest that should be
	// scheduled.
	ReplicasDesired int

	// When disabled, this controller will not make any scheduling changes
	Disabled bool

	// Distinguishes between dynamic, static or other strategies for allocating
	// nodes on which the rc can schedule the manifest.
	AllocationStrategy Strategy
}

// RawRC defines the JSON format used to store data into Consul. It should only be used
// while (de-)serializing the RC state. Prefer using the "RC" when possible.
type RawRC struct {
	ID           ID         `json:"id"`
	Manifest     string     `json:"manifest"`
	NodeSelector string     `json:"node_selector"`
	PodLabels    labels.Set `json:"pod_labels"`

	// ReplicasDesired is an int pointer so we can distinguish between a
	// zero-count indicating the RC handler should remove any and all pods
	// from a case (for instance if the json key was changed) where golang
	// is defaulting to the 0 value
	ReplicasDesired    *int     `json:"replicas_desired"`
	Disabled           bool     `json:"disabled"`
	AllocationStrategy Strategy `json:"allocation_strategy"`
}

// MarshalJSON implements the json.Marshaler interface for serializing the RC to JSON
// format.
//
// The RC struct contains interfaces (manifest.Manifest, labels.Selector), and
// unmarshaling into a nil, non-empty interface is impossible (unless the value
// is a JSON null), because the unmarshaler doesn't know what structure to
// allocate there
// we own manifest.Manifest, but we don't own labels.Selector, so we have to
// implement the json marshaling here to wrap around the interface values
func (rc RC) MarshalJSON() ([]byte, error) {
	rawRC, err := rc.ToRaw()
	if err != nil {
		return nil, err
	}

	return json.Marshal(rawRC)
}

// Converts an RC to a type that will marshal cleanly as JSON
func (rc RC) ToRaw() (RawRC, error) {
	var manifest []byte
	var err error
	if rc.Manifest != nil {
		manifest, err = rc.Manifest.Marshal()
		if err != nil {
			return RawRC{}, err
		}
	}

	var nodeSel string
	if rc.NodeSelector != nil {
		nodeSel = rc.NodeSelector.String()
	}

	return RawRC{
		ID:                 rc.ID,
		Manifest:           string(manifest),
		NodeSelector:       nodeSel,
		PodLabels:          rc.PodLabels,
		ReplicasDesired:    &rc.ReplicasDesired,
		Disabled:           rc.Disabled,
		AllocationStrategy: rc.AllocationStrategy,
	}, nil
}

var _ json.Marshaler = RC{}

// UnmarshalJSON implements the json.Unmarshaler interface for deserializing the JSON
// representation of an RC.
func (rc *RC) UnmarshalJSON(b []byte) error {
	var rawRC RawRC
	if err := json.Unmarshal(b, &rawRC); err != nil {
		return err
	}

	if rawRC.ReplicasDesired == nil {
		return util.Errorf("RC %s has no replicas_desired field", rawRC.ID)
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

	*rc = RC{
		ID:                 rawRC.ID,
		Manifest:           m,
		NodeSelector:       nodeSel,
		PodLabels:          rawRC.PodLabels,
		ReplicasDesired:    *rawRC.ReplicasDesired,
		Disabled:           rawRC.Disabled,
		AllocationStrategy: rawRC.AllocationStrategy,
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
