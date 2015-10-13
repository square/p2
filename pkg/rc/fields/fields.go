package fields

import (
	"encoding/json"

	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/pods"
)

type ID string

func (id ID) String() string {
	return string(id)
}

type RC struct {
	ID              ID
	Manifest        pods.Manifest
	NodeSelector    labels.Selector
	PodLabels       labels.Set
	ReplicasDesired int
	Disabled        bool
}

type jsonRC struct {
	ID              ID
	Manifest        string
	NodeSelector    string
	PodLabels       labels.Set
	ReplicasDesired int
	Disabled        bool
}

// the RC struct contains interfaces (pods.Manifest, labels.Selector), and
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

	return json.Marshal(jsonRC{
		ID:              rc.ID,
		Manifest:        string(manifest),
		NodeSelector:    nodeSel,
		PodLabels:       rc.PodLabels,
		ReplicasDesired: rc.ReplicasDesired,
		Disabled:        rc.Disabled,
	})
}

var _ json.Marshaler = RC{}

func (rc *RC) UnmarshalJSON(b []byte) error {
	var jrc jsonRC
	if err := json.Unmarshal(b, &jrc); err != nil {
		return err
	}

	m, err := pods.ManifestFromBytes([]byte(jrc.Manifest))
	if err != nil {
		return err
	}

	nodeSel, err := labels.Parse(jrc.NodeSelector)
	if err != nil {
		return err
	}

	*rc = RC{
		ID:              jrc.ID,
		Manifest:        m,
		NodeSelector:    nodeSel,
		PodLabels:       jrc.PodLabels,
		ReplicasDesired: jrc.ReplicasDesired,
		Disabled:        rc.Disabled,
	}
	return nil
}

var _ json.Unmarshaler = &RC{}
