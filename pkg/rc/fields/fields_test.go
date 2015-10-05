package fields_test

import (
	"encoding/json"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
)

func TestJSONMarshal(t *testing.T) {
	mb := pods.NewManifestBuilder()
	mb.SetID("hello")
	m := mb.GetManifest()

	f := &fields.RC{
		ID:       "hello",
		Manifest: m,
	}

	b, err := json.Marshal(f)
	Assert(t).IsNil(err, "should have marshaled")
	Assert(t).AreEqual(string(b),
		`{"ID":"hello","Manifest":"id: hello\nlaunchables: {}\nconfig: {}\n","NodeSelector":"","PodLabels":null,"ReplicasDesired":0,"Disabled":false}`,
		"should have marshaled to equal values")

	err = json.Unmarshal(b, f)
	Assert(t).IsNil(err, "should have unmarshaled")
	Assert(t).AreEqual(f.ID.String(), "hello", "should have unmarshaled to hello")
	Assert(t).AreEqual(f.Manifest.ID(), "hello", "should have unmarshaled manifest to hello")
}
