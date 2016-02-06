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

	rc1 := fields.RC{
		ID:       "hello",
		Manifest: m,
	}

	b, err := json.Marshal(&rc1)
	Assert(t).IsNil(err, "should have marshaled")
	t.Log("serialized format:", string(b))

	var rc2 fields.RC
	err = json.Unmarshal(b, &rc2)
	Assert(t).IsNil(err, "should have unmarshaled")
	Assert(t).AreEqual(rc1.ID, rc2.ID, "RC ID changed when serialized")
	Assert(t).AreEqual(rc1.Manifest.ID(), rc2.Manifest.ID(), "Manifest ID changed when serialized")
}

func TestJSONUnmarshalEmpty(t *testing.T) {
	rc := fields.RC{}
	b, err := json.Marshal(&rc)
	Assert(t).IsNil(err, "should have marshaled")

	err = json.Unmarshal(b, &rc)
	Assert(t).IsNil(err, "Unexpected error unmarshaling an empty RC")
}
