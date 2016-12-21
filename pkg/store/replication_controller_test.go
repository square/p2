package store

import (
	"encoding/json"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestJSONMarshal(t *testing.T) {
	mb := NewBuilder()
	mb.SetID("hello")
	m := mb.GetManifest()

	rc1 := ReplicationController{
		ID:       "hello",
		Manifest: m,
	}

	b, err := json.Marshal(&rc1)
	Assert(t).IsNil(err, "should have marshaled")
	t.Log("serialized format:", string(b))

	var rc2 ReplicationController
	err = json.Unmarshal(b, &rc2)
	Assert(t).IsNil(err, "should have unmarshaled")
	Assert(t).AreEqual(rc1.ID, rc2.ID, "RC ID changed when serialized")
	Assert(t).AreEqual(rc1.Manifest.ID(), rc2.Manifest.ID(), "Manifest ID changed when serialized")
}

func TestZeroUnmarshal(t *testing.T) {
	var rc ReplicationController
	err := json.Unmarshal([]byte(`{}`), &rc)
	Assert(t).IsNil(err, "error unmarshaling")
}
