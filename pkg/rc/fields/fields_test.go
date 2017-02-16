package fields

import (
	"encoding/json"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/manifest"
)

func TestJSONMarshal(t *testing.T) {
	mb := manifest.NewBuilder()
	mb.SetID("hello")
	m := mb.GetManifest()

	rc1 := RC{
		ID:              "hello",
		Manifest:        m,
		ReplicasDesired: 2,
	}

	b, err := json.Marshal(&rc1)
	Assert(t).IsNil(err, "should have marshaled")
	t.Log("serialized format:", string(b))

	var rc2 RC
	err = json.Unmarshal(b, &rc2)
	Assert(t).IsNil(err, "should have unmarshaled")
	Assert(t).AreEqual(rc1.ID, rc2.ID, "RC ID changed when serialized")
	Assert(t).AreEqual(rc1.Manifest.ID(), rc2.Manifest.ID(), "Manifest ID changed when serialized")
}

func TestZeroUnmarshal(t *testing.T) {
	var rc RC
	err := json.Unmarshal([]byte(`{}`), &rc)
	if err == nil {
		t.Errorf("expected an error unmarshaling an RC missing the replicas_desired field")
	}

	err = json.Unmarshal([]byte(`{"replicas_desired": 0}`), &rc)
	if err != nil {
		t.Errorf("got an error unmarshaling an otherwise-empty RC with a replicas_desired count of 0: %s", err)
	}
}
