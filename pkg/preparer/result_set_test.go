package preparer

import (
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
)

func podWithID(id string) pods.Manifest {
	builder := pods.NewManifestBuilder()
	builder.SetID(id)
	return builder.GetManifest()
}

func TestZipBasic(t *testing.T) {
	intent := []kp.ManifestResult{
		{Manifest: podWithID("foo")},
		{Manifest: podWithID("bar")}}
	reality := []kp.ManifestResult{
		{Manifest: podWithID("baz")},
		{Manifest: podWithID("bar")},
	}

	pairs := ZipResultSets(intent, reality)
	ValidatePairList(t, pairs)
	for _, pair := range pairs {
		switch pair.ID {
		case "bar":
			Assert(t).IsNotNil(pair.Intent, "bar pair should have intent")
			Assert(t).IsNotNil(pair.Reality, "bar pair should have reality")
		case "baz":
			// the nil pointer inside ManifestPair is getting boxed into an
			// interface when passed as an argument
			// therefore, Assert(t).IsNil doesn't work here
			if pair.Intent != nil {
				t.Error("baz pair should not have intent")
			}
			Assert(t).IsNotNil(pair.Reality, "baz pair should have reality")
		case "foo":
			Assert(t).IsNotNil(pair.Intent, "foo pair should have intent")
			if pair.Reality != nil {
				t.Error("baz pair should not have reality")
			}
		}
	}
}

func ValidatePairList(t *testing.T, l []ManifestPair) {
	for _, pair := range l {
		if pair.Intent != nil {
			Assert(t).AreEqual(pair.ID, pair.Intent.ID(), "intent manifest should have had same ID")
		}
		if pair.Reality != nil {
			Assert(t).AreEqual(pair.ID, pair.Reality.ID(), "reality manifest should have had same ID")
		}
	}
}
