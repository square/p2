package preparer

import (
	"math/rand"
	"strconv"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
)

func podWithID(id string) pods.Manifest {
	builder := pods.NewManifestBuilder()
	builder.SetID(types.PodID(id))
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
	ValidatePairList(t, pairs, intent, reality)
}

func ValidatePairList(t *testing.T, l []ManifestPair, intent, reality []kp.ManifestResult) {
	for _, pair := range l {
		// fuzz check #1: all three fields of the Pair should have the same ID
		if pair.Intent != nil {
			Assert(t).AreEqual(pair.ID, pair.Intent.ID(), "intent manifest should have had same ID")
		}
		if pair.Reality != nil {
			Assert(t).AreEqual(pair.ID, pair.Reality.ID(), "reality manifest should have had same ID")
		}

		// fuzz check #2: if Pair.Intent is non-nil, then it should be in the
		// source intent list, and vice versa
		var intentFound bool
		for _, m := range intent {
			if pair.ID == m.Manifest.ID() {
				// cannot use Assert.IsNil against a pointer
				if pair.Intent == nil {
					t.Errorf("source intent list and pair contained matching id %q, but pair did not contain intent manifest", pair.ID)
				}
				intentFound = true
			}
		}
		Assert(t).IsTrue(pair.Intent == nil || intentFound, "should have found at least one manifest in the source intent list")

		// fuzz check #3: same as #2, but for reality
		var realityFound bool
		for _, m := range reality {
			if pair.ID == m.Manifest.ID() {
				// cannot use Assert.IsNil against a pointer
				if pair.Reality == nil {
					t.Errorf("source reality list and pair contained matching id %q, but pair did not contain reality manifest", pair.ID)
				}
				realityFound = true
			}
		}
		Assert(t).IsTrue(pair.Reality == nil || realityFound, "should have found at least one manifest in the source reality list")
	}
}

func TestZipFuzz(t *testing.T) {
	for i := 0; i < 500; i++ {
		intent := generateRandomManifestList(100)
		reality := generateRandomManifestList(100)
		ValidatePairList(t, ZipResultSets(intent, reality), intent, reality)
	}
}

// generates a list of manifests that is baseLength in length, then randomly
// discards up to half the elements
func generateRandomManifestList(baseLength int) []kp.ManifestResult {
	indices := rand.Perm(baseLength)
	discard := rand.Intn(baseLength / 2)

	ret := make([]kp.ManifestResult, baseLength-discard)
	for i := range ret {
		ret[i] = kp.ManifestResult{
			Manifest: podWithID(strconv.Itoa(indices[i])),
		}
	}

	return ret
}
