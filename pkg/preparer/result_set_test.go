package preparer

import (
	"math/rand"
	"strconv"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/podstore"
	"github.com/square/p2/pkg/kp/statusstore/podstatus"
	"github.com/square/p2/pkg/kp/statusstore/statusstoretest"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
)

func podWithID(id string) manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID(types.PodID(id))
	return builder.GetManifest()
}

// Instantiates a preparer with a podStore and podStatusStore backed by a fake
// consul KV implementation. The podStore field is necessary for acquiring the
// "intent" manifest for uuid pods, while the podStatusStore is necessary for
// getting the reality manifest
func testResultSetPreparer() *Preparer {
	fakeStatusStore := statusstoretest.NewFake()
	return &Preparer{
		podStatusStore: podstatus.NewConsul(fakeStatusStore, kp.PreparerPodStatusNamespace),
		podStore:       podstore.NewConsul(consulutil.NewFakeClient().KV()),
	}
}

func TestZipBasic(t *testing.T) {
	intent := []kp.ManifestResult{
		{Manifest: podWithID("foo")},
		{PodUniqueKey: "abc123", Manifest: podWithID("baz")},
		{PodUniqueKey: "def456", Manifest: podWithID("foo")},
		{Manifest: podWithID("bar")}}
	reality := []kp.ManifestResult{
		{Manifest: podWithID("baz")},
		{PodUniqueKey: "abc123", Manifest: podWithID("baz")},
		{PodUniqueKey: "deadbeef", Manifest: podWithID("bar")},
		{Manifest: podWithID("bar")},
	}

	preparer := testResultSetPreparer()

	pairs := preparer.ZipResultSets(intent, reality)
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
			if pair.PodUniqueKey == m.PodUniqueKey && pair.ID == m.Manifest.ID() {
				// cannot use Assert.IsNil against a pointer
				if pair.Intent == nil {
					t.Errorf("source intent list and pair contained matching id %s (unique key '%s'), but pair did not contain intent manifest", pair.ID, pair.PodUniqueKey)
				}
				intentFound = true
			}
		}
		Assert(t).IsTrue(pair.Intent == nil || intentFound, "should have found at least one manifest in the source intent list")

		// fuzz check #3: same as #2, but for reality
		var realityFound bool
		for _, m := range reality {
			if pair.PodUniqueKey == m.PodUniqueKey && pair.ID == m.Manifest.ID() {
				if pair.Reality == nil {
					t.Errorf("source reality list and pair contained matching id %s (unique key '%s'), but pair did not contain reality manifest", pair.ID, pair.PodUniqueKey)
				}
				realityFound = true
			}
		}
		Assert(t).IsTrue(pair.Reality == nil || realityFound, "should have found at least one manifest in the source reality list")
	}
}

func TestZipFuzz(t *testing.T) {
	preparer := testResultSetPreparer()
	for i := 0; i < 500; i++ {
		intent := generateRandomManifestList(100, preparer.podStore, t)
		reality := generateRandomManifestList(100, preparer.podStore, t)
		ValidatePairList(t, preparer.ZipResultSets(intent, reality), intent, reality)
	}
}

// generates a list of manifests that is baseLength in length, then randomly
// discards up to half the elements. Each entry that remains has a 50% chance
// of being a uuid pod and thus having its manifest put into the pod store.
func generateRandomManifestList(baseLength int, podStore podstore.Store, t *testing.T) []kp.ManifestResult {
	indices := rand.Perm(baseLength)
	discard := rand.Intn(baseLength / 2)

	ret := make([]kp.ManifestResult, baseLength-discard)
	for i := range ret {
		uuidPod := rand.Intn(2)
		manifest := podWithID(strconv.Itoa(indices[i]))
		if uuidPod == 0 {
			ret[i] = kp.ManifestResult{
				Manifest: manifest,
			}
		} else {
			uuid, err := podStore.Schedule(manifest, "some_node")
			if err != nil {
				t.Fatal(err)
			}
			ret[i] = kp.ManifestResult{
				PodUniqueKey: uuid,
				Manifest:     manifest,
			}
		}
	}

	return ret
}
