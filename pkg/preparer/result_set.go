package preparer

import (
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
)

type ManifestPair struct {
	// save the ID in a separate field, so that the user of this object doesn't
	// have to check both manifests
	ID      types.PodID
	Intent  manifest.Manifest
	Reality manifest.Manifest

	// Used to determine where reality came from (and should be written to). If nil,
	// reality should be written to the /reality tree. If non-nil, status should be
	// written to the pod status store
	PodUniqueKey types.PodUniqueKey
}

// Uniquely represents a pod. There can exist no two intent results or two
// reality results with the same uniqueKey.
type uniqueKey struct {
	podID        types.PodID
	podUniqueKey types.PodUniqueKey
}

func getUniqueKey(result consul.ManifestResult) uniqueKey {
	return uniqueKey{
		podID:        result.Manifest.ID(),
		podUniqueKey: result.PodUniqueKey,
	}
}

// A ManifestResult may have either a non-nil Manifest OR a non-nil *PodUniqueKey. This function
// assumes that there will not be duplicate PodIDs (i.e. Manifest.ID()) or PodUniqueKeys in
// the same slice, and the behavior is undefined if this were to occur.
func (p *Preparer) ZipResultSets(intent []consul.ManifestResult, reality []consul.ManifestResult) []ManifestPair {
	keyToPair := make(map[uniqueKey]*ManifestPair)

	for _, intentResult := range intent {
		keyToPair[getUniqueKey(intentResult)] = &ManifestPair{
			Intent:       intentResult.Manifest,
			ID:           intentResult.Manifest.ID(),
			PodUniqueKey: intentResult.PodUniqueKey,
		}
	}

	for _, realityResult := range reality {
		key := getUniqueKey(realityResult)
		if keyToPair[key] == nil {
			keyToPair[key] = &ManifestPair{
				ID:           realityResult.Manifest.ID(),
				PodUniqueKey: realityResult.PodUniqueKey,
			}
		}
		keyToPair[key].Reality = realityResult.Manifest

		// These may or may not be set already depending on if there was an intent
		// value, but setting them twice is harmless and sometimes it won't be set
		keyToPair[key].ID = realityResult.Manifest.ID()
		keyToPair[key].PodUniqueKey = realityResult.PodUniqueKey
	}

	var ret []ManifestPair
	for _, pair := range keyToPair {
		ret = append(ret, *pair)
	}

	return ret
}

func checkResultsForID(intent []consul.ManifestResult, id types.PodID) bool {
	for _, result := range intent {
		if result.Manifest.ID() == id {
			return true
		}
	}
	return false
}
