package preparer

import (
	"sort"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/statusstore"
	"github.com/square/p2/pkg/manifest"
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
	PodUniqueKey *types.PodUniqueKey
}

func (p *Preparer) ZipUUIDPods(uuidPods []kp.ManifestResult) []ManifestPair {
	var ret []ManifestPair
	for _, uuidPod := range uuidPods {

		if uuidPod.PodUniqueKey == nil {
			// This shouldn't happen and probably should result in an error at some
			// point. However, given that uuid pods are new functionality it's a soft
			// error for now
			continue
		}

		var realityManifest manifest.Manifest
		status, _, err := p.podStatusStore.Get(*uuidPod.PodUniqueKey)
		if err != nil && !statusstore.IsNoStatus(err) {
			// Soft error for same reasoning as above
			continue
		} else if err == nil {
			realityManifest, err = manifest.FromBytes([]byte(status.Manifest))
			if err != nil {
				// Soft error for now
				continue
			}
		}

		ret = append(ret, ManifestPair{
			ID:           uuidPod.PodLocation.PodID,
			PodUniqueKey: uuidPod.PodUniqueKey,
			Intent:       uuidPod.Manifest,
			Reality:      realityManifest,
		})
	}

	return ret
}

// Given two lists of ManifestResults, group them into pairs based on their pod
// ID. This function assumes that each list has unique pod IDs (ie no
// duplicates), otherwise its behavior is undefined. All returned ManifestPair
// values will have a nil PodUniqueKey because this function only handles
// legacy (non-uuid) pods
func ZipResultSets(intent, reality []kp.ManifestResult) (ret []ManifestPair) {
	sort.Sort(sortByID(intent))
	sort.Sort(sortByID(reality))

	var intentIndex, realityIndex int

	// this is like a union algorithm, we want to exhaust both lists
	for intentIndex < len(intent) || realityIndex < len(reality) {
		var iID, rID types.PodID
		// since one of the lists may go off-the-end before the other, we have
		// to guard any indexing into the lists to avoid out-of-range panics
		if intentIndex < len(intent) {
			iID = intent[intentIndex].Manifest.ID()
		}
		if realityIndex < len(reality) {
			rID = reality[realityIndex].Manifest.ID()
		}

		if iID != "" && (rID == "" || iID < rID) {
			// if rID == "", then realityIndex is out of range, but we're still in
			// the loop, therefore intentIndex must be in range
			ret = append(ret, ManifestPair{
				ID:           iID,
				Intent:       intent[intentIndex].Manifest,
				PodUniqueKey: nil,
			})
			intentIndex++
		} else if rID != "" && (iID == "" || rID < iID) {
			// and vice versa
			ret = append(ret, ManifestPair{
				ID:           rID,
				Reality:      reality[realityIndex].Manifest,
				PodUniqueKey: nil,
			})
			realityIndex++
		} else {
			ret = append(ret, ManifestPair{
				ID:           rID,
				Intent:       intent[intentIndex].Manifest,
				Reality:      reality[realityIndex].Manifest,
				PodUniqueKey: nil,
			})
			intentIndex++
			realityIndex++
		}
	}
	return
}

type sortByID []kp.ManifestResult

func (s sortByID) Len() int           { return len(s) }
func (s sortByID) Less(i, j int) bool { return s[i].Manifest.ID() < s[j].Manifest.ID() }
func (s sortByID) Swap(i, j int)      { tmp := s[i]; s[i] = s[j]; s[j] = tmp }

func checkResultsForID(intent []kp.ManifestResult, id types.PodID) bool {
	for _, result := range intent {
		if result.Manifest.ID() == id {
			return true
		}
	}
	return false
}
