package preparer

import (
	"sort"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
)

type ManifestPair struct {
	// save the ID in a separate field, so that the user of this object doesn't
	// have to check both manifests
	ID      string
	Intent  pods.Manifest
	Reality pods.Manifest
}

// Given two lists of ManifestResults, group them into pairs based on their
// pod ID. This function assumes that each list has unique pod IDs (ie no
// duplicates), otherwise its behavior is undefined.
func ZipResultSets(intent, reality []kp.ManifestResult) (ret []ManifestPair) {
	sort.Sort(sortByID(intent))
	sort.Sort(sortByID(reality))

	var intentIndex, realityIndex int

	// this is like a union algorithm, we want to exhaust both lists
	for intentIndex < len(intent) || realityIndex < len(reality) {
		var iID, rID string
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
				ID:     iID,
				Intent: intent[intentIndex].Manifest,
			})
			intentIndex++
		} else if rID != "" && (iID == "" || rID < iID) {
			// and vice versa
			ret = append(ret, ManifestPair{
				ID:      rID,
				Reality: reality[realityIndex].Manifest,
			})
			realityIndex++
		} else {
			ret = append(ret, ManifestPair{
				ID:      rID,
				Intent:  intent[intentIndex].Manifest,
				Reality: reality[realityIndex].Manifest,
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

func checkResultsForID(intent []kp.ManifestResult, id string) bool {
	for _, result := range intent {
		if result.Manifest.ID() == id {
			return true
		}
	}
	return false
}
