package labels

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/logging"
	"k8s.io/kubernetes/pkg/labels"
)

const endpointSuffix = "/select"

func getMatches(t *testing.T, httpResponse string) ([]Labeled, error) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		Assert(t).AreEqual(r.URL.Path, endpointSuffix, "Unexpected path requested")
		Assert(t).AreEqual(r.URL.Query().Get("selector"), "r1=v1,r2=v2", "Unexpected selector requested")
		Assert(t).AreEqual(r.URL.Query().Get("type"), NODE.String(), "Unexpected type requested")
		Assert(t).AreEqual(r.URL.Query().Get("cachedMatch"), "true", "Expected a cachedMatch query to be sent")
		fmt.Fprintln(w, httpResponse)
	}))
	defer server.Close()

	url, err := url.Parse(server.URL)
	Assert(t).IsNil(err, "expected no error parsing url")

	applicator, err := NewHTTPApplicator(nil, url)
	Assert(t).IsNil(err, "expected no error creating HTTP applicator")
	selector := labels.Everything().Add("r1", labels.EqualsOperator, []string{"v1"}).Add("r2", labels.EqualsOperator, []string{"v2"})
	return applicator.GetMatches(selector, NODE, true)
}

func TestGetMatches(t *testing.T) {
	matches, err := getMatches(t, `["a","b"]`)
	Assert(t).IsNil(err, "expected no error getting matches")
	Assert(t).AreEqual(len(matches), 2, "Expected two matches")
	Assert(t).AreEqual(matches[0].ID, "a", "Unexpected ID of first match")
	Assert(t).AreEqual(matches[1].ID, "b", "Unexpected ID of second match")
}

func TestGetMatchesEmpty(t *testing.T) {
	matches, err := getMatches(t, `[]`)
	Assert(t).IsNil(err, "expected no error getting matches")
	Assert(t).AreEqual(len(matches), 0, "Expected no matches")
}

func TestGetMatchesTypeError(t *testing.T) {
	_, err := getMatches(t, `[1]`)
	Assert(t).IsNotNil(err, "expected error getting matches")
}

func TestGetMatchesNoJson(t *testing.T) {
	_, err := getMatches(t, `[`)
	Assert(t).IsNotNil(err, "expected error getting matches")
}

func TestGetMatchesFullFormat(t *testing.T) {
	matches, err := getMatches(t, `[
{
       "id": "red-rocket-10",
       "type": "node",
       "labels": {
               "r1": "v1",
               "r2": "v2",
               "r3": "red"
       }
},
{
       "id": "blue-blaster-20",
       "type": "node",
       "labels": {
               "r1": "v1",
               "r2": "v2",
               "r3": "blue"
       }
}
]`)
	Assert(t).IsNil(err, "Should not have erred getting a result")
	Assert(t).AreEqual(len(matches), 2, "Should have two results")
	Assert(t).AreEqual(matches[0].ID, "red-rocket-10", "should have seen correct label")
	Assert(t).AreEqual(matches[1].ID, "blue-blaster-20", "should have seen correct label")
	Assert(t).AreEqual(len(matches[0].Labels), 3, "Should have seen 3 labels for red-rocket-10")
}

func TestBatchRequests(t *testing.T) {
	server, applicator := fakeServerAndApplicator(t, 100*time.Millisecond)
	defer server.Close()
	Assert(t).IsNil(applicator.SetLabels(POD, "abc", labels.Set{"color": "green", "state": "experimental"}), "Should not err setting labels")
	Assert(t).IsNil(applicator.SetLabels(POD, "def", labels.Set{"color": "green", "state": "production"}), "Should not err setting labels")
	Assert(t).IsNil(applicator.SetLabels(POD, "f98", labels.Set{"color": "blue", "state": "production"}), "Should not err setting labels")
	Assert(t).IsNil(applicator.SetLabels(POD, "c56", labels.Set{"color": "blue", "state": "experimental"}), "Should not err setting labels")

	queryToResults := map[string][]string{
		"color = green":                      []string{"abc", "def"},
		"state = production":                 []string{"def", "f98"},
		"color = blue, state = production":   []string{"f98"},
		"color = blue":                       []string{"f98", "c56"},
		"state = experimental":               []string{"c56", "abc"},
		"color = blue, state = experimental": []string{"c56"},
	}
	var tests sync.WaitGroup
	for q, expect := range queryToResults {
		tests.Add(1)
		go func(query string, expect []string) {
			defer tests.Done()
			selector, err := labels.Parse(query)
			if err != nil {
				t.Errorf("Test setup error: %v", err)
				return
			}
			res, err := applicator.GetMatches(selector, POD, false)
			if err != nil {
				t.Errorf("Could not run applicator query: %v", err)
				return
			}
			if len(expect) != len(res) {
				t.Errorf("Incorrect number of query results for %v", query)
				return
			}
			for _, labeled := range res {
				var found bool
				for _, id := range expect {
					if id == labeled.ID {
						found = true
					}
				}
				if !found {
					t.Errorf("Found %v but shouldn't have found it", labeled.ID)
				}
			}
		}(q, expect)
	}
	doneCh := make(chan struct{})
	go func() {
		tests.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		return
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Tests timed out")
	}
}

func TestMutateAndSelect(t *testing.T) {
	server, applicator := fakeServerAndApplicator(t, 0)
	defer server.Close()
	podID := "abc/123"
	colorLabel := "p2/color"

	// set a single label and assert its presence
	Assert(t).IsNil(applicator.SetLabel(POD, podID, colorLabel, "red"), "Should not have erred setting label")
	podLabels, err := applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual("red", podLabels.Labels.Get(colorLabel), "Should have seen red on the color label")
	matches, err := applicator.GetMatches(labels.Everything().Add(colorLabel, labels.EqualsOperator, []string{"red"}), POD, false)
	Assert(t).IsNil(err, "There should not have been an error running a selector")
	Assert(t).AreEqual(1, len(matches), "Should have gotten a match")
	Assert(t).AreEqual(podID, matches[0].ID, "Wrong pod returned")
	Assert(t).AreEqual("red", matches[0].Labels.Get(colorLabel), "Wrong color returned")

	// set all labels, expect all to change
	Assert(t).IsNil(applicator.SetLabels(POD, podID, labels.Set{colorLabel: "green", "state": "experimental"}), "Should not err setting labels")
	podLabels, err = applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual("green", podLabels.Labels.Get(colorLabel), "Should have seen green on the color label")
	Assert(t).AreEqual("experimental", podLabels.Labels.Get("state"), "Should have seen experimental on the state label")

	// set a single label, expect only one of several labels to change
	Assert(t).IsNil(applicator.SetLabel(POD, podID, colorLabel, "orange"), "Should not have erred setting label")
	podLabels, err = applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual("orange", podLabels.Labels.Get(colorLabel), "Should have seen orange on the color label")
	Assert(t).AreEqual("experimental", podLabels.Labels.Get("state"), "Should have seen experimental on the state label")

	// set a label on a new pod, expect list to contain two pods
	Assert(t).IsNil(applicator.SetLabel(POD, "def-456", colorLabel, "blue"), "Should not have erred setting label")
	allPodLabels, err := applicator.ListLabels(POD)
	Assert(t).AreEqual(len(allPodLabels), 2, "All labeld pods should have been returned")
	bluePod := allPodLabels[0]
	if allPodLabels[0].ID == podID {
		bluePod = allPodLabels[1]
	}
	Assert(t).AreEqual("blue", bluePod.Labels.Get(colorLabel), "Should have returned label data")

	// remove a specific label, expect only one remains
	Assert(t).IsNil(applicator.RemoveLabel(POD, podID, colorLabel), "Should not have erred removing label")
	podLabels, err = applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual("experimental", podLabels.Labels.Get("state"), "Should have seen experimental on the state label")
	Assert(t).AreEqual(1, len(podLabels.Labels), "Should have only had one label")

	// remove all labels, expect none left
	Assert(t).IsNil(applicator.RemoveAllLabels(POD, podID), "Should not have erred removing labels")
	podLabels, err = applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual(0, len(podLabels.Labels), "Should have only had one label")
}

func fakeServerAndApplicator(t *testing.T, batchTime time.Duration) (*httptest.Server, *httpApplicator) {
	labelServer := NewHTTPLabelServer(NewFakeApplicator(), batchTime, logging.DefaultLogger)
	server := httptest.NewServer(labelServer.Handler())

	url, err := url.Parse(server.URL)
	Assert(t).IsNil(err, "expected no error parsing url")

	applicator, err := NewHTTPApplicator(nil, url)
	Assert(t).IsNil(err, "expected no error creating HTTP applicator")
	return server, applicator
}
