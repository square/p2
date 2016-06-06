package labels

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"k8s.io/kubernetes/pkg/labels"
)

const endpointSuffix = "/testendpoint"

func getMatches(t *testing.T, httpResponse string) ([]Labeled, error) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		Assert(t).AreEqual(r.URL.Path, endpointSuffix, "Unexpected path requested")
		Assert(t).AreEqual(r.URL.Query().Get("selector"), "r1=v1,r2=v2", "Unexpected selector requested")
		Assert(t).AreEqual(r.URL.Query().Get("type"), NODE.String(), "Unexpected type requested")
		fmt.Fprintln(w, httpResponse)
	}))
	defer server.Close()

	url, err := url.Parse(server.URL + endpointSuffix)
	Assert(t).IsNil(err, "expected no error parsing url")

	applicator, err := NewHttpApplicator(nil, url)
	Assert(t).IsNil(err, "expected no error creating HTTP applicator")
	selector := labels.Everything().Add("r1", labels.EqualsOperator, []string{"v1"}).Add("r2", labels.EqualsOperator, []string{"v2"})
	return applicator.GetMatches(selector, NODE)
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
