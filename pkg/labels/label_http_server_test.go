// +build !race

package labels

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/consulutil"

	"github.com/gorilla/mux"
)

func TestGetLabels(t *testing.T) {
	fixture := consulutil.NewFixture(t)

	applicator := NewConsulApplicator(fixture.Client, 0)
	server := NewHTTPLabelServer(applicator, 0, logging.TestLogger())
	router := mux.NewRouter()
	server.AddRoutes(router)

	err := applicator.SetLabel(NODE, "node1", "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	// get the index from the raw applicator so we can test for it later
	_, index, err := applicator.GetLabelsWithIndex(NODE, "node1")
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("http://doesntmatter.com/api/labels/%s/%s", "node", "node1"), nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	respBytes := w.Body.Bytes()
	if w.Code != http.StatusOK {
		t.Fatalf("got %d response from server with respone: %s", w.Code, string(respBytes))
	}

	var resp LabeledWithIndex
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.Labeled.Labels) != 1 {
		t.Fatalf("expected 1 label but there were %d", len(resp.Labeled.Labels))
	}

	if resp.Labeled.Labels["foo"] != "bar" {
		t.Fatalf("expected label value for %q to be %q but was %q", "foo", "bar", resp.Labeled.Labels["foo"])
	}

	if resp.Index != index {
		t.Fatalf("expected index to be %d but was %d", index, resp.Index)
	}
}
