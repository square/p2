package labels

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/square/p2/pkg/logging"
	klabels "k8s.io/kubernetes/pkg/labels"
)

// A simple http server that operates on a given applicator and terminates all
// the endpoints expected by the httpApplicator.
type labelHTTPServer struct {
	applicator Applicator
	batcher    Batcher
	useBatcher bool
	logger     logging.Logger
}

// Construct a new HTTP Applicator server. If batchTime is non-zero, use batching for handling
// requests that operate over an entire label type (select, list).
func NewHTTPLabelServer(applicator Applicator, batchTime time.Duration, logger logging.Logger) *labelHTTPServer {
	return &labelHTTPServer{applicator, NewBatcher(applicator, batchTime), batchTime > 0, logger}
}

func (l *labelHTTPServer) AddRoutes(r *mux.Router) {
	r.Methods("GET").Path("/select").HandlerFunc(l.Select)
	r.Methods("GET").Path("/labels/{type}/{id}").HandlerFunc(l.GetLabels)
	r.Methods("GET").Path("/labels/{type}").HandlerFunc(l.ListLabels)
	r.Methods("PUT").Path("/labels/{type}/{id}/{name}").HandlerFunc(l.SetLabel)
	r.Methods("PUT").Path("/labels/{type}/{id}").HandlerFunc(l.SetLabels)
	r.Methods("DELETE").Path("/labels/{type}/{id}/{name}").HandlerFunc(l.RemoveLabel)
	r.Methods("DELETE").Path("/labels/{type}/{id}").HandlerFunc(l.RemoveLabels)
}

func (l *labelHTTPServer) Handler() *mux.Router {
	r := mux.NewRouter()
	l.AddRoutes(r)
	return r
}

func getMuxLabelType(req *http.Request) (Type, error) {
	vars := mux.Vars(req)
	return AsType(vars["type"])
}

func getMuxLabelTypeAndID(req *http.Request) (Type, string, error) {
	vars := mux.Vars(req)
	t, err := AsType(vars["type"])
	if err != nil {
		return t, "", err
	}
	id, err := url.QueryUnescape(vars["id"])
	if err != nil {
		return t, "", err
	}
	if id == "" {
		return t, "", fmt.Errorf("ID was not passed")
	}
	return t, id, err
}

func getMuxLabelTypeIDAndName(req *http.Request) (Type, string, string, error) {
	vars := mux.Vars(req)
	t, err := AsType(vars["type"])
	if err != nil {
		return t, "", "", err
	}
	id, err := url.QueryUnescape(vars["id"])
	if err != nil {
		return t, "", "", err
	}
	if id == "" {
		return t, "", "", fmt.Errorf("ID was not passed")
	}
	name, err := url.QueryUnescape(vars["name"])
	if err != nil {
		return t, "", "", err
	}
	if name == "" {
		return t, "", "", fmt.Errorf("Name was not passed")
	}
	return t, id, name, err
}

func (l *labelHTTPServer) respondWithJSON(out interface{}, resp http.ResponseWriter) {
	result, err := json.Marshal(out)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}

	resp.Header().Set("Content-Type", "application/json")
	_, err = resp.Write(result)
	if err != nil {
		l.logger.Errorln(err)
	}
}

func (l *labelHTTPServer) Select(resp http.ResponseWriter, req *http.Request) {
	labelType, err := AsType(req.URL.Query().Get("type"))
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	selector, err := klabels.Parse(req.URL.Query().Get("selector"))
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}

	matches := []Labeled{}

	if l.useBatcher {
		allLabels, err := l.batcher.ForType(labelType).Retrieve()
		if err != nil {
			http.Error(resp, err.Error(), http.StatusServiceUnavailable)
			return
		}
		for _, candidate := range allLabels {
			if selector.Matches(candidate.Labels) {
				matches = append(matches, candidate)
			}
		}
	} else {
		matches, err = l.applicator.GetMatches(selector, labelType, false)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	l.respondWithJSON(matches, resp)
}

func (l *labelHTTPServer) GetLabels(resp http.ResponseWriter, req *http.Request) {
	labelType, id, err := getMuxLabelTypeAndID(req)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	labeled, err := l.applicator.GetLabels(labelType, id)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	l.respondWithJSON(labeled, resp)
}

func (l *labelHTTPServer) ListLabels(resp http.ResponseWriter, req *http.Request) {
	labelType, err := getMuxLabelType(req)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	var labeled []Labeled
	if l.useBatcher {
		labeled, err = l.batcher.ForType(labelType).Retrieve()
	} else {
		labeled, err = l.applicator.ListLabels(labelType)
	}

	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	l.respondWithJSON(labeled, resp)
}

func (l *labelHTTPServer) SetLabel(resp http.ResponseWriter, req *http.Request) {
	labelType, id, name, err := getMuxLabelTypeIDAndName(req)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	var setLabelRequest SetLabelRequest
	defer req.Body.Close()
	in, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	err = json.Unmarshal(in, &setLabelRequest)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	err = l.applicator.SetLabel(labelType, id, name, setLabelRequest.Value)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	resp.WriteHeader(http.StatusNoContent)
}

func (l *labelHTTPServer) SetLabels(resp http.ResponseWriter, req *http.Request) {
	labelType, id, err := getMuxLabelTypeAndID(req)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	var setLabelsRequest SetLabelsRequest
	defer req.Body.Close()
	in, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	err = json.Unmarshal(in, &setLabelsRequest)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	err = l.applicator.SetLabels(labelType, id, setLabelsRequest.Values)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	resp.WriteHeader(http.StatusNoContent)
}

func (l *labelHTTPServer) RemoveLabel(resp http.ResponseWriter, req *http.Request) {
	labelType, id, name, err := getMuxLabelTypeIDAndName(req)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	err = l.applicator.RemoveLabel(labelType, id, name)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	resp.WriteHeader(http.StatusNoContent)
}

func (l *labelHTTPServer) RemoveLabels(resp http.ResponseWriter, req *http.Request) {
	labelType, id, err := getMuxLabelTypeAndID(req)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	err = l.applicator.RemoveAllLabels(labelType, id)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusServiceUnavailable)
		return
	}
	resp.WriteHeader(http.StatusNoContent)
}
