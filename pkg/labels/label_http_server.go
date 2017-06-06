package labels

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	"github.com/square/p2/pkg/logging"
	p2metrics "github.com/square/p2/pkg/metrics"
	klabels "k8s.io/kubernetes/pkg/labels"
)

// General purpose backing store for labels and assignment
// to P2 objects. This is a subset of the labels.Applicator that
// omits the Watch functions
type ApplicatorWithoutWatches interface {
	SetLabel(labelType Type, id, name, value string) error
	SetLabels(labelType Type, id string, labels map[string]string) error
	RemoveLabel(labelType Type, id, name string) error
	RemoveAllLabels(labelType Type, id string) error
	ListLabels(labelType Type) ([]Labeled, error)
	GetLabels(labelType Type, id string) (Labeled, error)
	GetLabelsStale(labelType Type, id string) (Labeled, error)
	GetMatches(selector klabels.Selector, labelType Type, cachedMatch bool) ([]Labeled, error)
}

// A simple http server that operates on a given applicator and terminates all
// the endpoints expected by the httpApplicator.
type labelHTTPServer struct {
	applicator ApplicatorWithoutWatches
	batcher    Batcher
	useBatcher bool
	logger     logging.Logger
}

// Construct a new HTTP Applicator server. If batchTime is non-zero, use batching for handling
// requests that operate over an entire label type (select, list).
func NewHTTPLabelServer(applicator ApplicatorWithoutWatches, batchTime time.Duration, logger logging.Logger) *labelHTTPServer {
	return &labelHTTPServer{applicator, NewBatcher(applicator, batchTime), batchTime > 0, logger}
}

func (l *labelHTTPServer) AddRoutes(r *mux.Router) {
	r.Methods("GET").Path("/api/select").HandlerFunc(l.Select)
	r.Methods("GET").Path("/api/labels/{type}/{id}").HandlerFunc(l.GetLabels)
	r.Methods("GET").Path("/api/labels/{type}").HandlerFunc(l.ListLabels)
	r.Methods("PUT").Path("/api/labels/{type}/{id}/{name}").HandlerFunc(l.SetLabel)
	r.Methods("PUT").Path("/api/labels/{type}/{id}").HandlerFunc(l.SetLabels)
	r.Methods("DELETE").Path("/api/labels/{type}/{id}/{name}").HandlerFunc(l.RemoveLabel)
	r.Methods("DELETE").Path("/api/labels/{type}/{id}").HandlerFunc(l.RemoveLabels)
}

func timeHandler(endpoint string, t Type, fn func(string)) {
	endpoint = fmt.Sprintf("%v-%v", endpoint, t)
	hist := metrics.GetOrRegisterHistogram(endpoint, p2metrics.Registry, metrics.NewUniformSample(1000))
	start := time.Now()
	defer func() {
		hist.Update(int64(time.Now().Sub(start) / time.Millisecond))
	}()
	fn(endpoint)
}

// A not found error means that consul returned no labels for the label type.
// This is really bad for label types like replication controllers where we
// expect there to always be labels.  It might not be bad for types like
// rolling updates where there are none during steady state.
func (l *labelHTTPServer) notFound(resp http.ResponseWriter, endpoint string, labelType Type, err error) {
	http.Error(resp, err.Error(), http.StatusNotFound)
	counter := metrics.GetOrRegisterCounter(fmt.Sprintf("%v-%v-not-found", labelType, endpoint), p2metrics.Registry)
	counter.Inc(1)
}

func (l *labelHTTPServer) badRequest(resp http.ResponseWriter, endpoint string, err error) {
	http.Error(resp, err.Error(), http.StatusBadRequest)
	counter := metrics.GetOrRegisterCounter(fmt.Sprintf("%v-bad-requests", endpoint), p2metrics.Registry)
	counter.Inc(1)
}

func (l *labelHTTPServer) unavailable(resp http.ResponseWriter, endpoint string, err error) {
	http.Error(resp, err.Error(), http.StatusServiceUnavailable)
	counter := metrics.GetOrRegisterCounter(fmt.Sprintf("%v-unavailable", endpoint), p2metrics.Registry)
	counter.Inc(1)
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

func (l *labelHTTPServer) respondWithJSON(out interface{}, endpoint string, resp http.ResponseWriter) {
	result, err := json.Marshal(out)
	if err != nil {
		l.unavailable(resp, endpoint, err)
		return
	}

	resp.Header().Set("Content-Type", "application/json")
	_, err = resp.Write(result)
	if err != nil {
		l.logger.Errorln(err)
	}
}

func (l *labelHTTPServer) Select(resp http.ResponseWriter, req *http.Request) {
	endpoint := "select"
	labelType, err := AsType(req.URL.Query().Get("type"))
	if err != nil {
		l.badRequest(resp, endpoint, err)
		return
	}
	timeHandler(endpoint, labelType, func(endpoint string) {
		selector, err := klabels.Parse(req.URL.Query().Get("selector"))
		if err != nil {
			l.badRequest(resp, endpoint, err)
			return
		}

		matches := []Labeled{}

		if l.useBatcher {
			allLabels, err := l.batcher.ForType(labelType).Retrieve()
			if err != nil {
				l.unavailable(resp, endpoint, err)
				return
			}
			for _, candidate := range allLabels {
				if selector.Matches(candidate.Labels) {
					matches = append(matches, candidate)
				}
			}
		} else {
			matches, err = l.applicator.GetMatches(selector, labelType, false)
			if IsNoLabelsFound(err) {
				l.notFound(resp, endpoint, labelType, err)
				return
			}
			if err != nil {
				l.unavailable(resp, endpoint, err)
				return
			}
		}

		l.respondWithJSON(matches, endpoint, resp)
	})
}

func (l *labelHTTPServer) GetLabels(resp http.ResponseWriter, req *http.Request) {
	endpoint := "get-labels"
	labelType, id, err := getMuxLabelTypeAndID(req)
	if err != nil {
		l.badRequest(resp, endpoint, err)
		return
	}
	timeHandler(endpoint, labelType, func(endpoint string) {
		var labeled Labeled
		if _, ok := req.URL.Query()["stale"]; ok {
			// use the latest query from the batcher to quickly return a
			// result at the expense of consistency
			labeled, err = l.batcher.ForType(labelType).RetrieveStaleByID(id)
		} else {
			// TODO: consider using the batcher here
			labeled, err = l.applicator.GetLabels(labelType, id)
		}
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		l.respondWithJSON(labeled, endpoint, resp)
	})
}

func (l *labelHTTPServer) ListLabels(resp http.ResponseWriter, req *http.Request) {
	endpoint := "list-labels"
	labelType, err := getMuxLabelType(req)
	if err != nil {
		l.badRequest(resp, endpoint, err)
		return
	}
	timeHandler(endpoint, labelType, func(endpoint string) {
		var labeled []Labeled
		if l.useBatcher {
			labeled, err = l.batcher.ForType(labelType).Retrieve()
		} else {
			labeled, err = l.applicator.ListLabels(labelType)
		}

		if IsNoLabelsFound(err) {
			l.notFound(resp, endpoint, labelType, err)
			return
		}
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		l.respondWithJSON(labeled, endpoint, resp)
	})
}

func (l *labelHTTPServer) SetLabel(resp http.ResponseWriter, req *http.Request) {
	endpoint := "set-label"
	labelType, id, name, err := getMuxLabelTypeIDAndName(req)
	if err != nil {
		l.badRequest(resp, endpoint, err)
		return
	}
	timeHandler(endpoint, labelType, func(endpoint string) {
		var setLabelRequest SetLabelRequest
		defer req.Body.Close()
		in, err := ioutil.ReadAll(req.Body)
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		err = json.Unmarshal(in, &setLabelRequest)
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		err = l.applicator.SetLabel(labelType, id, name, setLabelRequest.Value)
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		resp.WriteHeader(http.StatusNoContent)
	})
}

func (l *labelHTTPServer) SetLabels(resp http.ResponseWriter, req *http.Request) {
	endpoint := "set-labels"
	labelType, id, err := getMuxLabelTypeAndID(req)
	if err != nil {
		l.badRequest(resp, endpoint, err)
		return
	}
	timeHandler(endpoint, labelType, func(endpoint string) {
		var setLabelsRequest SetLabelsRequest
		defer req.Body.Close()
		in, err := ioutil.ReadAll(req.Body)
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		err = json.Unmarshal(in, &setLabelsRequest)
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		err = l.applicator.SetLabels(labelType, id, setLabelsRequest.Values)
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		resp.WriteHeader(http.StatusNoContent)
	})
}

func (l *labelHTTPServer) RemoveLabel(resp http.ResponseWriter, req *http.Request) {
	endpoint := "remove-label"
	labelType, id, name, err := getMuxLabelTypeIDAndName(req)
	if err != nil {
		l.badRequest(resp, endpoint, err)
		return
	}
	timeHandler(endpoint, labelType, func(endpoint string) {
		err = l.applicator.RemoveLabel(labelType, id, name)
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		resp.WriteHeader(http.StatusNoContent)
	})
}

func (l *labelHTTPServer) RemoveLabels(resp http.ResponseWriter, req *http.Request) {
	endpoint := "remove-labels"
	labelType, id, err := getMuxLabelTypeAndID(req)
	if err != nil {
		l.badRequest(resp, endpoint, err)
		return
	}
	timeHandler(endpoint, labelType, func(endpoint string) {
		err = l.applicator.RemoveAllLabels(labelType, id)
		if err != nil {
			l.unavailable(resp, endpoint, err)
			return
		}
		resp.WriteHeader(http.StatusNoContent)
	})
}
