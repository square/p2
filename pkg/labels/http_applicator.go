package labels

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"
)

// Makes remote calls to an API that can answer queries. See labelHTTPServer.
type httpApplicator struct {
	client *http.Client
	// The endpoint that will be queried for matches. It should not have any paths, but if there are
	// any they will be removed (this is for backwards compatibility for servers that added a path to
	// the URL.)
	matchesEndpoint *url.URL
	logger          logging.Logger
}

var _ ApplicatorWithoutWatches = &httpApplicator{}

func NewHTTPApplicator(client *http.Client, matchesEndpoint *url.URL) (*httpApplicator, error) {
	if matchesEndpoint == nil {
		return nil, util.Errorf("matches endpoint cannot be nil")
	}

	c := client
	if c == nil {
		c = http.DefaultClient
	}

	return &httpApplicator{
		logger:          logging.DefaultLogger,
		matchesEndpoint: matchesEndpoint,
		client:          c,
	}, nil
}

func (h *httpApplicator) toEntityURL(pathTail string, labelType Type, id string, params url.Values) *url.URL {
	return h.toURL(fmt.Sprintf("/api/labels/%v/%v%v", labelType, url.QueryEscape(id), pathTail), params)
}

func (h *httpApplicator) toURL(path string, params url.Values) *url.URL {
	// Make value copy of URL; don't want to mutate the URL in the struct.
	urlToGet := *h.matchesEndpoint
	urlToGet.Path = path
	urlToGet.RawQuery = params.Encode()
	return &urlToGet
}

type SetLabelRequest struct {
	Value string `json:"value"`
}

type SetLabelsRequest struct {
	Values map[string]string `json:"values"`
}

func convertHTTPRespToErr(resp *http.Response) error {
	if resp.StatusCode > 299 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			respBody = []byte("<no body decoded>")
		}
		return util.Errorf("%v\n%v", resp.Status, string(respBody))
	}
	return nil
}

func (h *httpApplicator) getJSON(target *url.URL, toPopulate interface{}) error {
	req, err := http.NewRequest("GET", target.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err = convertHTTPRespToErr(resp); err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, toPopulate)
	return err
}

func (h *httpApplicator) putJSON(target *url.URL, toMarshal interface{}) error {
	body, err := json.Marshal(toMarshal)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", target.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err = convertHTTPRespToErr(resp); err != nil {
		return err
	}
	return nil
}

// Sets one label value, only replacing that one label name if already set.
//
// POST /api/labels/:type/:id/:name
// {
// 	"value": "value_of_label"
// }
//
func (h *httpApplicator) SetLabel(labelType Type, id, name, value string) error {
	toMarshal := SetLabelRequest{
		Value: value,
	}
	target := h.toEntityURL(fmt.Sprintf("/%v", url.QueryEscape(name)), labelType, id, url.Values{})
	err := h.putJSON(target, toMarshal)
	return err
}

// Replaces all labels on the entity with the new set
//
// POST /api/labels/:type/:id
// {
// 	"values": {
//	 	"name1": "value1",
// 		"name2": "value2"
// 	}
// }
func (h *httpApplicator) SetLabels(labelType Type, id string, labels map[string]string) error {
	toMarshal := SetLabelsRequest{
		Values: labels,
	}
	target := h.toEntityURL("", labelType, id, url.Values{})
	err := h.putJSON(target, toMarshal)
	return err
}

// Removes all labels on the entity
//
// DELETE /api/labels/:type/:id/:name
//
func (h *httpApplicator) RemoveLabel(labelType Type, id, name string) error {
	target := h.toEntityURL(fmt.Sprintf("/%v", url.QueryEscape(name)), labelType, id, url.Values{})
	req, err := http.NewRequest("DELETE", target.String(), bytes.NewBufferString(""))
	if err != nil {
		return err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	if err = convertHTTPRespToErr(resp); err != nil {
		return err
	}
	return nil
}

// Removes all labels on the entity
//
// DELETE /api/labels/:type/:id
//
func (h *httpApplicator) RemoveAllLabels(labelType Type, id string) error {
	target := h.toEntityURL("", labelType, id, url.Values{})
	req, err := http.NewRequest("DELETE", target.String(), bytes.NewBufferString(""))
	if err != nil {
		return err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	if err = convertHTTPRespToErr(resp); err != nil {
		return err
	}
	return nil
}

// Finds all labels assigned to all entities under a type
//
// GET /api/labels/:type
//
func (h *httpApplicator) ListLabels(labelType Type) ([]Labeled, error) {
	target := h.toURL(fmt.Sprintf("/api/labels/%v", labelType), url.Values{})
	var labeled []Labeled
	err := h.getJSON(target, &labeled)
	return labeled, err
}

// Finds all labels on the given type and ID.
//
// GET /api/labels/:type/:id
func (h *httpApplicator) GetLabels(labelType Type, id string) (Labeled, error) {
	target := h.toEntityURL("", labelType, id, url.Values{})
	var labeled Labeled
	err := h.getJSON(target, &labeled)
	return labeled, err
}

func (h *httpApplicator) GetLabelsWithIndex(labelType Type, id string) (Labeled, uint64, error) {
	target := h.toEntityURL("", labelType, id, url.Values{})
	var out LabeledWithIndex
	err := h.getJSON(target, &out)
	return out.Labeled, out.Index, err
}

// Finds all matches for the given type and selector.
//
// GET /api/select?selector=:selector&type=:type&cachedMatch=:cachedMatch
func (h *httpApplicator) getMatches(selector labels.Selector, labelType Type, cachedMatch bool) ([]Labeled, error) {
	params := url.Values{}
	params.Add("selector", selector.String())
	params.Add("type", labelType.String())
	params.Add("cachedMatch", strconv.FormatBool(cachedMatch))

	// Make value copy of URL; don't want to mutate the URL in the struct.
	urlToGet := h.toURL("/api/select", params)

	req, err := http.NewRequest("GET", urlToGet.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	bodyData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []Labeled{}, err
	}

	if resp.StatusCode != 200 {
		return nil, util.Errorf("got %d response from http label server: %s", resp.StatusCode, string(bodyData))
	}

	// try to unmarshal as a set of labeled objects.
	var labeled []Labeled
	err = json.Unmarshal(bodyData, &labeled)
	if err == nil {
		return labeled, nil
	}
	h.logger.Warnln(err)

	// fallback to a list of IDs
	matches := []string{}
	err = json.Unmarshal(bodyData, &matches)
	if err != nil {
		l := len(bodyData)
		if l > 80 {
			l = 80
		}
		return []Labeled{}, util.Errorf(
			"bad response from http applicator %s: %s: %s %q",
			urlToGet.String(),
			err,
			resp.Status,
			string(bodyData[:l]),
		)
	}

	labeled = make([]Labeled, len(matches))

	for i, s := range matches {
		labeled[i] = Labeled{
			ID:        s,
			LabelType: labelType,
			Labels:    labels.Set{},
		}
	}

	return labeled, nil
}

func (h *httpApplicator) GetMatches(selector labels.Selector, labelType Type) ([]Labeled, error) {
	return h.getMatches(selector, labelType, false)
}

// aggregationRate is ignored here because that is configured on the server.
func (h *httpApplicator) GetCachedMatches(selector labels.Selector, labelType Type, aggregationRate time.Duration) ([]Labeled, error) {
	return h.getMatches(selector, labelType, true)
}
