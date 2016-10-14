package labels

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"
)

type httpApplicator struct {
	client *http.Client
	// The endpoint that will be queried for matches.
	// GetMatches will add to this endpoint a query parameter with key "selector" and value selector.String()
	// WatchMatches will add the above "selector" as well as "watch=true"
	matchesEndpoint *url.URL
	logger          logging.Logger
}

var _ Applicator = &httpApplicator{}

func NewHttpApplicator(client *http.Client, matchesEndpoint *url.URL) (*httpApplicator, error) {
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

func (h *httpApplicator) SetLabel(labelType Type, id, name, value string) error {
	return util.Errorf("SetLabel not implemented for HttpApplicator (type %s, id %s, name %s, value %s)", labelType, id, name, value)
}

func (h *httpApplicator) SetLabels(labelType Type, id string, labels map[string]string) error {
	return util.Errorf("SetLabels not implemented for HttpApplicator (type %s, id %s, labels %s)", labelType, id, labels)
}

func (h *httpApplicator) RemoveLabel(labelType Type, id, name string) error {
	return util.Errorf("RemoveLabel not implemented for HttpApplicator (type %s, id %s, name %s)", labelType, id, name)
}

func (h *httpApplicator) RemoveAllLabels(labelType Type, id string) error {
	return util.Errorf("RemoveAllLabels not implemented for HttpApplicator (type %s, id %s)", labelType, id)
}

func (h *httpApplicator) ListLabels(labelType Type) ([]Labeled, error) {
	return nil, util.Errorf("ListLabels not implemented for HttpApplicator")
}

func (h *httpApplicator) GetLabels(labelType Type, id string) (Labeled, error) {
	return Labeled{}, util.Errorf("GetLabels not implemented for HttpApplicator (type %s, id %s)", labelType, id)
}

func (h *httpApplicator) GetMatches(selector labels.Selector, labelType Type, cachedMatch bool) ([]Labeled, error) {
	params := url.Values{}
	params.Add("selector", selector.String())
	params.Add("type", labelType.String())
	params.Add("cachedMatch", strconv.FormatBool(cachedMatch))

	// Make value copy of URL; don't want to mutate the URL in the struct.
	urlToGet := *h.matchesEndpoint
	urlToGet.RawQuery = params.Encode()

	req, err := http.NewRequest("GET", urlToGet.String(), nil)
	if err != nil {
		return []Labeled{}, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return []Labeled{}, err
	}
	defer resp.Body.Close()

	bodyData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []Labeled{}, err
	}

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

	labeled := make([]Labeled, len(matches))

	for i, s := range matches {
		labeled[i] = Labeled{
			ID:        s,
			LabelType: labelType,
			Labels:    labels.Set{},
		}
	}

	return labeled, nil
}

func (h *httpApplicator) WatchMatches(selector labels.Selector, labelType Type, quitCh <-chan struct{}) chan []Labeled {
	panic("Not implemented")
}

func (h *httpApplicator) WatchMatchDiff(
	selector labels.Selector,
	labelType Type,
	quitCh <-chan struct{},
) <-chan *LabeledChanges {
	panic("Not implemented")
}
