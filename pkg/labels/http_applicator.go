package labels

import (
	"encoding/json"
	"net/http"
	"net/url"

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

func (h *httpApplicator) GetLabels(labelType Type, id string) (Labeled, error) {
	return Labeled{}, util.Errorf("GetLabels not implemented for HttpApplicator (type %s, id %s)", labelType, id)
}

func (h *httpApplicator) GetMatches(selector labels.Selector, labelType Type) ([]Labeled, error) {
	params := url.Values{}
	params.Add("selector", selector.String())
	params.Add("type", labelType.String())

	// Make value copy of URL; don't want to mutate the URL in the struct.
	urlToGet := *h.matchesEndpoint
	urlToGet.RawQuery = params.Encode()

	resp, err := h.client.Get(urlToGet.String())
	if err != nil {
		return []Labeled{}, err
	}

	defer resp.Body.Close()

	matches := []string{}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&matches)
	if err != nil {
		return []Labeled{}, err
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

func (h *httpApplicator) WatchMatches(selector labels.Selector, labelType Type, quitCh chan struct{}) chan []Labeled {
	panic("Not implemented")
	return nil
}
