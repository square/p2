package alerting

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/square/p2/pkg/util"
)

const (
	pagerdutyURI = "https://events.pagerduty.com/generic/2010-04-15/create_event.json"
	eventType    = "trigger"
)

// Subset of *http.Client functionality, useful for testing
type Poster interface {
	Post(uri string, contentType string, body io.Reader) (resp *http.Response, err error)
}

type pagerdutyAlerter struct {
	ServiceKey string
	Client     Poster
}

var _ Alerter = &pagerdutyAlerter{}

// Duplicates the information from AlertInfo but has the appropriate JSON tags
// as well as ServiceKey
type pagerdutyBody struct {
	// required, provided in AlertInfo
	Description string `json:"description"`
	IncidentKey string `json:"incident_key"`

	// optional, provided in AlertInfo
	Details interface{} `json:"details,omitempty"`

	// provided by pagerdutyAlerter
	ServiceKey string `json:"service_key"`
	EventType  string `json:"event_type"`
}

func (p *pagerdutyAlerter) Alert(alertInfo AlertInfo) error {
	// IncidentKey is not actually required by the PD API, but it's good
	// practice to set it and is useful in error messages
	if alertInfo.IncidentKey == "" {
		return util.Errorf("An incident key was not provided for the alert")
	}

	if alertInfo.Description == "" {
		return util.Errorf("A description was not provided for alert '%s", alertInfo.IncidentKey)
	}
	body := pagerdutyBody{
		ServiceKey:  p.ServiceKey,
		Description: alertInfo.Description,
		IncidentKey: alertInfo.IncidentKey,
		Details:     alertInfo.Details,
		EventType:   eventType,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return util.Errorf("Unable to marshal alert as JSON: %s", err)
	}

	resp, err := p.Client.Post(pagerdutyURI, "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		return util.Errorf("Unable to trigger incident: %s", err)
	}
	defer resp.Body.Close()

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return util.Errorf("Unable to read response from pagerduty when triggering incident: %s", err)
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	if resp.StatusCode == http.StatusForbidden {
		// TODO: retry these with backoff?
		return util.Errorf("Unable to trigger incident %s due to PagerDuty rate limiting", alertInfo.IncidentKey)
	}

	return p.handleError(resp.StatusCode, respBytes)
}

func (p *pagerdutyAlerter) handleError(code int, respBytes []byte) error {
	respJSON, err := json.Marshal(respBytes)
	if err != nil {
		// The response probably wasn't JSON
		return util.Errorf("%d response from PagerDuty: %s", code, string(respBytes))
	}
	return util.Errorf("%d response from PagerDuty: %s", code, string(respJSON))
}
