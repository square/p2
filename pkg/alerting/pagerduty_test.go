package alerting

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/square/p2/pkg/util"
)

func TestNewPagerduty(t *testing.T) {
	alerter, err := NewPagerduty("some_service_key", "some_other_service_key", nil)
	if err != nil {
		t.Fatalf("Unexpected error creating pagerduty alerter: %s", err)
	}

	if alerter == nil {
		t.Fatalf("Received unexpectedly nil Alerter")
	}

	pdAlerter, ok := alerter.(*pagerdutyAlerter)
	if !ok {
		t.Fatalf("NewPagerduty() should have returned a pagerdutyAlerter")
	}

	if pdAlerter.Client == nil {
		t.Fatalf("NewPagerduty() should have created an http.Client for pagerdutyAlerter")
	}

	alerter, err = NewPagerduty("", "low_urgency", nil)
	if err == nil {
		t.Fatalf("Should have had an error creating a pagerduty alerter with an empty high urgency service key")
	}

	if alerter != nil {
		t.Fatalf("Returned alerter should be nil if there was an error")
	}

	alerter, err = NewPagerduty("high_urgency", "", nil)
	if err == nil {
		t.Fatalf("Should have had an error creating a pagerduty alerter with an empty low urgency service key")
	}

	if alerter != nil {
		t.Fatalf("Returned alerter should be nil if there was an error")
	}
}

func TestAlert(t *testing.T) {
	alerter := pagerdutyAlerter{
		HighUrgencyServiceKey: "high_urgency_service_key",
		Client:                okPagerdutyClient{},
	}

	alertInfo := AlertInfo{
		Description: "a fake error happened",
		IncidentKey: "incident_key",
		Details: struct {
			Host string `json:"host"`
		}{"host.com"},
	}

	err := alerter.Alert(alertInfo, HighUrgency)
	if err != nil {
		t.Fatalf("Unexpected error sending fake alert: %s", err)
	}
}

func TestForbidden(t *testing.T) {
	alerter := pagerdutyAlerter{
		HighUrgencyServiceKey: "high_urgency_service_key",
		Client:                forbiddenPagerdutyClient{},
	}

	alertInfo := AlertInfo{
		Description: "a fake error happened",
		IncidentKey: "incident_key",
		Details: struct {
			Host string `json:"host"`
		}{"host.com"},
	}

	err := alerter.Alert(alertInfo, HighUrgency)
	if err == nil {
		t.Fatalf("Expected error message due to rate limiting")
	}
}

func TestServerError(t *testing.T) {
	alerter := pagerdutyAlerter{
		HighUrgencyServiceKey: "high_urgency_service_key",
		Client:                badPagerdutyClient{},
	}

	alertInfo := AlertInfo{
		Description: "a fake error happened",
		IncidentKey: "incident_key",
		Details: struct {
			Host string `json:"host"`
		}{"host.com"},
	}

	err := alerter.Alert(alertInfo, HighUrgency)
	if err == nil {
		t.Fatalf("Expected error message due to server error")
	}
}

func TestIncompleteInformation(t *testing.T) {
	alerter := pagerdutyAlerter{
		HighUrgencyServiceKey: "high_urgency_service_key",
		Client:                okPagerdutyClient{},
	}

	// missing Description
	alertInfo := AlertInfo{
		IncidentKey: "incident_key",
		Details: struct {
			Host string `json:"host"`
		}{"host.com"},
	}

	err := alerter.Alert(alertInfo, HighUrgency)
	if err == nil {
		t.Fatalf("Expected error message due to missing description")
	}

	// missing IncidentKey
	alertInfo = AlertInfo{
		Description: "some description",
		Details: struct {
			Host string `json:"host"`
		}{"host.com"},
	}

	err = alerter.Alert(alertInfo, HighUrgency)
	if err == nil {
		t.Fatalf("Expected error message due to missing incident key")
	}
}

func TestUrgencyAndServiceKey(t *testing.T) {
	recordingClient := &serviceKeyRecordingPagerdutyClient{}
	alerter := pagerdutyAlerter{
		HighUrgencyServiceKey: "high_urgency",
		LowUrgencyServiceKey:  "low_urgency",
		Client:                recordingClient,
	}

	alertInfo := AlertInfo{
		Description: "a fake error happened",
		IncidentKey: "incident_key",
		Details: struct {
			Host string `json:"host"`
		}{"host.com"},
	}

	err := alerter.Alert(alertInfo, HighUrgency)
	if err != nil {
		t.Fatal(err)
	}

	if recordingClient.serviceKeyUsed != "high_urgency" {
		t.Errorf("expected the alerter to use the high urgency service key %q for high urgency alerts but used %q", "high_urgency", recordingClient.serviceKeyUsed)
	}

	err = alerter.Alert(alertInfo, LowUrgency)
	if err != nil {
		t.Fatal(err)
	}

	if recordingClient.serviceKeyUsed != "low_urgency" {
		t.Errorf("expected the alerter to use the low urgency service key %q for low urgency alerts but used %q", "low_urgency", recordingClient.serviceKeyUsed)
	}
}

type okPagerdutyClient struct{}

var _ Poster = okPagerdutyClient{}

func (okPagerdutyClient) Post(uri string, contentType string, body io.Reader) (resp *http.Response, err error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("ok"))),
	}, nil
}

type forbiddenPagerdutyClient struct{}

var _ Poster = forbiddenPagerdutyClient{}

func (forbiddenPagerdutyClient) Post(uri string, contentType string, body io.Reader) (resp *http.Response, err error) {
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("rate limiting"))),
	}, nil
}

type badPagerdutyClient struct{}

var _ Poster = badPagerdutyClient{}

func (badPagerdutyClient) Post(uri string, contentType string, body io.Reader) (resp *http.Response, err error) {
	return &http.Response{
		StatusCode: http.StatusInternalServerError,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("an error occurred"))),
	}, nil
}

type serviceKeyRecordingPagerdutyClient struct {
	serviceKeyUsed string
}

var _ Poster = &serviceKeyRecordingPagerdutyClient{}

func (s *serviceKeyRecordingPagerdutyClient) Post(uri string, contentType string, body io.Reader) (*http.Response, error) {
	pdBody := pagerdutyBody{}
	bodyBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, util.Errorf("Test reader couldn't read the pagerduty request's body: %s", err)
	}

	err = json.Unmarshal(bodyBytes, &pdBody)
	if err != nil {
		return nil, util.Errorf("serviceKeyRecordingPagerdutyClient couldn't unmarshal the body of the request as JSON: %s", err)
	}

	s.serviceKeyUsed = pdBody.ServiceKey

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("ok"))),
	}, nil
}
