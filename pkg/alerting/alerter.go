package alerting

import (
	"net/http"

	"github.com/square/p2/pkg/util"
)

// Currently the Alerter interface only has a single implementation for PagerDuty. As a result,
// AlertInfo has information that PagerDuty needs, and other integrations may not. As a result,
// some information here may be ignored in future implementations.
type AlertInfo struct {
	Description string
	// Used to dedup alerts so multiple alerts don't occur from the same problem
	IncidentKey string
	// Arbitrary JSON for alert triage
	Details interface{}
}

type Alerter interface {
	Alert(alertInfo AlertInfo, urgency Urgency) error
}

func NewPagerduty(highUrgencyServiceKey string, lowUrgencyServiceKey string, client *http.Client) (Alerter, error) {
	if highUrgencyServiceKey == "" {
		return nil, util.Errorf("high urgency service key must be provided for pagerduty alerters")
	}

	if lowUrgencyServiceKey == "" {
		return nil, util.Errorf("low urgency service key must be provided for pagerduty alerters")
	}

	if client == nil {
		client = http.DefaultClient
	}

	return &pagerdutyAlerter{
		HighUrgencyServiceKey: highUrgencyServiceKey,
		LowUrgencyServiceKey:  lowUrgencyServiceKey,
		Client:                client,
	}, nil
}

func NewNop() Alerter {
	return &nopAlerter{}
}
