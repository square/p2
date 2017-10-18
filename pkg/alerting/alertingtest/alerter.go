package alertingtest

import (
	"github.com/square/p2/pkg/alerting"
)

type alertCall struct {
	alertInfo alerting.AlertInfo
	urgency   alerting.Urgency
}

type AlertRecorder struct {
	Alerts []alertCall
}

var _ alerting.Alerter = &AlertRecorder{}

func NewRecorder() *AlertRecorder {
	return &AlertRecorder{}
}

func (f *AlertRecorder) Alert(alertInfo alerting.AlertInfo, urgency alerting.Urgency) error {
	f.Alerts = append(f.Alerts, alertCall{
		alertInfo: alertInfo,
		urgency:   urgency,
	})
	return nil
}
