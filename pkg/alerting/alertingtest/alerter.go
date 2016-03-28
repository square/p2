package alertingtest

import (
	"github.com/square/p2/pkg/alerting"
)

type AlertRecorder struct {
	Alerts []alerting.AlertInfo
}

var _ alerting.Alerter = &AlertRecorder{}

func NewRecorder() *AlertRecorder {
	return &AlertRecorder{}
}

func (f *AlertRecorder) Alert(alertInfo alerting.AlertInfo) error {
	f.Alerts = append(f.Alerts, alertInfo)
	return nil
}
