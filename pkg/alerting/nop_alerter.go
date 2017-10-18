package alerting

type nopAlerter struct{}

var _ Alerter = &nopAlerter{}

func (*nopAlerter) Alert(alertInfo AlertInfo, urgency Urgency) error {
	return nil
}
