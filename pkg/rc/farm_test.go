package rc

import (
	"testing"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/rc/fields"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
)

type failsafeAlerter struct {
	savedInfo alerting.AlertInfo
}

func (f *failsafeAlerter) Alert(info alerting.AlertInfo) error {
	f.savedInfo = info
	return nil
}

func TestNoRCsWillCausePanic(t *testing.T) {
	alerter := &failsafeAlerter{}
	rcf := &Farm{
		alerter: alerter,
	}

	defer func() {
		if p := recover(); p == nil {
			t.Fatal("Should have panicked at sight of no RCs")
		}
		Assert(t).AreEqual("no_rcs_found", alerter.savedInfo.IncidentKey, "should have had a fired alert")
	}()
	rcf.failsafe([]fields.RC{})
}

func TestRCsWithCountsWillBeFine(t *testing.T) {
	alerter := &failsafeAlerter{}
	rcf := &Farm{
		alerter: alerter,
	}

	rcs := []fields.RC{
		fields.RC{
			ReplicasDesired: 5,
		},
	}

	defer func() {
		if p := recover(); p != nil {
			t.Fatal("Should not have panicked since everything is fine")
		}
		Assert(t).AreEqual("", alerter.savedInfo.IncidentKey, "should not have had a fired alert")
	}()
	rcf.failsafe(rcs)
}

func TestRCsWithZeroCountsWillTriggerIncident(t *testing.T) {
	alerter := &failsafeAlerter{}
	rcf := &Farm{
		alerter: alerter,
	}

	rcs := []fields.RC{
		fields.RC{
			ReplicasDesired: 0,
		},
	}

	defer func() {
		if p := recover(); p == nil {
			t.Fatal("Should have panicked due to no replicas")
		}
		Assert(t).AreEqual("zero_replicas_found", alerter.savedInfo.IncidentKey, "should have had a fired alert")
	}()
	rcf.failsafe(rcs)
}
