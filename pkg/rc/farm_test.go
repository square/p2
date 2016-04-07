package rc

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/rc/rcmetrics"

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
	rcf.failsafe([]rcstore.RCLockResult{})
}

func TestRCsWithCountsWillBeFine(t *testing.T) {
	alerter := &failsafeAlerter{}
	rcf := &Farm{
		alerter: alerter,
	}

	rcs := []rcstore.RCLockResult{
		{
			RC: fields.RC{
				ReplicasDesired: 5,
			},
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

	rcs := []rcstore.RCLockResult{
		{
			RC: fields.RC{
				ReplicasDesired: 0,
			},
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

type FakeMetrics struct {
	rcProcessingTimeSamples []time.Duration
}

func (f *FakeMetrics) RecordRCProcessingTime(processingTime time.Duration) {
	f.rcProcessingTimeSamples = append(f.rcProcessingTimeSamples, processingTime)
}

var _ rcmetrics.Metrics = &FakeMetrics{}

func TestRecordProcessingTime(t *testing.T) {
	rcf := &Farm{}
	fakeMetrics := &FakeMetrics{}
	rcf.SetMetrics(fakeMetrics)
	Assert(t).AreEqual(len(fakeMetrics.rcProcessingTimeSamples), 0, "No values should have been recorded for rc processing time yet")
	rcf.metrics.RecordRCProcessingTime(time.Second)
	Assert(t).AreEqual(len(fakeMetrics.rcProcessingTimeSamples), 1, "One value should have been recorded for rc processing time")
	Assert(t).AreEqual(fakeMetrics.rcProcessingTimeSamples[0], time.Second, "Recorded time sample didn't match expected")
}

func TestRecordProcessingTimeDoesntPanicIfNoMetricsRegistrySet(t *testing.T) {
	rcf := NewFarm(nil, nil, nil, nil, nil, logging.DefaultLogger, nil, nil)

	rcf.metrics.RecordRCProcessingTime(time.Second)
}
