package rc

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/rcstore"

	. "github.com/anthonybishopric/gotcha"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type failsafeAlerter struct {
	savedInfo alerting.AlertInfo
}

func (f *failsafeAlerter) Alert(info alerting.AlertInfo, urgency alerting.Urgency) error {
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
	fakeStore := rcstore.NewFake()
	alerter := &failsafeAlerter{}
	rcf := &Farm{
		alerter: alerter,
		rcStore: fakeStore,
	}

	rc, err := fakeStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", map[string]string{}, nil, "some_strategy")
	if err != nil {
		t.Fatalf("could not put an RC in the fake store: %s", err)
	}

	err = fakeStore.SetDesiredReplicas(rc.ID, 5)
	if err != nil {
		t.Fatalf("could not set replicas desired on fake RC: %s", err)
	}

	defer func() {
		if p := recover(); p != nil {
			t.Fatal("Should not have panicked since everything is fine")
		}
		Assert(t).AreEqual("", alerter.savedInfo.IncidentKey, "should not have had a fired alert")
	}()
	rcf.initialFailsafe()
}

func TestRCsWithZeroCountsWillTriggerIncident(t *testing.T) {
	fakeStore := rcstore.NewFake()
	alerter := &failsafeAlerter{}
	rcf := &Farm{
		alerter: alerter,
		rcStore: fakeStore,
	}

	// replica count is implicitly zero
	_, err := fakeStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", map[string]string{}, nil, "some_strategy")
	if err != nil {
		t.Fatalf("could not put an RC in the fake store: %s", err)
	}

	defer func() {
		if p := recover(); p == nil {
			t.Fatal("Should have panicked due to no replicas")
		}
		Assert(t).AreEqual("zero_replicas_found", alerter.savedInfo.IncidentKey, "should have had a fired alert")
	}()
	rcf.initialFailsafe()
}

func TestHTTPApplicatorImplementsFunctionality(t *testing.T) {
	// assign an http applicator to Labeler to make sure http applicator implements the
	// functionality it needs to
	var rcLabeler Labeler
	var err error
	rcLabeler, err = labels.NewHTTPApplicator(http.DefaultClient, new(url.URL))
	if err != nil {
		t.Fatal(err)
	}
	_ = rcLabeler
}

func testManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("some_pod")
	return builder.GetManifest()
}
