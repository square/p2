// +build !race

package roll

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/rollstore"
	"github.com/square/p2/pkg/store/consul/transaction"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestAuditLogCreation(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	logger := logging.TestLogger()
	rollStore := rollstore.NewConsul(fixture.Client, applicator, &logger)

	farm := &Farm{
		rls:     rollStore,
		txner:   fixture.Client.KV(),
		labeler: applicator,
		config: FarmConfig{
			ShouldCreateAuditLogRecords: true,
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := transaction.New(context.Background())
		defer cancel()
		farm.mustDeleteRU(ctx, "some_id", logger)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for RU deletion")
	}

	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())
	records, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("expected one audit log record to be created but there were %d", len(records))
	}

	for _, val := range records {
		if val.EventType != audit.RUCompletionEvent {
			t.Fatalf("expected audit log type to be %s but was %s", audit.RUCompletionEvent, val.EventType)
		}
	}
}

func TestCleanupOldRCHappy(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := rcstore.NewConsul(fixture.Client, applicator, 0)

	builder := manifest.NewBuilder()
	builder.SetID("whatever")

	rc, err := rcStore.Create(builder.GetManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}

	update := update{
		Update:  fields.Update{OldRC: rc.ID},
		logger:  logging.TestLogger(),
		rcStore: rcStore,
	}

	quit := make(chan struct{})
	defer close(quit)
	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	update.cleanupOldRC(ctx, quit)
	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	_, err = rcStore.Get(rc.ID)
	switch err {
	case rcstore.NoReplicationController:
		//good
	case nil:
		t.Fatal("expected an error due to missing old RC")
	default:
		t.Fatalf("unexpected error when checking that old RC was deleted: %s", err)
	}
}

// errorOnceChannelAlerter returns an error the first time you call Alert() and
// then does not on subsequent calls. This lets us test code that must run
// until an alert is sent
type errorOnceChannelAlerter struct {
	out     chan<- struct{}
	ranOnce bool
}

func (c errorOnceChannelAlerter) Alert(_ alerting.AlertInfo, _ alerting.Urgency) error {
	c.out <- struct{}{}
	if c.ranOnce {
		return nil
	}
	c.ranOnce = true
	return errors.New("some error")
}

func TestCleanupOldRCTooManyReplicas(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := rcstore.NewConsul(fixture.Client, applicator, 0)

	builder := manifest.NewBuilder()
	builder.SetID("whatever")

	rc, err := rcStore.Create(builder.GetManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}
	err = rcStore.SetDesiredReplicas(rc.ID, 2)
	if err != nil {
		t.Fatal(err)
	}

	alertOut := make(chan struct{})
	defer close(alertOut)

	update := update{
		Update:  fields.Update{OldRC: rc.ID},
		logger:  logging.TestLogger(),
		rcStore: rcStore,
		alerter: errorOnceChannelAlerter{out: alertOut},
	}

	quit := make(chan struct{})

	errCh := make(chan error)
	go func() {
		ctx, cancel := transaction.New(context.Background())
		defer cancel()
		update.cleanupOldRC(ctx, quit)
		errCh <- transaction.MustCommit(ctx, fixture.Client.KV())
	}()

	// the first attempt will error so make sure there are two attempts
	<-alertOut
	<-alertOut
	close(quit)

	err = <-errCh
	if err != nil {
		t.Fatalf("failed to delete the old RC: %s", err)
	}
	close(errCh)

	_, err = rcStore.Get(rc.ID)
	switch err {
	case rcstore.NoReplicationController:
		t.Fatal("rc was deleted even though it had some replicas desired")
	case nil:
		// good
	default:
		t.Fatalf("unexpected error when checking that old RC was not deleted: %s", err)
	}
}
