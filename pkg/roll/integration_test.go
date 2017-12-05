// +build !race

package roll

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/alerting/alertingtest"
	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul"
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

	err := applicator.SetLabel(labels.POD, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}

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
		labeler: applicator,
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	update.cleanupOldRC(ctx)
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

func TestCleanupOldRCNotZeroed(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := rcstore.NewConsul(fixture.Client, applicator, 0)

	err := applicator.SetLabel(labels.POD, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}

	builder := manifest.NewBuilder()
	builder.SetID("whatever")

	rc, err := rcStore.Create(
		builder.GetManifest(),
		klabels.Everything(),
		"some_az",
		"some_cn",
		nil,
		nil,
		"some_strategy",
	)
	if err != nil {
		t.Fatal(err)
	}

	// make the RC not have zero replicas
	err = rcStore.SetDesiredReplicas(rc.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	alerter := alertingtest.NewRecorder()
	update := update{
		Update:  fields.Update{OldRC: rc.ID},
		logger:  logging.TestLogger(),
		rcStore: rcStore,
		alerter: alerter,
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()

	update.cleanupOldRC(ctx)

	if len(alerter.Alerts) != 1 {
		t.Fatal("expected an alert to fire if the old RC isn't zeroed out")
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	_, err = rcStore.Get(rc.ID)
	switch err {
	case rcstore.NoReplicationController:
		t.Fatal("old RC was deleted even though it wasn't zeroed out")
	case nil:
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

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	errCh := make(chan error)
	go func() {
		update.cleanupOldRC(ctx)
		errCh <- transaction.MustCommit(ctx, fixture.Client.KV())
	}()

	// the first attempt will error so make sure there are two attempts
	<-alertOut
	<-alertOut
	cancel()

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

func TestLockRCs(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	session, renewalErrCh, err := consul.NewSession(fixture.Client, "test-lock-rcs", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Destroy()

	go func() {
		for {
			err, ok := <-renewalErrCh
			if !ok {
				return
			}

			if err != nil {
				t.Error(err)
			}
		}
	}()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	update := NewUpdate(fields.Update{
		NewRC: rc_fields.ID("new_rc"),
		OldRC: rc_fields.ID("old_rc"),
	},
		nil,
		nil,
		rcstore.NewConsul(fixture.Client, applicator, 0),
		nil,
		nil,
		fixture.Client.KV(),
		nil,
		nil,
		nil,
		logging.DefaultLogger,
		session,
		0,
		nil,
		nil,
	).(*update)
	lockCtx, lockCancel := transaction.New(context.Background())
	defer lockCancel()
	unlockCtx, unlockCancel := transaction.New(context.Background())
	defer unlockCancel()
	checkLockedCtx, checkLockedCancel := transaction.New(context.Background())
	defer checkLockedCancel()

	err = update.lockRCs(lockCtx, unlockCtx, checkLockedCtx, session)
	if err != nil {
		t.Errorf("unexpected error building RC locking transactions: %s", err)
	}

	// confirm we haven't locked yet (because we haven't committed) by trying the checkLockedCtx
	err = transaction.MustCommit(checkLockedCtx, fixture.Client.KV())
	if err == nil {
		t.Fatal("expected checkLockCtx to fail before locking RCs")
	}
	// refresh the transaction so we can try it again
	checkLockedCtx, checkLockedCancel = transaction.New(checkLockedCtx)
	defer checkLockedCancel()

	err = transaction.MustCommit(lockCtx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit RC locking transaction: %s", err)
	}

	err = transaction.MustCommit(checkLockedCtx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("expected checkLockCtx to succeed after locking RCs: %s", err)
	}
	// refresh the transaction so we can try it again
	checkLockedCtx, checkLockedCancel = transaction.New(checkLockedCtx)
	defer checkLockedCancel()

	err = transaction.MustCommit(unlockCtx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("unexpected error unlocking RCs: %s", err)
	}

	err = transaction.MustCommit(checkLockedCtx, fixture.Client.KV())
	if err == nil {
		t.Fatal("expected checkLockedCtx to fail after unlocking RCs")
	}
}
