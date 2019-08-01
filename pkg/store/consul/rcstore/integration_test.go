// +build !race

package rcstore

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	rcfields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"

	"github.com/gofrs/uuid"
	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestCreateTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	store := NewConsul(fixture.Client, applicator, 0)

	rcLabelsToSet := klabels.Set{
		"some_rc_label": "some_rc_value",
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	rc, err := store.CreateTxn(ctx, testManifest(), klabels.Everything(), "some_az", "some_cn", nil, rcLabelsToSet, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}

	// make sure neither the RC nor its labels were created since we haven't committed the transaction yet
	_, err = store.Get(rc.ID)
	switch {
	case err == nil:
		t.Fatal("expected the RC to not be created before transaction is committed")
	case err == NoReplicationController:
	case err != nil:
		t.Fatalf("unexpected error checking if RC was created (it shouldn't be) %s", err)
	}

	rcLabels, err := applicator.GetLabels(labels.RC, rc.ID.String())
	if err != nil {
		t.Fatalf("unexpected error ensuring RC labels don't exist before committing the transaction: %s", err)
	}

	if len(rcLabels.Labels) != 0 {
		t.Fatalf("expected there to be no labels for the RC before the transaction is committed")
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("unexpected error committing RC creation transaction: %s", err)
	}

	_, err = store.Get(rc.ID)
	if err != nil {
		t.Fatalf("unexpected error fetching RC after committing transaction: %s", err)
	}

	rcLabels, err = applicator.GetLabels(labels.RC, rc.ID.String())
	if err != nil {
		t.Fatalf("unexpected error ensuring RC labels were created after committing the transaction: %s", err)
	}

	if len(rcLabels.Labels) != 2 {
		t.Errorf("expected there to be only %q and %q labels but found %d total: %s", PodIDLabel, "some_rc_label", len(rcLabels.Labels), rcLabels.Labels)
	}

	podIDLabel := rcLabels.Labels[PodIDLabel]
	if podIDLabel != testManifest().ID().String() {
		t.Errorf("expected value for label key %q to be %q but was %q", PodIDLabel, testManifest().ID().String(), podIDLabel)
	}

	extraLabel := rcLabels.Labels["some_rc_label"]
	if extraLabel != "some_rc_value" {
		t.Errorf("expected value for label key %q to be %q but was %q", "some_rc_label", "some_rc_value", extraLabel)
	}
}

func TestDeleteTxnHappy(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := NewConsul(fixture.Client, applicator, 0)

	rc, err := rcStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	err = rcStore.DeleteTxn(ctx, rc.ID, false)
	if err != nil {
		t.Fatal(err)
	}

	// confirm the RC has not been deleted yet because the transaction wasn't yet committed
	rc, err = rcStore.Get(rc.ID)
	switch {
	case err == NoReplicationController:
		t.Fatal("the RC was deleted before the transaction was committed")
	case err != nil:
		t.Fatalf("unexpected error checking that RC wasn't deleted yet: %s", err)
	}

	// confirm the labels weren't removed yet either
	rcLabels, err := applicator.GetLabels(labels.RC, rc.ID.String())
	if err != nil {
		t.Fatal(err)
	}

	if len(rcLabels.Labels) == 0 {
		t.Fatal("the RCs labels were removed before the transaction was committed")
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	_, err = rcStore.Get(rc.ID)
	switch {
	case err == NoReplicationController:
		// good
	case err != nil:
		t.Fatalf("unexpected error checking that RC was deleted: %s", err)
	case err == nil:
		t.Fatal("RC should have been deleted")
	}

	// confirm the labels werer removed
	rcLabels, err = applicator.GetLabels(labels.RC, rc.ID.String())
	if err != nil {
		t.Fatal(err)
	}

	if len(rcLabels.Labels) != 0 {
		t.Fatal("the RCs labels were not removed")
	}

}

func TestDeleteTxnNonzeroReplicaCount(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := NewConsul(fixture.Client, applicator, 0)

	rc, err := rcStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}

	err = rcStore.SetDesiredReplicas(rc.ID, 1)
	if err != nil {
		t.Fatalf("could not set replicas desired to a nonzero value to test that it can't be deleted without force")
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	err = rcStore.DeleteTxn(ctx, rc.ID, false)
	if err == nil {
		t.Error("expected an error trying to delete an RC with a nonzero replica count without the force flag set")
	}

	err = rcStore.DeleteTxn(ctx, rc.ID, true)
	if err != nil {
		t.Errorf("unexpected error trying to delete an RC with a nonzero replica count when the force flag is set: %s", err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	rc, err = rcStore.Get(rc.ID)
	switch {
	case err == NoReplicationController:
		// good
	case err != nil:
		t.Fatalf("unexpected error checking that RC was deleted: %s", err)
	case err == nil:
		t.Fatal("RC should have been deleted")
	}
}

func TestTxnFailsIfRCChanged(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := NewConsul(fixture.Client, applicator, 0)

	rc, err := rcStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	err = rcStore.DeleteTxn(ctx, rc.ID, false)
	if err != nil {
		t.Fatal(err)
	}

	// now mutate the RC to make sure that the transaction will fail
	err = rcStore.SetDesiredReplicas(rc.ID, 1)
	if err != nil {
		t.Fatalf("could not mutate the RC to test transaction forming: %s", err)
	}

	ok, _, err := transaction.Commit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected an error committing RC deletion transaction when the RC changed since the delete operation was added to the transaction")
	}
}

func TestDisableTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := NewConsul(fixture.Client, applicator, 0)

	rc, err := rcStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	err = rcStore.DisableTxn(ctx, rc.ID)
	if err != nil {
		t.Fatal(err)
	}

	rc, err = rcStore.Get(rc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Disabled {
		t.Fatal("rc was disabled before transaction was committed")
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	rc, err = rcStore.Get(rc.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !rc.Disabled {
		t.Fatal("rc should have been disabled but wasn't")
	}
}

func TestDisableTxnFailsIfChanged(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := NewConsul(fixture.Client, applicator, 0)

	rc, err := rcStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	err = rcStore.DisableTxn(ctx, rc.ID)
	if err != nil {
		t.Fatal(err)
	}

	err = rcStore.SetDesiredReplicas(rc.ID, 5)
	if err != nil {
		t.Fatal(err)
	}

	ok, _, err := transaction.Commit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("transaction should have failed since RC was changed before transaction was committed")
	}
}

func TestEnableTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := NewConsul(fixture.Client, applicator, 0)

	rc, err := rcStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		t.Fatal(err)
	}

	err = rcStore.Disable(rc.ID)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	err = rcStore.EnableTxn(ctx, rc.ID)
	if err != nil {
		t.Fatal(err)
	}

	rc, err = rcStore.Get(rc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !rc.Disabled {
		t.Fatal("rc was enabled before transaction was committed")
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	rc, err = rcStore.Get(rc.ID)
	if err != nil {
		t.Fatal(err)
	}

	if rc.Disabled {
		t.Fatal("rc should have been enabled but wasn't")
	}
}

func TestEnableTxnFailsIfChanged(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := NewConsul(fixture.Client, applicator, 0)

	rc, err := rcStore.Create(testManifest(), klabels.Everything(), "some_az", "some_cn", nil, nil, "strategy")
	if err != nil {
		t.Fatal(err)
	}

	err = rcStore.Disable(rc.ID)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	err = rcStore.EnableTxn(ctx, rc.ID)
	if err != nil {
		t.Fatal(err)
	}

	err = rcStore.SetDesiredReplicas(rc.ID, 5)
	if err != nil {
		t.Fatal(err)
	}

	ok, _, err := transaction.Commit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("transaction should have failed since RC was changed before transaction was committed")
	}
}

func TestLockForNodeTransfer(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	store := consul.NewConsulStore(fixture.Client)
	session1, renewalErrCh, err := store.NewSession("session1", nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := <-renewalErrCh
		if err != nil {
			t.Fatal(err)
		}
	}()

	session2, renewalErrCh2, err := store.NewSession("session1", nil)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := <-renewalErrCh2
		if err != nil {
			t.Fatal(err)
		}
	}()

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)
	rcStore := NewConsul(fixture.Client, applicator, 0)

	rcID := rcfields.ID(uuid.Must(uuid.NewV4()).String())
	unlocker, err := rcStore.LockForNodeTransfer(rcID, session1)
	if err != nil {
		t.Fatal(err)
	}

	expectedLockPath := fmt.Sprintf("lock/replication_controllers/%s/node_transfer", rcID)
	if unlocker.Key() != expectedLockPath {
		t.Fatalf("expected lock path to be %q but was %q", expectedLockPath, unlocker.Key())
	}

	_, err = rcStore.LockForNodeTransfer(rcID, session2)
	if err == nil {
		t.Fatal("expected an error locking the same key with a different session")
	}

	err = unlocker.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	_, err = rcStore.LockForNodeTransfer(rcID, session2)
	if err != nil {
		t.Fatalf("unexpected error locking the key with another session after the first session released it: %s", err)
	}

	err = session1.Destroy()
	if err != nil {
		t.Fatal(err)
	}
	err = session2.Destroy()
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func testManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("some_pod_id")
	return builder.GetManifest()
}
