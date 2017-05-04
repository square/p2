// +build !race

package rollstore

import (
	"encoding/json"
	"testing"

	"github.com/square/p2/pkg/labels"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/transaction"

	"github.com/hashicorp/consul/api"
	klabels "k8s.io/kubernetes/pkg/labels"
)

// The tests in this file use transactions, and thus need a real consul
// instance to run against.
func TestCreateRollingUpdateFromExistingRCs(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	rollstore, _ := newRollStoreWithRealConsul(t, fixture, nil)

	newRCID := rc_fields.ID("new_rc")
	oldRCID := rc_fields.ID("old_rc")

	update := fields.Update{
		NewRC: newRCID,
		OldRC: oldRCID,
	}

	newRCLabels := klabels.Set(map[string]string{
		"some_key": "some_val",
	})

	txn := transaction.New()
	u, err := rollstore.CreateRollingUpdateFromExistingRCs(txn, update, newRCLabels, newRCLabels)
	if err != nil {
		t.Fatalf("Unexpected error creating update: %s", err)
	}

	storedUpdate, err := rollstore.Get(update.ID())
	if err != nil {
		t.Fatalf("Unable to retrieve value put in roll store: %s", err)
	}

	if storedUpdate.NewRC != newRCID {
		t.Errorf("Stored update didn't have expected new rc value: wanted '%s' but got '%s'", newRCID, storedUpdate.NewRC)
	}

	if storedUpdate.OldRC != oldRCID {
		t.Errorf("Stored update didn't have expected old rc value: wanted '%s' but got '%s'", oldRCID, storedUpdate.OldRC)
	}

	rcLabels, err := rollstore.labeler.GetLabels(labels.RC, newRCID.String())
	if err != nil {
		t.Fatalf("Unable to fetch labels for newly created new RC: %s", err)
	}

	if rcLabels.Labels["some_key"] != "some_val" {
		t.Errorf("Expected labels to be set on new RC")
	}

	ruLabels, err := rollstore.labeler.GetLabels(labels.RU, u.ID().String())
	if err != nil {
		t.Fatalf("Unable to fetch labels for newly created new RU: %s", err)
	}

	if ruLabels.Labels["some_key"] != "some_val" {
		t.Errorf("Expected labels to be set on new RU")
	}
}

func TestCreateRollingUpdateFromOneExistingRCWithID(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	oldRCID := rc_fields.ID("old_rc")

	rollstore, rcStore := newRollStoreWithRealConsul(t, fixture, nil)

	newRCLabels := klabels.Set(map[string]string{
		"some_key": "some_val",
	})

	txn := transaction.New()
	newUpdate, err := rollstore.CreateRollingUpdateFromOneExistingRCWithID(
		txn,
		oldRCID,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
		newRCLabels,
		newRCLabels,
	)
	if err != nil {
		t.Fatalf("Unable to create rolling update: %s", err)
	}

	storedUpdate, err := rollstore.Get(fields.ID(newUpdate.NewRC))
	if err != nil {
		t.Fatalf("Unable to retrieve value put in roll store: %s", err)
	}

	if storedUpdate.NewRC != newUpdate.NewRC {
		t.Errorf("Stored update didn't have expected new rc value: wanted '%s' but got '%s'", newUpdate.NewRC, storedUpdate.NewRC)
	}

	if storedUpdate.OldRC != oldRCID {
		t.Errorf("Stored update didn't have expected old rc value: wanted '%s' but got '%s'", oldRCID, storedUpdate.OldRC)
	}

	_, err = rcStore.Get(newUpdate.NewRC)
	if err != nil {
		t.Fatalf("Shouldn't have failed to fetch new RC: %s", err)
	}

	rcLabels, err := rollstore.labeler.GetLabels(labels.RC, storedUpdate.NewRC.String())
	if err != nil {
		t.Fatalf("Unable to fetch labels for newly created new RC: %s", err)
	}

	if rcLabels.Labels["some_key"] != "some_val" {
		t.Errorf("Expected labels to be set on new RC")
	}

	ruLabels, err := rollstore.labeler.GetLabels(labels.RU, newUpdate.ID().String())
	if err != nil {
		t.Fatalf("Unable to fetch labels for newly created new RU: %s", err)
	}

	if ruLabels.Labels["some_key"] != "some_val" {
		t.Errorf("Expected labels to be set on new RU")
	}
}

func TestCreateRollingUpdateFromOneExistingRCWithIDMutualExclusion(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	rollstore, rcStore := newRollStoreWithRealConsul(t, fixture, nil)

	// create the old RC
	oldRC, err := rollstore.rcstore.Create(testManifest(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create old rc: %s", err)
	}

	txn := transaction.New()
	conflictingEntry, err := rollstore.CreateRollingUpdateFromOneExistingRCWithID(
		txn,
		oldRC.ID,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Unable to create conflicting update: %s", err)
	}

	txn = transaction.New()
	newUpdate, err := rollstore.CreateRollingUpdateFromOneExistingRCWithID(
		txn,
		oldRC.ID,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("Should have erred creating conflicting update")
	}

	if conflictingErr, ok := err.(*ConflictingRUError); !ok {
		t.Errorf("Returned error didn't have ConflictingRUError type, was %s", err)
	} else {
		if conflictingErr.ConflictingID != conflictingEntry.ID() {
			t.Errorf("Expected error to have conflicting ID of '%s', was '%s'", conflictingEntry.ID(), conflictingErr.ConflictingID)
		}

		if conflictingErr.ConflictingRCID != conflictingEntry.OldRC {
			t.Errorf("Expected error to have conflicting rc ID of '%s', was '%s'", conflictingEntry.OldRC, conflictingErr.ConflictingRCID)
		}
	}

	update, err := rollstore.Get(fields.ID(newUpdate.NewRC))
	if err != nil {
		t.Fatalf("Should not have erred checking for update creation: %s", err)
	}

	if update.NewRC != "" {
		t.Fatalf("Update was created but shouldn't have been: %s", err)
	}

	rcs, err := rcStore.List()
	if err != nil {
		t.Fatalf("Shouldn't have failed to list RCs: %s", err)
	}

	if len(rcs) != 2 {
		t.Fatalf("There shouldn't be any new RCs after a failed update: expect 2 but were %d", len(rcs))
	}
}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorWhenDoesntExist(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	rollstore, rcStore := newRollStoreWithRealConsul(t, fixture, nil)

	// Make a selector that won't match anything
	oldRCSelector := klabels.Everything().
		Add("is_test_rc", klabels.EqualsOperator, []string{"true"})

	newRCLabels := klabels.Set(map[string]string{
		"some_key": "some_val",
	})

	txn := transaction.New()
	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		txn,
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
		newRCLabels,
		newRCLabels,
	)
	if err != nil {
		t.Fatalf("Shouldn't have failed to create update: %s", err)
	}

	if u.NewRC == "" {
		t.Fatalf("Update shouldn't have been empty")
	}

	_, err = rcStore.Get(u.NewRC)
	if err != nil {
		t.Fatalf("Shouldn't have failed to fetch newly created new rc: %s", err)
	}

	_, err = rcStore.Get(u.OldRC)
	if err != nil {
		t.Fatalf("Shouldn't have failed to fetch newly created old rc: %s", err)
	}

	rcLabels, err := rollstore.labeler.GetLabels(labels.RC, u.NewRC.String())
	if err != nil {
		t.Fatalf("Unable to fetch labels for newly created new RC: %s", err)
	}

	if rcLabels.Labels["some_key"] != "some_val" {
		t.Errorf("Expected labels to be set on new RC")
	}

	rcLabels, err = rollstore.labeler.GetLabels(labels.RC, u.OldRC.String())
	if err != nil {
		t.Fatalf("Unable to fetch labels for newly created old RC: %s", err)
	}
	if rcLabels.Labels["some_key"] != "some_val" {
		t.Errorf("Expected labels to be set on old RC")
	}

	ruLabels, err := rollstore.labeler.GetLabels(labels.RU, u.ID().String())
	if err != nil {
		t.Fatalf("Unable to fetch labels for newly created new RU: %s", err)
	}

	if ruLabels.Labels["some_key"] != "some_val" {
		t.Errorf("Expected labels to be set on new RU")
	}
}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorWhenExists(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	rollstore, rcStore := newRollStoreWithRealConsul(t, fixture, nil)

	// Put an RC in the rcstore that matches our label selector
	oldRC, err := rollstore.rcstore.Create(
		testManifest(),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Unable to create fake rc for test")
	}

	err = rollstore.labeler.SetLabel(labels.RC, string(oldRC.ID), "is_test_rc", "true")
	if err != nil {
		t.Fatalf("Unable to appropriately label old rc for test")
	}

	oldRCSelector := klabels.Everything().
		Add("is_test_rc", klabels.EqualsOperator, []string{"true"})

	txn := transaction.New()
	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		txn,
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
		nil,
		nil,
	)

	if err != nil {
		t.Fatalf("Shouldn't have failed to create update: %s", err)
	}

	if u.NewRC == "" {
		t.Fatalf("Update shouldn't have been empty")
	}

	if u.OldRC != oldRC.ID {
		t.Errorf("Created update didn't have expected old rc ID, wanted '%s' but got '%s'", oldRC.ID, u.OldRC)
	}

	_, err = rcStore.Get(u.NewRC)
	if err != nil {
		t.Fatalf("Shouldn't have failed to fetch newly created new rc: %s", err)
	}

}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorFailsWhenConflict(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	rollstore, _ := newRollStoreWithRealConsul(t, fixture, nil)

	// Put an RC in the rcstore that matches our label selector
	oldRC, err := rollstore.rcstore.Create(
		testManifest(),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Unable to create fake rc for test")
	}

	err = rollstore.labeler.SetLabel(labels.RC, string(oldRC.ID), "is_test_rc", "true")
	if err != nil {
		t.Fatalf("Unable to appropriately label old rc for test")
	}

	oldRCSelector := klabels.Everything().
		Add("is_test_rc", klabels.EqualsOperator, []string{"true"})

	// First one should succeed
	txn := transaction.New()
	conflictingEntry, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		txn,
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Should have succeeded in update creation: %s", err)
	}

	if conflictingEntry.NewRC == "" {
		t.Fatalf("Update shouldn't be empty")
	}

	// Second one should fail
	txn2 := transaction.New()
	_, err = rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		txn2,
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("Second update creation should have failed due to using the same old RC")
	}

	if conflictingErr, ok := err.(*ConflictingRUError); !ok {
		t.Error("Returned error didn't have ConflictingRUError type")
	} else {
		if conflictingErr.ConflictingID != conflictingEntry.ID() {
			t.Errorf("Expected error to have conflicting ID of '%s', was '%s'", conflictingEntry.ID(), conflictingErr.ConflictingID)
		}

		if conflictingErr.ConflictingRCID != conflictingEntry.OldRC {
			t.Errorf("Expected error to have conflicting rc ID of '%s', was '%s'", conflictingEntry.OldRC, conflictingErr.ConflictingRCID)
		}
	}

}

func newRollStoreWithRealConsul(t *testing.T, fixture consulutil.Fixture, entries []fields.Update) (*ConsulStore, testRCStore) {
	for _, u := range entries {
		path, err := RollPath(fields.ID(u.NewRC))
		if err != nil {
			t.Fatalf("Unable to create roll store for test: %s", err)
		}
		json, err := json.Marshal(u)
		if err != nil {
			t.Fatalf("Unable to marshal test field as JSON: %s", err)
		}
		pair := &api.KVPair{
			Key:   path,
			Value: json,
		}

		_, err = fixture.Client.KV().Put(pair, nil)
		if err != nil {
			t.Fatalf("could not seed consul with RC: %s", err)
		}
	}
	applicator := labels.NewConsulApplicator(fixture.Client, 0)
	rcStore := rcstore.NewConsul(fixture.Client, applicator, 0)
	return &ConsulStore{
		kv:      fixture.Client.KV(),
		store:   consul.NewConsulStore(fixture.Client),
		rcstore: rcStore,
		labeler: applicator,
	}, rcStore
}
