package rollstore

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/pods"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

const (
	testRCId  = rc_fields.ID("abcd-1234")
	testRCId2 = rc_fields.ID("def-456")
)

func TestRollPath(t *testing.T) {
	rollPath, err := RollPath(fields.ID(testRCId))
	if err != nil {
		t.Fatalf("Unable to compute roll path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s", rollTree, testRCId)
	if rollPath != expected {
		t.Errorf("Unexpected value for rollPath, wanted '%s' got '%s'",
			expected,
			rollPath,
		)
	}
}

func TestRollPathErrorNoID(t *testing.T) {
	_, err := RollPath("")
	if err == nil {
		t.Errorf("Expected error computing roll path with no id")
	}
}

func TestRollLockPath(t *testing.T) {
	rollLockPath, err := RollLockPath(fields.ID(testRCId))
	if err != nil {
		t.Fatalf("Unable to compute roll lock path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s/%s", kp.LOCK_TREE, rollTree, testRCId)
	if rollLockPath != expected {
		t.Errorf("Unexpected value for rollLockPath, wanted '%s' got '%s'",
			expected,
			rollLockPath,
		)
	}
}

func TestRollLockPathErrorNoID(t *testing.T) {
	_, err := RollLockPath("")
	if err == nil {
		t.Errorf("Expected error computing roll lock path with no id")
	}
}

type fakeKV struct {
	entries map[string]*api.KVPair
	mu      sync.Mutex
}

func newRollStore(t *testing.T, entries []fields.Update) consulStore {
	storeFields := make(map[string]*api.KVPair)
	for _, u := range entries {
		path, err := RollPath(fields.ID(u.NewRC))
		if err != nil {
			t.Fatalf("Unable to create roll store for test: %s", err)
		}
		json, err := json.Marshal(u)
		if err != nil {
			t.Fatalf("Unable to marshal test field as JSON: %s", err)
		}
		storeFields[path] = &api.KVPair{
			Value: json,
		}
	}
	return consulStore{
		kv: fakeKV{
			entries: storeFields,
		},
		store:   kptest.NewFakePodStore(nil, nil),
		rcstore: rcstore.NewFake(),
		labeler: labels.NewFakeApplicator(),
	}
}

func (f fakeKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.entries[key], nil, nil
}

func (f fakeKV) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ret := make(api.KVPairs, 0)
	for _, v := range f.entries {
		ret = append(ret, v)
	}
	return ret, nil, nil
}

func (f fakeKV) CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if keyPair, ok := f.entries[p.Key]; ok {
		if keyPair.ModifyIndex != p.ModifyIndex {
			return false, nil, util.Errorf("CAS error for %s", p.Key)
		}
	}

	f.entries[p.Key] = p
	return true, nil, nil
}
func (f fakeKV) Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error) {
	return nil, util.Errorf("Not implemented")
}
func (f fakeKV) Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	return false, nil, util.Errorf("Not implemented")
}

func TestGet(t *testing.T) {
	rollstore := newRollStore(t, []fields.Update{testRollValue(testRCId)})

	entry, err := rollstore.Get(fields.ID(testRCId))
	if err != nil {
		t.Fatalf("Unexpected error retrieving roll from roll store: %s", err)
	}

	if entry.NewRC != testRCId {
		t.Errorf("Expected roll to have NewRC of %s, was %s", testRCId, entry.NewRC)
	}
}

func TestList(t *testing.T) {
	entries := []fields.Update{testRollValue(testRCId), testRollValue(testRCId2)}
	rollstore := newRollStore(t, entries)

	rolls, err := rollstore.List()
	if err != nil {
		t.Fatalf("Unexpected error listing rollsfrom roll store: %s", err)
	}

	if len(rolls) != 2 {
		t.Errorf("Expected 2 rolls from list operation, got %d", len(entries))
	}

	var matched bool
	for _, val := range rolls {
		if val.NewRC == testRCId {
			matched = true
		}
	}

	if !matched {
		t.Errorf("Expected to find a roll with NewRC of %s", testRCId)
	}

	matched = false
	for _, val := range rolls {
		if val.NewRC == testRCId2 {
			matched = true
		}

	}
	if !matched {
		t.Errorf("Expected to find a roll with NewRC of %s", testRCId2)
	}
}

func TestCreateRollingUpdateFromExistingRCs(t *testing.T) {
	rollstore := newRollStore(t, nil)

	newRCID := rc_fields.ID("new_rc")
	oldRCID := rc_fields.ID("old_rc")

	update := fields.Update{
		NewRC: newRCID,
		OldRC: oldRCID,
	}

	_, err := rollstore.CreateRollingUpdateFromExistingRCs(update)
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
}

// Test that if a conflicting update exists, a new one will not be admitted
func TestCreateExistingRCsMutualExclusion(t *testing.T) {
	newRCID := rc_fields.ID("new_rc")
	oldRCID := rc_fields.ID("old_rc")

	conflictingEntry := fields.Update{
		OldRC: newRCID,
		NewRC: rc_fields.ID("some_other_rc"),
	}

	rollstore := newRollStore(t, []fields.Update{conflictingEntry})

	update := fields.Update{
		NewRC: newRCID,
		OldRC: oldRCID,
	}

	_, err := rollstore.CreateRollingUpdateFromExistingRCs(update)
	if err == nil {
		t.Fatal("Expected update creation to fail due to conflict")
	}

	ru, _ := rollstore.Get(fields.ID(update.NewRC))
	if ru.NewRC != "" || ru.OldRC != "" {
		t.Fatal("New ru shouldn't have been created but it was")
	}
}

func TestCreateFailsIfCantAcquireLock(t *testing.T) {
	newRCID := rc_fields.ID("new_rc")
	oldRCID := rc_fields.ID("old_rc")

	rollstore := newRollStore(t, nil)

	update := fields.Update{
		NewRC: newRCID,
		OldRC: oldRCID,
	}

	// Grab an update creation lock on one of the RCs and make sure the
	// creation fails
	session, _, err := rollstore.store.NewSession("conflicting session", nil)
	if err != nil {
		t.Fatalf("Unable to create session for conflicting lock: %s", err)
	}
	defer session.Destroy()

	_, err = rollstore.rcstore.LockForUpdateCreation(update.OldRC, session)
	if err != nil {
		t.Fatalf("Unable to acquire conflicting lock on old rc: %s", err)
	}

	_, err = rollstore.CreateRollingUpdateFromExistingRCs(update)
	if err == nil {
		t.Fatal("Expected update creation to fail due to lock conflict")
	}

	ru, _ := rollstore.Get(fields.ID(newRCID))
	if ru.NewRC != "" || ru.OldRC != "" {
		t.Fatal("New ru shouldn't have been created but it was")
	}
}

func TestCreateRollingUpdateFromOneExistingRCWithID(t *testing.T) {
	oldRCID := rc_fields.ID("old_rc")

	rollstore := newRollStore(t, nil)

	newUpdate, err := rollstore.CreateRollingUpdateFromOneExistingRCWithID(
		oldRCID,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
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

	_, err = rollstore.rcstore.Get(newUpdate.NewRC)
	if err != nil {
		t.Fatalf("Shouldn't have failed to fetch new RC: %s", err)
	}
}

func TestCreateRollingUpdateFromOneExistingRCWithIDMutualExclusion(t *testing.T) {
	rollstore := newRollStore(t, nil)

	// create the old RC
	oldRC, err := rollstore.rcstore.Create(testManifest(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create old rc: %s", err)
	}

	_, err = rollstore.CreateRollingUpdateFromOneExistingRCWithID(
		oldRC.ID,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)
	if err != nil {
		t.Fatalf("Unable to create conflicting update: %s", err)
	}

	newUpdate, err := rollstore.CreateRollingUpdateFromOneExistingRCWithID(
		oldRC.ID,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)

	if err == nil {
		t.Fatalf("Should have erred creating conflicting update")
	}

	update, err := rollstore.Get(fields.ID(newUpdate.NewRC))
	if err != nil {
		t.Fatalf("Should not have erred checking for update creation: %s", err)
	}

	if update.NewRC != "" {
		t.Fatalf("Update was created but shouldn't have been: %s", err)
	}

	rcs, err := rollstore.rcstore.List()
	if err != nil {
		t.Fatalf("Shouldn't have failed to list RCs: %s", err)
	}

	if len(rcs) != 2 {
		t.Fatalf("There shouldn't be any new RCs after a failed update: expect 2 but were %d", len(rcs))
	}
}

func TestCreateRollingUpdateFromOneExistingRCWithIDFailsIfCantAcquireLock(t *testing.T) {
	oldRCID := rc_fields.ID("old_rc")

	rollstore := newRollStore(t, nil)

	// Grab an update creation lock on the old RC and make sure the
	// creation fails
	session, _, err := rollstore.store.NewSession("conflicting session", nil)
	if err != nil {
		t.Fatalf("Unable to create session for conflicting lock: %s", err)
	}
	defer session.Destroy()

	_, err = rollstore.rcstore.LockForUpdateCreation(oldRCID, session)
	if err != nil {
		t.Fatalf("Unable to acquire conflicting lock on old rc: %s", err)
	}

	newUpdate, err := rollstore.CreateRollingUpdateFromOneExistingRCWithID(
		oldRCID,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)

	if err == nil {
		t.Fatalf("Should have erred creating conflicting update")
	}

	update, err := rollstore.Get(fields.ID(newUpdate.NewRC))
	if err != nil {
		t.Fatalf("Should nothave erred checking for update creation: %s", err)
	}

	if update.NewRC != "" {
		t.Fatalf("Update was created but shouldn't have been: %s", err)
	}

	rcs, err := rollstore.rcstore.List()
	if err != nil {
		t.Fatalf("Shouldn't have failed to list RCs: %s", err)
	}

	if len(rcs) != 0 {
		t.Fatalf("There shouldn't be any new RCs after a failed update: expect 0 but were %d", len(rcs))
	}
}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorWhenDoesntExist(t *testing.T) {
	rollstore := newRollStore(t, nil)

	// Make a selector that won't match anything
	oldRCSelector := klabels.Everything().
		Add("is_test_rc", klabels.EqualsOperator, []string{"true"})

	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)

	if err != nil {
		t.Fatalf("Shouldn't have failed to create update: %s", err)
	}

	if u.NewRC == "" {
		t.Fatalf("Update shouldn't have been empty")
	}

	_, err = rollstore.rcstore.Get(u.NewRC)
	if err != nil {
		t.Fatalf("Shouldn't have failed to fetch newly created new rc: %s", err)
	}

	_, err = rollstore.rcstore.Get(u.OldRC)
	if err != nil {
		t.Fatalf("Shouldn't have failed to fetch newly created old rc: %s", err)
	}
}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorWhenExists(t *testing.T) {
	rollstore := newRollStore(t, nil)

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

	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
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

	_, err = rollstore.rcstore.Get(u.NewRC)
	if err != nil {
		t.Fatalf("Shouldn't have failed to fetch newly created new rc: %s", err)
	}

}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorFailsWhenTwoMatches(t *testing.T) {
	rollstore := newRollStore(t, nil)

	// Put two RC in the rcstore that matches our label selector
	firstRC, err := rollstore.rcstore.Create(
		testManifest(),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Unable to create first fake rc for test")
	}

	err = rollstore.labeler.SetLabel(labels.RC, string(firstRC.ID), "is_test_rc", "true")
	if err != nil {
		t.Fatalf("Unable to appropriately label first rc for test")
	}

	secondRC, err := rollstore.rcstore.Create(
		testManifest(),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Unable to create second rc for test")
	}

	err = rollstore.labeler.SetLabel(labels.RC, string(secondRC.ID), "is_test_rc", "true")
	if err != nil {
		t.Fatalf("Unable to appropriately label second rc for test")
	}

	oldRCSelector := klabels.Everything().
		Add("is_test_rc", klabels.EqualsOperator, []string{"true"})

	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)

	if err == nil {
		t.Fatal("Should have failed to create update when two RCs match old selector")
	}

	if u.NewRC != "" {
		t.Fatal("Update should have been empty")
	}
}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorFailsWhenExistingIsLocked(t *testing.T) {
	rollstore := newRollStore(t, nil)

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

	// Grab an update creation lock on the old RC and make sure that
	// creation fails
	session, _, err := rollstore.store.NewSession("conflicting session", nil)
	if err != nil {
		t.Fatalf("Unable to create session for conflicting lock: %s", err)
	}
	defer session.Destroy()

	_, err = rollstore.rcstore.LockForUpdateCreation(oldRC.ID, session)
	if err != nil {
		t.Fatalf("Unable to acquire conflicting lock on old rc: %s", err)
	}

	oldRCSelector := klabels.Everything().
		Add("is_test_rc", klabels.EqualsOperator, []string{"true"})

	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)

	if err == nil {
		t.Fatalf("Should have failed to create update due to lock being held")
	}

	if u.NewRC != "" {
		t.Fatalf("Update should have been empty")
	}
}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorFailsWhenConflict(t *testing.T) {
	rollstore := newRollStore(t, nil)

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
	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)

	if err != nil {
		t.Fatalf("Should have succeeded in update creation: %s", err)
	}

	if u.NewRC == "" {
		t.Fatalf("Update shouldn't be empty")
	}

	// Second one should fail
	_, err = rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		oldRCSelector,
		1,
		0,
		false,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)

	if err == nil {
		t.Fatalf("Second update creation should have failed due to using the same old RC")
	}
}

func TestLeaveOldInvalidIfNoOldRC(t *testing.T) {
	rollstore := newRollStore(t, nil)

	// Make a selector that won't match anything
	oldRCSelector := klabels.Everything().
		Add("is_test_rc", klabels.EqualsOperator, []string{"true"})

	_, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		oldRCSelector,
		1,
		0,
		true,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
	)

	if err == nil {
		t.Fatalf("Should have failed to create update due to LeaveOld being set when there's no old RC")
	}
}

func testRollValue(id rc_fields.ID) fields.Update {
	// not a full update, just enough for a smoke test
	return fields.Update{
		NewRC: id,
	}
}

func testManifest() pods.Manifest {
	builder := pods.NewManifestBuilder()
	builder.SetID("slug")
	return builder.GetManifest()
}

func testNodeSelector() klabels.Selector {
	return klabels.Everything().
		Add("some_key", klabels.EqualsOperator, []string{"some_value"})
}
