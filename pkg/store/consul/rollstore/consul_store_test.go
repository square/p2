package rollstore

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consultest"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/transaction"

	"github.com/hashicorp/consul/api"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	testRCId  = rc_fields.ID("abcd-1234")
	testRCId2 = rc_fields.ID("def-456")
)

func TestNewConsul(t *testing.T) {
	rollstore := NewConsul(consul.NewConsulClient(consul.Options{}), labels.NewFakeApplicator(), nil)
	if rollstore.kv == nil {
		t.Fatal("kv should not be nil for constructed rollstore")
	}

	if rollstore.rcstore == nil {
		t.Fatal("rcstore should not be nil for constructed rollstore")
	}

	if rollstore.labeler == nil {
		t.Fatal("labeler should not be nil for constructed rollstore")
	}

	if rollstore.store == nil {
		t.Fatal("store should not be nil for constructed rollstore")
	}
}

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

	expected := fmt.Sprintf("%s/%s/%s", consulutil.LOCK_TREE, rollTree, testRCId)
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

type testRCStore interface {
	Get(id rc_fields.ID) (rc_fields.RC, error)
	List() ([]rc_fields.RC, error)
}

func newRollStoreWithFakeConsul(t *testing.T, entries []fields.Update) (ConsulStore, testRCStore) {
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
			Key:   path,
			Value: json,
		}
	}
	rcStore := rcstore.NewFake()
	return ConsulStore{
		kv: &consulutil.FakeKV{
			Entries: storeFields,
		},
		store:   consultest.NewFakePodStore(nil, nil),
		rcstore: rcStore,
		labeler: labels.NewFakeApplicator(),
	}, rcStore
}

func TestGet(t *testing.T) {
	rollstore, _ := newRollStoreWithFakeConsul(t, []fields.Update{testRollValue(testRCId)})

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
	rollstore, _ := newRollStoreWithFakeConsul(t, entries)

	rolls, err := rollstore.List()
	if err != nil {
		t.Fatalf("Unexpected error listing rollsfrom roll store: %s", err)
	}

	if len(rolls) != 2 {
		t.Errorf("Expected 2 rolls from list operation, got %d", len(rolls))
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

// Test that if a conflicting update exists, a new one will not be admitted
func TestCreateExistingRCsMutualExclusion(t *testing.T) {
	newRCID := rc_fields.ID("new_rc")
	oldRCID := rc_fields.ID("old_rc")

	conflictingEntry := fields.Update{
		OldRC: newRCID,
		NewRC: rc_fields.ID("some_other_rc"),
	}

	rollstore, _ := newRollStoreWithFakeConsul(t, []fields.Update{conflictingEntry})

	update := fields.Update{
		NewRC: newRCID,
		OldRC: oldRCID,
	}

	ctx, _ := transaction.New(context.Background())
	_, err := rollstore.CreateRollingUpdateFromExistingRCs(ctx, update, nil, nil)
	if err == nil {
		t.Fatal("Expected update creation to fail due to conflict")
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

	ru, _ := rollstore.Get(fields.ID(update.NewRC))
	if ru.NewRC != "" || ru.OldRC != "" {
		t.Fatal("New ru shouldn't have been created but it was")
	}
}

func TestCreateFailsIfCantAcquireLock(t *testing.T) {
	newRCID := rc_fields.ID("new_rc")
	oldRCID := rc_fields.ID("old_rc")

	rollstore, _ := newRollStoreWithFakeConsul(t, nil)

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

	ctx, _ := transaction.New(context.Background())
	_, err = rollstore.CreateRollingUpdateFromExistingRCs(ctx, update, nil, nil)
	if err == nil {
		t.Fatal("Expected update creation to fail due to lock conflict")
	}

	ru, _ := rollstore.Get(fields.ID(newRCID))
	if ru.NewRC != "" || ru.OldRC != "" {
		t.Fatal("New ru shouldn't have been created but it was")
	}
}

func TestCreateRollingUpdateFromOneExistingRCWithIDFailsIfCantAcquireLock(t *testing.T) {
	oldRCID := rc_fields.ID("old_rc")

	rollstore, rcStore := newRollStoreWithFakeConsul(t, nil)

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

	ctx, _ := transaction.New(context.Background())
	newUpdate, err := rollstore.CreateRollingUpdateFromOneExistingRCWithID(
		ctx,
		oldRCID,
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
		t.Fatalf("Should have erred creating conflicting update")
	}

	update, err := rollstore.Get(fields.ID(newUpdate.NewRC))
	if err != nil {
		t.Fatalf("Should nothave erred checking for update creation: %s", err)
	}

	if update.NewRC != "" {
		t.Fatalf("Update was created but shouldn't have been: %s", err)
	}

	rcs, err := rcStore.List()
	if err != nil {
		t.Fatalf("Shouldn't have failed to list RCs: %s", err)
	}

	if len(rcs) != 0 {
		t.Fatalf("There shouldn't be any new RCs after a failed update: expect 0 but were %d", len(rcs))
	}
}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorFailsWhenTwoMatches(t *testing.T) {
	rollstore, _ := newRollStoreWithFakeConsul(t, nil)

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

	ctx, _ := transaction.New(context.Background())
	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		ctx,
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
		t.Fatal("Should have failed to create update when two RCs match old selector")
	}

	if u.NewRC != "" {
		t.Fatal("Update should have been empty")
	}
}

func TestCreateRollingUpdateFromOneMaybeExistingWithLabelSelectorFailsWhenExistingIsLocked(t *testing.T) {
	rollstore, _ := newRollStoreWithFakeConsul(t, nil)

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

	ctx, _ := transaction.New(context.Background())
	u, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		ctx,
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
		t.Fatalf("Should have failed to create update due to lock being held")
	}

	if u.NewRC != "" {
		t.Fatalf("Update should have been empty")
	}
}

func TestLeaveOldInvalidIfNoOldRC(t *testing.T) {
	rollstore, _ := newRollStoreWithFakeConsul(t, nil)

	// Make a selector that won't match anything
	oldRCSelector := klabels.Everything().
		Add("is_test_rc", klabels.EqualsOperator, []string{"true"})

	ctx, _ := transaction.New(context.Background())
	_, err := rollstore.CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		ctx,
		oldRCSelector,
		1,
		0,
		true,
		0,
		testManifest(),
		testNodeSelector(),
		nil,
		nil,
		nil,
	)

	if err == nil {
		t.Fatalf("Should have failed to create update due to LeaveOld being set when there's no old RC")
	}
}

func TestPublishLatestRolls(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRolls(inCh, quitCh)
	go func() {
		select {
		case <-quitCh:
		case err, ok := <-errCh:
			if !ok {
				return
			}
			t.Fatalf("Unexpected error on errCh: %s", err)
		}
	}()

	var val []fields.Update
	// Put some values on the inCh and read them from outCh transformed
	// into RCs
	inCh <- rollsWithIDs(t, "a", 3)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, ru := range val {
		if ru.ID().String() != "a" {
			t.Errorf("Expected all RUs to have id %s, was %s", "a", ru.ID())
		}
	}

	inCh <- rollsWithIDs(t, "b", 2)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 2 {
		t.Errorf("Expected %d values on outCh, got %d", 2, len(val))
	}

	for _, ru := range val {
		if ru.ID().String() != "b" {
			t.Errorf("Expected all RUs to have id %s, was %s", "b", ru.ID())
		}
	}

	// Now, let's put some stuff on inCh but not read it for a bit
	inCh <- rollsWithIDs(t, "c", 4)
	inCh <- rollsWithIDs(t, "d", 5)
	inCh <- rollsWithIDs(t, "e", 6)

	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 6 {
		t.Errorf("Expected %d values on outCh, got %d", 6, len(val))
	}

	for _, ru := range val {
		if ru.ID().String() != "e" {
			t.Errorf("Expected all RUs to have id %s, was %s", "e", ru.ID())
		}
	}
}

func TestPublishLatestRCsSkipsIfCorrupt(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRolls(inCh, quitCh)

	// push some legitimate RCs and read them out
	var val []fields.Update
	inCh <- rollsWithIDs(t, "a", 3)
	select {
	case val = <-outCh:
	case err := <-errCh:
		t.Fatalf("Unexpected error on errCh: %s", err)
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, ru := range val {
		if ru.ID().String() != "a" {
			t.Errorf("Expected all RUs to have id %s, was %s", "a", ru.ID())
		}
	}

	// Now push some bogus JSON that will trigger an error
	corruptData := []*api.KVPair{{Value: []byte("bad_json")}}
	inCh <- corruptData

	select {
	case val = <-outCh:
		t.Fatalf("Didn't expect out value for bogus input")
	case <-errCh:
		// good
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out reading from channel")
	}

	// Now push more legitimate stuff and make sure that is written
	// push some legitimate RUs and read them out
	inCh <- rollsWithIDs(t, "b", 3)
	select {
	case val = <-outCh:
	case err := <-errCh:
		t.Fatalf("Unexpected error on errCh: %s", err)
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, ru := range val {
		if ru.ID().String() != "b" {
			t.Errorf("Expected all RUs to have id %s, was %s", "b", ru.ID())
		}
	}
}

func TestPublishQuitsOnQuitChannelClose(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})

	outCh, errCh := publishLatestRolls(inCh, quitCh)
	close(quitCh)

	select {
	case _, ok := <-outCh:
		if ok {
			t.Fatalf("outCh should have closed since quitCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for outCh to close")
	}

	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatalf("errCh should have closed since quitCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for errCh to close")
	}
}

func TestPublishQuitsOnInChannelCloseBeforeData(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRolls(inCh, quitCh)
	close(inCh)

	select {
	case _, ok := <-outCh:
		if ok {
			t.Fatalf("outCh should have closed since inCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for outCh to close")
	}

	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatalf("errCh should have closed since inCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for errCh to close")
	}
}

func TestPublishQuitsOnInChannelCloseAfterData(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRolls(inCh, quitCh)

	// Write some legitimate data and read it out
	var val []fields.Update
	// Put some values on the inCh and read them from outCh transformed
	// into RUs
	inCh <- rollsWithIDs(t, "a", 3)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, ru := range val {
		if ru.ID().String() != "a" {
			t.Errorf("Expected all RUs to have id %s, was %s", "a", ru.ID())
		}
	}

	close(inCh)

	select {
	case _, ok := <-outCh:
		if ok {
			t.Fatalf("outCh should have closed since inCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for outCh to close")
	}

	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatalf("errCh should have closed since inCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for errCh to close")
	}
}

func rollsWithIDs(t *testing.T, id string, num int) api.KVPairs {
	var pairs api.KVPairs
	for i := 0; i < num; i++ {
		ru := fields.Update{
			NewRC: rc_fields.ID(id),
		}

		jsonRU, err := json.Marshal(ru)
		if err != nil {
			t.Fatalf("Unable to marshal test RU as json: %s", err)
		}

		pairs = append(pairs, &api.KVPair{
			Value: jsonRU,
		})
	}

	return pairs
}

func testRollValue(id rc_fields.ID) fields.Update {
	// not a full update, just enough for a smoke test
	return fields.Update{
		NewRC: id,
	}
}

func testManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("slug")
	return builder.GetManifest()
}

func testNodeSelector() klabels.Selector {
	return klabels.Everything().
		Add("some_key", klabels.EqualsOperator, []string{"some_value"})
}
