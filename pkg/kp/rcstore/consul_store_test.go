package rcstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"

	"github.com/hashicorp/consul/api"
)

func TestPublishLatestRCs(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRCs(inCh, quitCh)
	go func() {
		select {
		case <-quitCh:
		case err := <-errCh:
			t.Fatalf("Unexpected error on errCh: %s", err)
		}
	}()

	var val []fields.RC
	// Put some values on the inCh and read them from outCh transformed
	// into RCs
	inCh <- rcsWithIDs(t, "a", 3)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "a" {
			t.Errorf("Expected all RCs to have id %s, was %s", "a", rc.ID)
		}
	}

	inCh <- rcsWithIDs(t, "b", 2)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 2 {
		t.Errorf("Expected %d values on outCh, got %d", 2, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "b" {
			t.Errorf("Expected all RCs to have id %s, was %s", "b", rc.ID)
		}
	}

	// Now, let's put some stuff on inCh but not read it for a bit
	inCh <- rcsWithIDs(t, "c", 4)
	inCh <- rcsWithIDs(t, "d", 5)
	inCh <- rcsWithIDs(t, "e", 6)

	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 6 {
		t.Errorf("Expected %d values on outCh, got %d", 6, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "e" {
			t.Errorf("Expected all RCs to have id %s, was %s", "e", rc.ID)
		}
	}
}

func TestPublishLatestRCsSkipsIfCorrupt(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRCs(inCh, quitCh)

	// push some legitimate RCs and read them out
	var val []fields.RC
	inCh <- rcsWithIDs(t, "a", 3)
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

	for _, rc := range val {
		if rc.ID.String() != "a" {
			t.Errorf("Expected all RCs to have id %s, was %s", "a", rc.ID)
		}
	}

	// Now push some bogus JSON that will trigger an error
	corruptData := []*api.KVPair{&api.KVPair{Value: []byte("bad_json")}}
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
	// push some legitimate RCs and read them out
	inCh <- rcsWithIDs(t, "b", 3)
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

	for _, rc := range val {
		if rc.ID.String() != "b" {
			t.Errorf("Expected all RCs to have id %s, was %s", "b", rc.ID)
		}
	}
}

func TestPublishQuitsOnQuitChannelClose(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})

	outCh, errCh := publishLatestRCs(inCh, quitCh)
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

	outCh, errCh := publishLatestRCs(inCh, quitCh)
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

	outCh, errCh := publishLatestRCs(inCh, quitCh)

	// Write some legitimate data and read it out
	var val []fields.RC
	// Put some values on the inCh and read them from outCh transformed
	// into RCs
	inCh <- rcsWithIDs(t, "a", 3)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "a" {
			t.Errorf("Expected all RCs to have id %s, was %s", "a", rc.ID)
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

func TestLockTypeFromKey(t *testing.T) {
	store := consulStore{}
	expectedRCID := fields.ID("abcd-1234")
	mutationLockPath, err := store.mutationLockPath(expectedRCID)
	if err != nil {
		t.Fatalf("Unable to compute lock path for rc")
	}

	updateCreationLockPath, err := store.updateCreationLockPath(expectedRCID)
	if err != nil {
		t.Fatalf("Unable to compute lock path for rc")
	}

	ownershipLockPath, err := store.ownershipLockPath(expectedRCID)
	if err != nil {
		t.Fatalf("Unable to compute lock path for rc")
	}

	type lockTypeExpectation struct {
		Key          string
		ExpectedType LockType
		ExpectError  bool
	}

	expectations := []lockTypeExpectation{
		{mutationLockPath, MutationLockType, false},
		{updateCreationLockPath, UpdateCreationLockType, false},
		{ownershipLockPath, OwnershipLockType, false},
		{"bogus_key", UnknownLockType, true},
		{"/lock/bogus_key", UnknownLockType, true},
		{"/lock/replication_controllers/bogus/key/blah", UnknownLockType, true},
	}

	for _, expectation := range expectations {
		rcId, lockType, err := store.lockTypeFromKey(expectation.Key)
		if lockType != expectation.ExpectedType {
			t.Errorf("Expected lock type for %s to be %s, was %s", expectation.Key, expectation.ExpectedType.String(), lockType.String())
		}

		if expectation.ExpectError {
			if err == nil {
				t.Errorf("Expected an error to be returned for key %s, but there wasn't", expectation.Key)
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error for key %s: %s", expectation.Key, err)
			}

			if rcId != expectedRCID {
				t.Errorf("Expected returned rcID to be '%s', was '%s'", expectedRCID, rcId)
			}
		}
	}
}

type LockInfoTestCase struct {
	InputRCs       []fields.RC
	ExpectedOutput []RCLockResult
}

func TestPublishLatestRCsWithLockInfoNoLocks(t *testing.T) {
	rcstore := consulStore{
		kv: kptest.NewFakeKV(),
	}

	inCh := make(chan []fields.RC)
	defer close(inCh)
	quitCh := make(chan struct{})
	defer close(quitCh)

	lockResultCh, errCh := rcstore.publishLatestRCsWithLockInfo(inCh, quitCh)
	go func() {
		for err := range errCh {
			t.Fatalf("Unexpected error on errCh: %s", err)
		}
	}()

	// Create a test case with 2 RCs with no locks
	unlockedCase := LockInfoTestCase{
		InputRCs: []fields.RC{fields.RC{ID: "abc"}, fields.RC{ID: "123"}},
		ExpectedOutput: []RCLockResult{
			{
				RC: fields.RC{ID: "abc"},
			},
			{
				RC: fields.RC{ID: "123"},
			},
		},
	}

	verifyLockInfoTestCase(t, unlockedCase, inCh, lockResultCh)

	// Write the same input once more without reading the channel and make
	// sure that doesn't cause timeouts
	select {
	case inCh <- unlockedCase.InputRCs:
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out writing to input channel")
	}

	// create a new case
	unlockedCase2 := LockInfoTestCase{
		InputRCs: []fields.RC{
			fields.RC{ID: "abc"},
			fields.RC{ID: "123"},
			fields.RC{ID: "456"},
		},
		ExpectedOutput: []RCLockResult{
			{
				RC: fields.RC{ID: "abc"},
			},
			{
				RC: fields.RC{ID: "123"},
			},
			{
				RC: fields.RC{ID: "456"},
			},
		},
	}

	verifyLockInfoTestCase(t, unlockedCase2, inCh, lockResultCh)
}

func TestPublishLatestRCsWithLockInfoWithLocks(t *testing.T) {
	fakeKV := kptest.NewFakeKV()
	rcstore := consulStore{
		kv: fakeKV,
	}

	inCh := make(chan []fields.RC)
	defer close(inCh)
	quitCh := make(chan struct{})
	defer close(quitCh)

	lockResultCh, errCh := rcstore.publishLatestRCsWithLockInfo(inCh, quitCh)
	go func() {
		for err := range errCh {
			t.Fatalf("Unexpected error on errCh: %s", err)
		}
	}()

	// Create a test case with 2 RCs with no locks
	lockedCase := LockInfoTestCase{
		InputRCs: []fields.RC{fields.RC{ID: "abc"}, fields.RC{ID: "123"}},
		ExpectedOutput: []RCLockResult{
			{
				RC:                fields.RC{ID: "abc"},
				LockedForMutation: true,
			},
			{
				RC:                 fields.RC{ID: "123"},
				LockedForOwnership: true,
			},
		},
	}

	ownershipLockPath, err := rcstore.ownershipLockPath("123")
	if err != nil {
		t.Fatalf("Unable to compute ownership lock path: %s", err)
	}
	_, _, err = fakeKV.Acquire(&api.KVPair{
		Key: ownershipLockPath,
	}, nil)
	if err != nil {
		t.Fatalf("Unable to lock for ownership: %s", err)
	}

	mutationLockPath, err := rcstore.mutationLockPath("abc")
	if err != nil {
		t.Fatalf("Unable to compute mutation lock path: %s", err)
	}
	_, _, err = fakeKV.Acquire(&api.KVPair{
		Key: mutationLockPath,
	}, nil)
	if err != nil {
		t.Fatalf("Unable to lock for mutation: %s", err)
	}

	verifyLockInfoTestCase(t, lockedCase, inCh, lockResultCh)

	// Add an update creation lock to the second one
	lockedCase2 := LockInfoTestCase{
		InputRCs: []fields.RC{fields.RC{ID: "abc"}, fields.RC{ID: "123"}},
		ExpectedOutput: []RCLockResult{
			{
				RC:                fields.RC{ID: "abc"},
				LockedForMutation: true,
			},
			{
				RC:                      fields.RC{ID: "123"},
				LockedForOwnership:      true,
				LockedForUpdateCreation: true,
			},
		},
	}

	updateCreationLockPath, err := rcstore.updateCreationLockPath("123")
	if err != nil {
		t.Fatalf("Unable to compute update creation lock path: %s", err)
	}
	_, _, err = fakeKV.Acquire(&api.KVPair{
		Key: updateCreationLockPath,
	}, nil)
	if err != nil {
		t.Fatalf("Unable to lock for updateCreation: %s", err)
	}

	verifyLockInfoTestCase(t, lockedCase2, inCh, lockResultCh)
}

func verifyLockInfoTestCase(t *testing.T, lockInfoTestCase LockInfoTestCase, inCh chan []fields.RC, lockResultCh chan []RCLockResult) {
	select {
	case inCh <- lockInfoTestCase.InputRCs:
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out writing to input channel")
	}

	select {
	case res := <-lockResultCh:
		if len(res) != len(lockInfoTestCase.ExpectedOutput) {
			t.Errorf("Expected %d results but there were %d", len(lockInfoTestCase.ExpectedOutput), len(res))
		}

		for _, val := range lockInfoTestCase.ExpectedOutput {
			matched := false
			for _, result := range res {
				if val.RC.ID != result.RC.ID {
					continue
				}
				matched = true

				if val.LockedForOwnership != result.LockedForOwnership {
					t.Errorf("Expected LockedForOwnership to be %t for %s, was %t", val.LockedForOwnership, result.RC.ID, result.LockedForOwnership)
				}
				if val.LockedForMutation != result.LockedForMutation {
					t.Errorf("Expected LockedForMutation to be %t for %s, was %t", val.LockedForMutation, result.RC.ID, result.LockedForMutation)
				}
				if val.LockedForUpdateCreation != result.LockedForUpdateCreation {
					t.Errorf("Expected LockedForUpdateCreation to be %t for %s, was %t", val.LockedForUpdateCreation, result.RC.ID, result.LockedForUpdateCreation)
				}

				if matched {
					break
				}
			}

			if !matched {
				t.Errorf("Expected an RCLockResult to match %+v, but none did", val)
			}
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out reading from output channel")
	}
}

func rcsWithIDs(t *testing.T, id string, num int) api.KVPairs {
	var pairs api.KVPairs
	builder := pods.NewManifestBuilder()
	builder.SetID("slug")
	manifest := builder.GetManifest()
	for i := 0; i < num; i++ {
		rc := fields.RC{
			ID:       fields.ID(id),
			Manifest: manifest,
		}

		jsonRC, err := json.Marshal(rc)
		if err != nil {
			t.Fatalf("Unable to marshal test RC as json: %s", err)
		}

		pairs = append(pairs, &api.KVPair{
			Value: jsonRC,
		})
	}

	return pairs
}
