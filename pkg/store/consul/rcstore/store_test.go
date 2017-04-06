package rcstore

import (
	"fmt"
	"testing"

	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/consultest"
	"github.com/square/p2/pkg/store/consul/consulutil"
)

const testRCId = fields.ID("abcd-1234")

func TestLockForOwnership(t *testing.T) {
	store := consultest.NewFakePodStore(nil, nil)

	session, _, err := store.NewSession("test rc ownership session", nil)
	if err != nil {
		t.Fatalf("Unable to create fake session in fake store: %s", err)
	}

	rcstore := ConsulStore{}
	unlocker, err := rcstore.LockForOwnership(testRCId, session)
	if err != nil {
		t.Fatalf("Unable to lock rc for ownership: %s", err)
	}

	expectedKey := fmt.Sprintf("%s/%s/%s", consulutil.LOCK_TREE, rcTree, testRCId)
	if unlocker.Key() != expectedKey {
		t.Errorf("Key did not match expected: wanted '%s' but got '%s'", expectedKey, unlocker.Key())
	}
}

func TestLockForMutation(t *testing.T) {
	store := consultest.NewFakePodStore(nil, nil)

	session, _, err := store.NewSession("test rc mutation session", nil)
	if err != nil {
		t.Fatalf("Unable to create fake session in fake store: %s", err)
	}

	rcstore := ConsulStore{}
	unlocker, err := rcstore.LockForMutation(testRCId, session)
	if err != nil {
		t.Fatalf("Unable to lock rc for mutation: %s", err)
	}

	expectedKey := fmt.Sprintf("%s/%s/%s/%s", consulutil.LOCK_TREE, rcTree, testRCId, mutationSuffix)
	if unlocker.Key() != expectedKey {
		t.Errorf("Key did not match expected: wanted '%s' but got '%s'", expectedKey, unlocker.Key())
	}
}

func TestLockForUpdateCreation(t *testing.T) {
	store := consultest.NewFakePodStore(nil, nil)

	session, _, err := store.NewSession("test rc update creation session", nil)
	if err != nil {
		t.Fatalf("Unable to create fake session in fake store: %s", err)
	}

	rcstore := ConsulStore{}
	unlocker, err := rcstore.LockForUpdateCreation(testRCId, session)
	if err != nil {
		t.Fatalf("Unable to lock rc for update creation: %s", err)
	}

	expectedKey := fmt.Sprintf("%s/%s/%s/%s", consulutil.LOCK_TREE, rcTree, testRCId, updateCreationSuffix)
	if unlocker.Key() != expectedKey {
		t.Errorf("Key did not match expected: wanted '%s' but got '%s'", expectedKey, unlocker.Key())
	}
}
