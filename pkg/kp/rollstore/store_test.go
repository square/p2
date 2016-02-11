package rollstore

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/square/p2/pkg/kp"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
)

const (
	testRCId  = rc_fields.ID("abcd-1234")
	testRCId2 = rc_fields.ID("def-456")
)

func TestRollPath(t *testing.T) {
	rollPath, err := RollPath(testRCId)
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
	rollLockPath, err := RollLockPath(testRCId)
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
}

func newRollStore(entries map[rc_fields.ID]*api.KVPair) (consulStore, error) {
	storeFields := make(map[string]*api.KVPair)
	for k, v := range entries {
		path, err := RollPath(k)
		if err != nil {
			return consulStore{}, err
		}
		storeFields[path] = v
	}
	return consulStore{
		kv: fakeKV{
			entries: storeFields,
		},
	}, nil
}

func (f fakeKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	return f.entries[key], nil, nil
}

func (f fakeKV) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	ret := make(api.KVPairs, 0)
	for _, v := range f.entries {
		ret = append(ret, v)
	}
	return ret, nil, nil
}

func (f fakeKV) CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	return false, nil, util.Errorf("Not implemented")
}
func (f fakeKV) Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error) {
	return nil, util.Errorf("Not implemented")
}
func (f fakeKV) Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	return false, nil, util.Errorf("Not implemented")
}

func TestGet(t *testing.T) {
	entries := map[rc_fields.ID]*api.KVPair{
		testRCId: &api.KVPair{
			Value: testRollValue(t, testRCId),
		},
	}
	rcStore, err := newRollStore(entries)
	if err != nil {
		t.Fatalf("Unable to initialize fake roll store: %s", err)
	}

	entry, err := rcStore.Get(testRCId)
	if err != nil {
		t.Fatalf("Unexpected error retrieving roll from roll store: %s", err)
	}

	if entry.NewRC != testRCId {
		t.Errorf("Expected roll to have NewRC of %s, was %s", testRCId, entry.NewRC)
	}
}

func TestList(t *testing.T) {
	entries := map[rc_fields.ID]*api.KVPair{
		testRCId: &api.KVPair{
			Value: testRollValue(t, testRCId),
		},
		testRCId2: &api.KVPair{
			Value: testRollValue(t, testRCId2),
		},
	}
	rcStore, err := newRollStore(entries)
	if err != nil {
		t.Fatalf("Unable to initialize fake roll store: %s", err)
	}

	rolls, err := rcStore.List()
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

func testRollValue(t *testing.T, id rc_fields.ID) []byte {
	// not a full update, just enough for a smoke test
	update := fields.Update{
		NewRC: id,
	}

	json, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("Unable to marshal test field as JSON: %s", err)
	}
	return json
}
