// +build !race

package consulutil

import (
	"bytes"
	"reflect"
	"sync"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/hashicorp/consul/api"
)

// PairRecord is a record of a single update to the Consul KV store
type PairRecord struct {
	// "create", "update", "delete", or "close"
	Change string
	// k/v details
	Key   string
	Value string
}

func (r PairRecord) IsCreate(key string) bool {
	return r.Change == "create" && r.Key == key
}

func (r PairRecord) IsUpdate(pair *api.KVPair) bool {
	return r.Change == "update" && r.Key == pair.Key && r.Value == string(pair.Value)
}

func (r PairRecord) IsDelete(key string) bool {
	return r.Change == "delete" && r.Key == key
}

func (r PairRecord) IsClose(key string) bool {
	return r.Change == "close" && r.Key == key
}

type PairRecords []PairRecord

// Filter returns a slice of records for the given key
func (rs PairRecords) Filter(key string) PairRecords {
	newRs := make(PairRecords, 0)
	for _, r := range rs {
		if r.Key == key {
			newRs = append(newRs, r)
		}
	}
	return newRs
}

// PairRecorder can subscribe to a watch stream and record all notifications it receives.
type PairRecorder struct {
	T       *testing.T
	Mutex   sync.Mutex
	Cond    *sync.Cond
	Records []PairRecord
}

func NewRecorder(t *testing.T) *PairRecorder {
	p := &PairRecorder{
		T:       t,
		Records: make([]PairRecord, 0),
	}
	p.Cond = sync.NewCond(&p.Mutex)
	return p
}

func (p *PairRecorder) WaitFor(length int) PairRecords {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()
	for len(p.Records) < length {
		p.Cond.Wait()
	}
	return PairRecords(p.Records[:])
}

func (p *PairRecorder) RecordList() PairRecords {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()
	return PairRecords(p.Records[:])
}

func (p *PairRecorder) Append(change, key string, value []byte) {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()
	p.Records = append(p.Records, PairRecord{change, key, string(value)})
	p.Cond.Broadcast()
}

// Handler is a NewKeyHandler that will process new keys and arrange for their values to
// be recorded.
func (p *PairRecorder) Handler(key string) chan<- *api.KVPair {
	p.T.Logf("%s create", key)
	p.Append("create", key, nil)
	updates := make(chan *api.KVPair)
	go func() {
		for pair := range updates {
			if pair != nil {
				p.T.Logf("%s = %s", key, string(pair.Value))
				p.Append("update", key, pair.Value)
			} else {
				p.T.Logf("%s delete", key)
				p.Append("delete", key, nil)
			}
		}
		p.T.Logf("%s done", key)
		p.Append("close", key, nil)
	}()
	return updates
}

func kvToMap(pairs api.KVPairs) map[string]string {
	m := make(map[string]string)
	for _, pair := range pairs {
		m[pair.Key] = string(pair.Value)
	}
	return m
}

func kvMatch(m map[string]string, pair *api.KVPair) bool {
	val, ok := m[pair.Key]
	return ok && val == string(pair.Value)
}

func kvEqual(a, b *api.KVPair) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	return a.Key == b.Key && (bytes.Compare(a.Value, b.Value) == 0)
}

func testLogger(t *testing.T) chan<- error {
	c := make(chan error)
	go func() {
		for err := range c {
			t.Log(err)
		}
	}()
	return c
}

// TestWatchPrefix verifies some basic operations of the WatchPrefix() function. It should
// find existing data, send new updates when the data changes, and ignore changes outside
// its prefix.
func TestWatchPrefix(t *testing.T) {
	t.Parallel()
	f := NewFixture(t)
	defer f.Stop()

	done := make(chan struct{})
	defer func() {
		if done != nil {
			close(done)
		}
	}()
	pairsChan := make(chan api.KVPairs)
	kv1a := &api.KVPair{Key: "prefix/hello", Value: []byte("world")}
	kv1b := &api.KVPair{Key: "prefix/hello", Value: []byte("computer")}
	kv2a := &api.KVPair{Key: "prefix/test", Value: []byte("foo")}
	kv3a := &api.KVPair{Key: "something", Value: []byte("different")}

	// Process existing data
	f.Client.KV().Put(kv1a, nil)
	go WatchPrefix("prefix/", f.Client.KV(), pairsChan, done, testLogger(t), 0)
	pairs := kvToMap(<-pairsChan)
	if !kvMatch(pairs, kv1a) {
		t.Error("existing data not recognized")
	}

	// Get an updates when the data changes (create, modify, delete)
	f.Client.KV().Put(kv1b, nil)
	pairs = kvToMap(<-pairsChan)
	if !kvMatch(pairs, kv1b) {
		t.Error("value not updated")
	}
	f.Client.KV().Put(kv2a, nil)
	pairs = kvToMap(<-pairsChan)
	if !kvMatch(pairs, kv2a) {
		t.Error("did not find new value")
	}
	if !kvMatch(pairs, kv1b) {
		t.Error("old value disappeared")
	}
	f.Client.KV().Delete(kv1a.Key, nil)
	pairs = kvToMap(<-pairsChan)
	if _, ok := pairs[kv1a.Key]; ok {
		t.Error("did not register deletion")
	}

	// The watcher should ignore kv3a, which is outside its prefix
	f.Client.KV().Put(kv3a, nil)
	f.Client.KV().Delete(kv2a.Key, nil)
	pairs = kvToMap(<-pairsChan)
	if _, ok := pairs[kv3a.Key]; ok {
		t.Error("found a key with the wrong prefix")
	}
	close(done)
	done = nil
	for p := range pairsChan {
		pairs = kvToMap(p)
		if _, ok := pairs[kv3a.Key]; ok {
			t.Error("found a key with the wrong prefix")
		}
	}
}

// TestWatchKeys verifies some basic operations of the WatchKeys() function. It
// should find existing keys, send new updates when the data changes, and
// ignore changes outside its prefix.
func TestWatchKeys(t *testing.T) {
	t.Parallel()
	f := NewFixture(t)
	defer f.Stop()

	done := make(chan struct{})
	defer func() {
		if done != nil {
			close(done)
		}
	}()
	kv1a := &api.KVPair{Key: "prefix/hello", Value: []byte("these")}
	kv1b := &api.KVPair{Key: "prefix/hello", Value: []byte("do")}
	kv2a := &api.KVPair{Key: "prefix/test", Value: []byte("not")}
	kv3a := &api.KVPair{Key: "something", Value: []byte("matter")}

	// Process existing data
	f.Client.KV().Put(kv1a, nil)
	keysChan := WatchKeys("prefix/", f.Client.KV(), done, 0)
	keys := <-keysChan
	expected := []string{kv1a.Key}
	if !reflect.DeepEqual(keys.Keys, expected) {
		t.Errorf("existing data not recognized, wanted %s but got %s", expected, keys.Keys)
	}

	// Get an updates when the data changes (create, modify, delete)
	f.Client.KV().Put(kv1b, nil)
	keys = <-keysChan
	expected = []string{kv1b.Key}
	if !reflect.DeepEqual(keys.Keys, expected) {
		t.Errorf("keys changed inappropriately: wanted %s got %s", expected, keys.Keys)
	}

	f.Client.KV().Put(kv2a, nil)
	keys = <-keysChan
	expected = []string{kv1a.Key, kv2a.Key}
	if !reflect.DeepEqual(keys.Keys, expected) {
		t.Errorf("did not find new key: wanted %s got %s", expected, keys.Keys)
	}

	f.Client.KV().Delete(kv1a.Key, nil)
	keys = <-keysChan
	expected = []string{kv2a.Key}
	if !reflect.DeepEqual(keys.Keys, expected) {
		t.Errorf("did not notice key deletion: wanted %s got %s", expected, keys.Keys)
	}

	// The watcher should ignore kv3a, which is outside its prefix
	f.Client.KV().Put(kv3a, nil)
	f.Client.KV().Delete(kv2a.Key, nil)
	keys = <-keysChan
	if len(keys.Keys) != 0 {
		t.Errorf("watch did not ignore keys outside of prefix: got %s but should have been 0 keys", keys.Keys)
	}
	close(done)
	done = nil
	for range keysChan {
		t.Error("found a key after quitting")
	}
}
func TestWatchSingle(t *testing.T) {
	t.Parallel()
	f := NewFixture(t)
	defer f.Stop()

	done := make(chan struct{})
	defer func() {
		if done != nil {
			close(done)
		}
	}()
	kvpChan := make(chan *api.KVPair)
	kv1a := &api.KVPair{Key: "hello", Value: []byte("world")}
	kv1b := &api.KVPair{Key: "hello", Value: []byte("computer")}
	kv2a := &api.KVPair{Key: "hello/goodbye", Value: []byte("foo")}

	// Process existing data
	f.Client.KV().Put(kv1a, nil)
	go WatchSingle("hello", f.Client.KV(), kvpChan, done, testLogger(t))
	if !kvEqual(kv1a, <-kvpChan) {
		t.Error("existing data not recognized")
	}

	// Get updates when the data changes (modify, delete, create)
	f.Client.KV().Put(kv1b, nil)
	if !kvEqual(kv1b, <-kvpChan) {
		t.Error("value not updated")
	}
	f.Client.KV().Delete("hello", nil)
	if !kvEqual(nil, <-kvpChan) {
		t.Error("value not deleted")
	}
	f.Client.KV().Put(kv1a, nil)
	if !kvEqual(kv1a, <-kvpChan) {
		t.Error("value not recreated")
	}

	// Ignore other keys
	f.Client.KV().Put(kv2a, nil)
	select {
	case <-kvpChan:
		t.Error("found a key that was not being watched")
	default:
	}

	close(done)
	done = nil
	for range kvpChan {
		t.Error("found a key that was never modified")
	}
}

// TestWatchNewKeysSimple is a simple test for WatchNewKeys() basic functionality. Create
// a key, change it, then delete it.
func TestWatchNewKeysSimple(t *testing.T) {
	t.Parallel()
	pairsInput := make(chan api.KVPairs)
	defer close(pairsInput)
	recorder := NewRecorder(t)
	go WatchNewKeys(pairsInput, recorder.Handler, nil)
	pairsInput <- api.KVPairs{}
	key := "hello"
	kv1 := &api.KVPair{Key: key, Value: []byte("world"), CreateIndex: 1, ModifyIndex: 1}
	kv2 := &api.KVPair{Key: key, Value: []byte("computer"), CreateIndex: 1, ModifyIndex: 2}

	// Create a key
	pairsInput <- api.KVPairs{kv1} // put hello
	rs := recorder.WaitFor(2)
	if !rs[0].IsCreate(key) || !rs[1].IsUpdate(kv1) {
		t.Error("unexpected record sequence")
	}

	// Change the key
	pairsInput <- api.KVPairs{kv2} // put hello
	rs = recorder.WaitFor(3)
	if !rs[2].IsUpdate(kv2) {
		t.Error("unexpected record sequence")
	}

	// Delete the key
	pairsInput <- api.KVPairs{} // delete hello
	rs = recorder.WaitFor(5)
	if !rs[3].IsDelete(key) || !rs[4].IsClose(key) {
		t.Error("unexpected record sequence")
	}

	t.Log("full record sequence", recorder.RecordList())
}

// TestWatchNewKeysIgnore verifies that the watcher can handle keys that are ignored.
func TestWatchNewKeysIgnore(t *testing.T) {
	t.Parallel()
	var newKeyCounter int
	pairsInput := make(chan api.KVPairs)
	done := make(chan struct{})
	defer close(done)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		WatchNewKeys(
			pairsInput,
			func(key string) chan<- *api.KVPair {
				t.Log("new key:", key)
				newKeyCounter++
				return nil
			},
			done,
		)
		wg.Done()
	}()
	pairsInput <- api.KVPairs{}
	kv1a := &api.KVPair{Key: "foo", Value: []byte("A"), CreateIndex: 1, ModifyIndex: 1}
	kv1b := &api.KVPair{Key: "foo", Value: []byte("B"), CreateIndex: 1, ModifyIndex: 2}
	kv2a := &api.KVPair{Key: "bar", Value: []byte("A"), CreateIndex: 3, ModifyIndex: 3}

	// Perform a batch of writes
	pairsInput <- api.KVPairs{kv1a}       // New key
	pairsInput <- api.KVPairs{kv1b}       // Update should have no effect
	pairsInput <- api.KVPairs{kv1b, kv2a} // Another new key
	close(pairsInput)

	// Wait for updates to be noticed
	wg.Wait()
	if newKeyCounter != 2 {
		t.Errorf("writes had 2 new keys, found %d", newKeyCounter)
	}
}

// TestWatchNewKeysMulti writes to two different keys and verifies that their update
// notifications are independent.
func TestWatchNewKeysMulti(t *testing.T) {
	t.Parallel()
	pairsInput := make(chan api.KVPairs)
	defer close(pairsInput)
	recorder := NewRecorder(t)
	go WatchNewKeys(pairsInput, recorder.Handler, nil)
	pairsInput <- api.KVPairs{}
	key1 := "foo"
	key2 := "bar"
	kv1a := &api.KVPair{Key: key1, Value: []byte("1A"), CreateIndex: 1, ModifyIndex: 1}
	kv1b := &api.KVPair{Key: key1, Value: []byte("1B"), CreateIndex: 1, ModifyIndex: 2}
	kv1c := &api.KVPair{Key: key1, Value: []byte("1C"), CreateIndex: 1, ModifyIndex: 3}
	kv2a := &api.KVPair{Key: key2, Value: []byte("2A"), CreateIndex: 4, ModifyIndex: 4}
	kv2b := &api.KVPair{Key: key2, Value: []byte("2B"), CreateIndex: 4, ModifyIndex: 5}
	kv2c := &api.KVPair{Key: key2, Value: []byte("2C"), CreateIndex: 4, ModifyIndex: 6}

	pairsInput <- api.KVPairs{kv1a}       // put foo=1A
	pairsInput <- api.KVPairs{kv1b}       // put foo=1B
	pairsInput <- api.KVPairs{kv1b, kv2a} // put bar=2A

	rs := recorder.WaitFor(5)
	rs1 := rs.Filter(key1)
	rs2 := rs.Filter(key2)
	t.Log("rs1", rs1)
	t.Log("rs2", rs2)
	if !(len(rs1) == 3 && rs1[0].IsCreate(key1) && rs1[1].IsUpdate(kv1a) && rs1[2].IsUpdate(kv1b)) ||
		!(len(rs2) == 2 && rs2[0].IsCreate(key2) && rs2[1].IsUpdate(kv2a)) {
		t.Error("unexpected record sequence")
	}

	pairsInput <- api.KVPairs{kv1c, kv2a} // put foo=1C
	pairsInput <- api.KVPairs{kv1c, kv2b} // put bar=2B
	pairsInput <- api.KVPairs{kv2b}       // delete foo
	pairsInput <- api.KVPairs{kv2c}       // put bar=2C

	rs = recorder.WaitFor(10)
	rs1 = rs.Filter(key1)
	rs2 = rs.Filter(key2)
	t.Log("rs1", rs1)
	t.Log("rs2", rs2)
	if !(len(rs1) == 6 && rs1[3].IsUpdate(kv1c) && rs1[4].IsDelete(key1) && rs1[5].IsClose(key1)) ||
		!(len(rs2) == 4 && rs2[2].IsUpdate(kv2b) && rs2[3].IsUpdate(kv2c)) {
		t.Error("unexpected record sequence")
	}

	t.Log("full record sequence", recorder.RecordList())
}

// TestWatchNewKeysExit verifies that the watcher is capable of exiting early and that it
// will notify its subscribers.
func TestWatchNewKeysExit(t *testing.T) {
	t.Parallel()
	pairsInput := make(chan api.KVPairs)
	recorder := NewRecorder(t)
	done := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		WatchNewKeys(pairsInput, recorder.Handler, done)
		wg.Done()
	}()
	pairsInput <- api.KVPairs{}
	key1 := "foo"
	kv1a := &api.KVPair{Key: key1, Value: []byte("1A"), CreateIndex: 1, ModifyIndex: 1}

	pairsInput <- api.KVPairs{kv1a}
	rs := recorder.WaitFor(2)
	if !rs[0].IsCreate(key1) || !rs[1].IsUpdate(kv1a) {
		t.Errorf("error creating key")
	}

	// Ask the watcher to exit, eventually
	close(done)

	// Because the watcher is asynchronous, it might need to consume more input before
	// exiting. In practice, the "done" signal should also stop the producer.
	exiting := make(chan struct{})
	defer close(exiting)
	go func() {
		for {
			select {
			case pairsInput <- api.KVPairs{kv1a}: // no change
			case <-exiting:
				return
			}
		}
	}()

	wg.Wait()
	rs = recorder.WaitFor(3)
	if !rs[2].IsClose(key1) {
		t.Errorf("subscriber did not receive close notification")
	}
}

// TestWatchNewKeysExistingData verifies that the watcher will find existing keys (i.e.,
// when its first input is nonempty) and report them as new data.
func TestWatchNewKeysExistingData(t *testing.T) {
	t.Parallel()
	pairsInput := make(chan api.KVPairs)
	recorder := NewRecorder(t)
	go WatchNewKeys(pairsInput, recorder.Handler, nil)
	kv1a := &api.KVPair{Key: "test", Value: []byte("1A"), CreateIndex: 1, ModifyIndex: 1}

	pairsInput <- api.KVPairs{kv1a}

	rs := recorder.WaitFor(2)
	if !rs[0].IsCreate("test") || !rs[1].IsUpdate(kv1a) {
		t.Error("error picking up existing data")
	}
}

func TestWatchDiff(t *testing.T) {
	t.Parallel()
	f := NewFixture(t)
	defer f.Stop()

	done := make(chan struct{})
	defer func() {
		if done != nil {
			close(done)
		}
	}()
	kv1a := &api.KVPair{Key: "prefix/hello", Value: []byte("world")}
	kv1b := &api.KVPair{Key: "prefix/hello", Value: []byte("computer")}
	kv2a := &api.KVPair{Key: "prefix/test", Value: []byte("foo")}
	kv2b := &api.KVPair{Key: "prefix/test", Value: []byte("bar")}
	kv3a := &api.KVPair{Key: "something", Value: []byte("different")}

	// Process existing data
	var changes *WatchedChanges

	if _, err := f.Client.KV().Put(kv1a, nil); err != nil {
		t.Error("Unexpected error during put operation")
	}
	if _, err := f.Client.KV().Put(kv2a, nil); err != nil {
		t.Error("Unexpected error during put operation")
	}

	watchedCh, errCh := WatchDiff("prefix/", f.Client.KV(), done)
	go func() {
		for err := range errCh {
			t.Log(err)
		}
	}()

	select {
	case changes = <-watchedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}

	pairs := kvToMap(changes.Created)
	if !kvMatch(pairs, kv1a) {
		t.Error("did not find new value")
	}
	if !kvMatch(pairs, kv2a) {
		t.Error("did not find new value")
	}
	Assert(t).AreEqual(len(changes.Created), 2, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(changes.Updated), 0, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(changes.Deleted), 0, "Unexpected number of deletes watched")

	// Get an updates when the data changes (create, modify, delete)
	if _, err := f.Client.KV().Put(kv1b, nil); err != nil {
		t.Error("Unexpected error during put operation")
	}

	select {
	case changes = <-watchedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}

	pairs = kvToMap(changes.Updated)
	if !kvMatch(pairs, kv1b) {
		t.Error("value not updated")
	}
	Assert(t).AreEqual(len(changes.Created), 0, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(changes.Updated), 1, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(changes.Deleted), 0, "Unexpected number of deletes watched")

	if _, err := f.Client.KV().Delete(kv1a.Key, nil); err != nil {
		t.Error("Unexpected error during delete operation")
	}

	select {
	case changes = <-watchedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}

	pairs = kvToMap(changes.Deleted)
	if _, ok := pairs[kv1a.Key]; !ok {
		t.Error("did not register deletion")
	}
	Assert(t).AreEqual(len(changes.Created), 0, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(changes.Updated), 0, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(changes.Deleted), 1, "Unexpected number of deletes watched")

	// Make sure the watcher can output both a created and updated kvPair
	if _, err := f.Client.KV().Put(kv1a, nil); err != nil {
		t.Error("Unexpected error during put operation")
	}
	if _, err := f.Client.KV().Put(kv2b, nil); err != nil {
		t.Error("Unexpected error during put operation")
	}

	select {
	case changes = <-watchedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}

	pairs = kvToMap(changes.Created)
	if !kvMatch(pairs, kv1a) {
		t.Error("did not find new value")
	}
	pairs = kvToMap(changes.Updated)
	if !kvMatch(pairs, kv2b) {
		t.Error("value not updated")
	}
	Assert(t).AreEqual(len(changes.Created), 1, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(changes.Updated), 1, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(changes.Deleted), 0, "Unexpected number of deletes watched")

	// The watcher should ignore kv3a, which is outside its prefix
	if _, err := f.Client.KV().Put(kv3a, nil); err != nil {
		t.Error("Unexpected error during put operation")
	}
	if _, err := f.Client.KV().Delete(kv2a.Key, nil); err != nil {
		t.Error("Unexpected error during delete operation")
	}

	select {
	case changes = <-watchedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}

	pairs = kvToMap(changes.Created)
	if _, ok := pairs[kv3a.Key]; ok {
		t.Error("found a key with the wrong prefix")
	}
	Assert(t).AreEqual(len(changes.Created), 0, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(changes.Updated), 0, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(changes.Deleted), 1, "Unexpected number of deletes watched")

	close(done)
	done = nil
}
