package podstore

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/types"

	"github.com/hashicorp/consul/api"
)

func TestSchedule(t *testing.T) {
	store, kv := storeWithFakeKV(t, nil, nil)

	node := types.NodeName("some_node")
	key, err := store.Schedule(testManifest(), node)
	if err != nil {
		t.Fatalf("Unexpected error scheduling pod: %s", err)
	}

	// Now reach into the consul impl to make sure it was stored with an index
	// and a main pod
	podPath := fmt.Sprintf("pods/%s", key)
	indexPath := fmt.Sprintf("intent/%s/%s", node, key)

	if kv.Entries[podPath] == nil {
		t.Fatalf("Key '%s' wasn't set as expected", podPath)
	}

	var pod Pod
	err = json.Unmarshal(kv.Entries[podPath].Value, &pod)
	if err != nil {
		t.Fatal(err)
	}

	if pod.Node != node {
		t.Errorf("Pod wasn't stored with correct node, wanted '%s' was '%s'", node, pod.Node)
	}

	expectedManifestSha, err := testManifest().SHA()
	if err != nil {
		t.Fatal(err)
	}

	podManifestSha, err := pod.Manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}

	if expectedManifestSha != podManifestSha {
		t.Error("Pod was scheduled with the wrong manifest")
	}

	if kv.Entries[indexPath] == nil {
		t.Fatalf("Index '%s' wasn't set as expected", indexPath)
	}

	var index PodIndex
	err = json.Unmarshal(kv.Entries[indexPath].Value, &index)
	if err != nil {
		t.Fatal(err)
	}

	if index.PodKey != key {
		t.Errorf("Index didn't have expected key, wanted '%s' was '%s'", key, index.PodKey)
	}
}

func TestUnschedule(t *testing.T) {
	node := types.NodeName("some_node")
	key := types.NewPodUUID()

	podPath := fmt.Sprintf("pods/%s", key)
	indexPath := fmt.Sprintf("intent/%s/%s", node, key)

	// Initialize the store with entries at the pod path and index path
	pods := map[string]Pod{
		podPath: {
			Manifest: testManifest(),
			Node:     node,
		},
	}

	indices := map[string]PodIndex{
		indexPath: {
			PodKey: key,
		},
	}
	store, kv := storeWithFakeKV(t, pods, indices)

	// Now delete the pod entry
	err := store.Unschedule(key)
	if err != nil {
		t.Fatalf("Unexpected error deleting pod: %s", err)
	}

	if kv.Entries[podPath] != nil {
		t.Fatalf("Key '%s' was deleted as expected", podPath)
	}

	if kv.Entries[indexPath] != nil {
		t.Fatalf("Index '%s' was deleted as expected", indexPath)
	}
}

func TestReadPod(t *testing.T) {
	node := types.NodeName("some_node")
	key := types.NewPodUUID()

	podPath := fmt.Sprintf("pods/%s", key)
	indexPath := fmt.Sprintf("intent/%s/%s", node, key)

	pod := Pod{
		Manifest: testManifest(),
		Node:     node,
	}

	index := PodIndex{
		PodKey: key,
	}

	// Initialize the store with entries at the pod path and index path
	pods := map[string]Pod{
		podPath: pod,
	}

	// This test doesn't actually care if there's an index, but let's keep it hygienic
	indices := map[string]PodIndex{
		indexPath: index,
	}
	store, _ := storeWithFakeKV(t, pods, indices)

	outPod, err := store.ReadPodFromIndex(index)
	if err != nil {
		t.Fatalf("Unexpected error reading pod: %s", err)
	}

	if pod.Node != outPod.Node {
		t.Errorf("Pod node, didn't match expected, wanted %+v was %+v", pod.Node, outPod.Node)
	}

	expectedManifestSha, err := testManifest().SHA()
	if err != nil {
		t.Fatal(err)
	}

	podManifestSha, err := outPod.Manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}

	if expectedManifestSha != podManifestSha {
		t.Error("Pod returned with the wrong manifest")
	}
}

func TestReadPodFromIndex(t *testing.T) {
	node := types.NodeName("some_node")
	key := types.NewPodUUID()

	podPath := fmt.Sprintf("pods/%s", key)
	indexPath := fmt.Sprintf("intent/%s/%s", node, key)

	pod := Pod{
		Manifest: testManifest(),
		Node:     node,
	}

	index := PodIndex{
		PodKey: key,
	}

	// Initialize the store with entries at the pod path and index path
	pods := map[string]Pod{
		podPath: pod,
	}

	// This test doesn't actually care if there's an index, but let's keep it hygienic
	indices := map[string]PodIndex{
		indexPath: index,
	}
	store, _ := storeWithFakeKV(t, pods, indices)

	outPod, err := store.ReadPod(key)
	if err != nil {
		t.Fatalf("Unexpected error reading pod: %s", err)
	}

	if pod.Node != outPod.Node {
		t.Errorf("Pod node, didn't match expected, wanted %+v was %+v", pod.Node, outPod.Node)
	}

	expectedManifestSha, err := testManifest().SHA()
	if err != nil {
		t.Fatal(err)
	}

	podManifestSha, err := outPod.Manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}

	if expectedManifestSha != podManifestSha {
		t.Error("Pod returned with the wrong manifest")
	}
}

func TestWriteRealityIndex(t *testing.T) {
	node := types.NodeName("some_node")
	key := types.NewPodUUID()

	realityIndexPath := fmt.Sprintf("reality/%s/%s", node, key)

	store, fakeKV := storeWithFakeKV(t, make(map[string]Pod), make(map[string]PodIndex))

	// confirm that the reality index doesn't exist
	pair, _, err := fakeKV.Get(realityIndexPath, nil)
	if err != nil {
		t.Fatalf("Initial conditions were not met: error fetching %s: %s", realityIndexPath, err)
	}

	if pair != nil {
		t.Fatalf("Initial conditions were not met: expected key %s to not exist", realityIndexPath)
	}

	err = store.WriteRealityIndex(key, node)
	if err != nil {
		t.Fatal(err)
	}

	pair, _, err = fakeKV.Get(realityIndexPath, nil)
	if err != nil {
		t.Fatalf("Unable to fetch the deleted key (%s): %s", realityIndexPath, err)
	}

	if pair == nil {
		t.Fatalf("%s should have been written but it wasn't", realityIndexPath)
	}
}

func TestDeleteRealityIndex(t *testing.T) {
	node := types.NodeName("some_node")
	key := types.NewPodUUID()

	realityIndexPath := fmt.Sprintf("reality/%s/%s", node, key)

	index := PodIndex{
		PodKey: key,
	}

	indices := map[string]PodIndex{
		realityIndexPath: index,
	}
	store, fakeKV := storeWithFakeKV(t, make(map[string]Pod), indices)

	// confirm that the reality index exists
	pair, _, err := fakeKV.Get(realityIndexPath, nil)
	if err != nil {
		t.Fatalf("Initial conditions were not met: error fetching %s: %s", realityIndexPath, err)
	}

	if pair == nil {
		t.Fatalf("Initial conditions were not met: expected key %s to exist", realityIndexPath)
	}

	err = store.DeleteRealityIndex(key, node)
	if err != nil {
		t.Fatal(err)
	}

	pair, _, err = fakeKV.Get(realityIndexPath, nil)
	if err != nil {
		t.Fatalf("Unable to fetch the deleted key (%s): %s", realityIndexPath, err)
	}

	if pair != nil {
		t.Fatalf("%s should have been deleted but it wasn't", realityIndexPath)
	}
}

// Returns a store, as well as a reference to the underlying KV so that tests can reach a layer underneath to
// verify store behavior.
func storeWithFakeKV(t *testing.T, pods map[string]Pod, indices map[string]PodIndex) (Store, *consulutil.FakeKV) {
	entries := make(map[string]*api.KVPair)
	for key, pod := range pods {
		bytes, err := json.Marshal(pod)
		if err != nil {
			t.Fatal(err)
		}

		entries[key] = &api.KVPair{
			Key:   key,
			Value: bytes,
		}
	}

	for key, index := range indices {
		if _, ok := entries[key]; ok {
			t.Fatalf("Can't have an index and a pod both at key '%s'", key)
		}

		bytes, err := json.Marshal(index)
		if err != nil {
			t.Fatal(err)
		}

		entries[key] = &api.KVPair{
			Key:   key,
			Value: bytes,
		}
	}

	kv := consulutil.NewKVWithEntries(entries)
	return NewConsul(kv), kv
}

func testManifest() store.Manifest {
	builder := store.NewBuilder()

	// bare minimum manifest, we don't care what's in it
	builder.SetID("some_pod")
	return builder.GetManifest()
}
