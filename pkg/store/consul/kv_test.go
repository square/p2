package consul

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/types"

	"github.com/hashicorp/consul/api"
)

func TestGetHealthNoEntry(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	// Get a get without a key
	_, err := f.Store.GetHealth("testservice", "testnode")
	if err != nil {
		t.Fatalf("GetHealth returned an error: %v", err)
	}
	_, err = f.Store.GetServiceHealth("testservice")
	if err != nil {
		t.Fatalf("GetServiceHealth returned an error: %v", err)
	}
}

func TestGetHealthWithEntry(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	// Put the key
	watch := WatchResult{
		Id:      "id",
		Node:    "node",
		Service: "service",
	}
	_, _, err := f.Store.PutHealth(watch)
	if err != nil {
		t.Fatalf("PutHealth failed: %v", err)
	}

	watch2 := WatchResult{
		Id:      "id2",
		Node:    "node2",
		Service: "service",
	}

	_, _, err = f.Store.PutHealth(watch2)
	if err != nil {
		t.Fatalf("PutHealth failed: %v", err)
	}

	otherWatch := WatchResult{
		Id:      "id3",
		Node:    "node3",
		Service: "servicewithsuffix",
	}
	_, _, err = f.Store.PutHealth(otherWatch)
	if err != nil {
		t.Fatalf("PutHealth failed: %v", err)
	}

	// Get should work
	watchRes, err := f.Store.GetHealth(watch.Service, watch.Node)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if watchRes.Id != watch.Id {
		t.Fatalf("watchRes and watch ids did not match. GetHealth failed: %#v", watchRes)
	}
	if watchRes.Node != watch.Node {
		t.Fatalf("watchRes and watch Node did not match. GetHealth failed: %#v", watchRes)
	}
	if watchRes.Service != watch.Service {
		t.Fatalf("watchRes and watch Service did not match. GetHealth failed: %#v", watchRes)
	}

	// List should work
	results, err := f.Store.GetServiceHealth(watch.Service)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected to have 2 results, got %v", len(results))
	}
}

func TestPodUniqueKeyOrIDFromConsulPath(t *testing.T) {
	type expectation struct {
		path  string
		err   bool
		uuid  types.PodUniqueKey
		podID types.PodID
	}

	uuid := types.NewPodUUID()
	expectations := []expectation{
		{
			path:  "intent/example.com/mysql",
			err:   false,
			uuid:  "",
			podID: "mysql",
		},
		{
			path:  "reality/example.com/mysql",
			err:   false,
			uuid:  "",
			podID: "mysql",
		},
		{
			path: "labels/example.com/mysql",
			err:  true,
		},
		{
			path: "intent/example/com/mysql",
			err:  true,
		},
		{
			path:  "hooks/all_hooks",
			err:   false,
			uuid:  "",
			podID: "all_hooks",
		},
		{
			path:  fmt.Sprintf("intent/example.com/%s", uuid),
			uuid:  uuid,
			err:   false,
			podID: "",
		},
	}

	for _, expectation := range expectations {
		podUniqueKey, podID, err := PodUniqueKeyOrIDFromConsulPath(expectation.path)
		if expectation.err {
			if err == nil {
				t.Errorf("Expected an error for key '%s'", expectation.path)
			}
			continue
		}

		if err != nil {
			t.Errorf("Unexpected error for key '%s': %s", expectation.path, err)
			continue
		}

		if podUniqueKey != expectation.uuid {
			t.Errorf("Expected podUniqueKey to be %s, was %s", expectation.uuid, podUniqueKey)
		}

		if podID != expectation.podID {
			t.Errorf("Expected podID to be %s but was %s", expectation.podID, podID)
		}
	}
}

func TestAllPods(t *testing.T) {
	fakeConsulClient := consulutil.NewFakeClient()
	store := NewConsulStore(fakeConsulClient)

	// Add a new uuid pod (i.e. we expect an index rather than a manifest to be written to /intent)
	uuidKey, err := store.podStore.Schedule(testManifest("first_pod"), "node1")
	if err != nil {
		t.Fatal(err)
	}

	// Add a status entry for the pod
	err = store.podStatusStore.Set(uuidKey, podstatus.PodStatus{Manifest: "id: first_pod"})
	if err != nil {
		t.Fatal(err)
	}

	// Write the /reality index for the pod
	err = store.podStore.WriteRealityIndex(uuidKey, "node1")
	if err != nil {
		t.Fatal(err)
	}

	// now write a legacy manifest to /intent and /reality
	_, err = store.SetPod(INTENT_TREE, "node2", testManifest("second_pod"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.SetPod(REALITY_TREE, "node2", testManifest("second_pod"))
	if err != nil {
		t.Fatal(err)
	}

	// Now retrieve all the /intent pods and make sure the manifest results are sane
	allIntentPods, _, err := store.AllPods(INTENT_TREE)
	if err != nil {
		t.Fatalf("Unexpected error calling all pods: %s", err)
	}

	if len(allIntentPods) != 2 {
		t.Errorf("Expected 2 intent pods to be returned, but there were %d", len(allIntentPods))
	}

	// Make sure the legacy pod was found
	legacyFound := false
	for _, result := range allIntentPods {
		if result.PodLocation.Node == "node2" {
			legacyFound = true
			if result.Manifest.ID() != "second_pod" {
				t.Errorf("Legacy pod manifest should have had id '%s' but was '%s'", "second_pod", result.Manifest.ID())
			}

			if result.PodUniqueKey != "" {
				t.Error("Legacy pod should not have a uuid")
			}
		}
	}

	if !legacyFound {
		t.Error("Didn't find the legacy (non uuid) pod")
	}

	uuidPodFound := false
	for _, result := range allIntentPods {
		if result.PodLocation.Node == "node1" {
			uuidPodFound = true
			if result.Manifest.ID() != "first_pod" {
				t.Errorf("UUID pod manifest should have had id '%s' but was '%s'", "first_pod", result.Manifest.ID())
			}

			if result.PodUniqueKey == "" {
				t.Error("UUID pod should have a uuid")
			}

			if result.PodUniqueKey != uuidKey {
				t.Errorf("Expected legacy pod to have PodUniqueKey '%s', was '%s'", "node2/second_pod", result.PodUniqueKey)
			}
		}
	}

	if !uuidPodFound {
		t.Error("Didn't find uuid pod")
	}

	// Now retrieve all the pods and make sure the manifest results are sane
	allRealityPods, _, err := store.AllPods(REALITY_TREE)
	if err != nil {
		t.Fatalf("Unexpected error calling all pods: %s", err)
	}

	if len(allRealityPods) != 2 {
		t.Errorf("Expected 2 reality pods to be returned, but there were %d", len(allRealityPods))
	}

	// Make sure the legacy pod was found
	legacyFound = false
	for _, result := range allRealityPods {
		if result.PodLocation.Node == "node2" {
			legacyFound = true
			if result.Manifest.ID() != "second_pod" {
				t.Errorf("Legacy pod manifest should have had id '%s' but was '%s'", "second_pod", result.Manifest.ID())
			}

			if result.PodUniqueKey != "" {
				t.Error("Legacy pod should not have a uuid")
			}
		}
	}

	if !legacyFound {
		t.Error("Didn't find the legacy (non uuid) pod")
	}

	uuidPodFound = false
	for _, result := range allRealityPods {
		if result.PodLocation.Node == "node1" {
			uuidPodFound = true
			if result.Manifest.ID() != "first_pod" {
				t.Errorf("UUID pod manifest should have had id '%s' but was '%s'", "first_pod", result.Manifest.ID())
			}

			if result.PodUniqueKey == "" {
				t.Error("UUID pod should have a uuid")
			}

			if result.PodUniqueKey != uuidKey {
				t.Errorf("Expected legacy pod to have PodUniqueKey '%s', was '%s'", "node2/second_pod", result.PodUniqueKey)
			}
		}
	}

	if !uuidPodFound {
		t.Error("Didn't find uuid pod")
	}
}

// fakeWatcher provides a way to mock out consulutil.WatchPrefix with canned
// values.
type fakeWatcher struct {
	// outCh is a direct pass-through to the outPairs channel passed to WatchPrefix()
	outCh <-chan api.KVPairs

	// errCh is a direct pass-through to the outErrors channel passed to WatchPrefix()
	errCh <-chan error
}

func (f fakeWatcher) WatchPrefix(
	_ string,
	_ consulutil.ConsulLister,
	outPairs chan<- api.KVPairs,
	done <-chan struct{},
	outErrors chan<- error,
	_ time.Duration,
) {
	for {
		select {
		case <-done:
			return
		case outVal := <-f.outCh:
			outPairs <- outVal
		case outError := <-f.errCh:
			outErrors <- outError
		}
	}
}

type panicLister struct {
}

func (panicLister) List(_ string, _ *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	// panic rather than error to make it clear when tests aren't trying to test error behavior from the lister
	panic("this should not be called if the test is structured correctly")
}

func TestWatchIntentKeys(t *testing.T) {
	// really we're going to call innerWatchIntentKeys because that allows us to
	// avoid calling consulutil.WatchPrefix which is already well-tested
	pairsSource := make(chan api.KVPairs)
	defer close(pairsSource)
	errsSource := make(chan error)
	defer close(errsSource)

	quitCh := make(chan struct{})
	defer close(quitCh)
	watcher := fakeWatcher{
		outCh: pairsSource,
		errCh: errsSource,
	}
	outCh := innerWatchIntentKeys("some_node", quitCh, watcher.WatchPrefix, panicLister{})

	uuid := types.NewPodUUID()
	go func() {
		pairsSource <- api.KVPairs{
			{
				Key:         "intent/some_node/mysql",
				ModifyIndex: 1800,
			},
			{
				Key:         fmt.Sprintf("intent/some_node/%s", uuid),
				ModifyIndex: 400,
			},
		}
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for a value from innerWatchIntentKeys")
	case out := <-outCh:
		if out.Error != nil {
			t.Fatalf("error in intent key results: %s", out.Error)
		}

		keys := out.Results
		if len(keys) != 2 {
			t.Fatalf("expected 2 keys but there were %d", len(keys))
		}

		var foundLegacy bool
		var foundUUID bool
		for _, key := range keys {
			if key.PodID == "mysql" {
				foundLegacy = true
				if key.ModifyIndex != 1800 {
					t.Errorf("expected modify index to be 1800 but was %d", key.ModifyIndex)
				}
			}

			if key.PodUniqueKey == uuid {
				foundUUID = true
				if key.ModifyIndex != 400 {
					t.Errorf("expected modify index to be 400 but was %d", key.ModifyIndex)
				}
			}
		}

		if !foundLegacy {
			t.Error("expected a legacy pod in the returned results")
		}

		if !foundUUID {
			t.Error("expected a uuid pod in the returned results")
		}
	}

	// pass an error and mmake sure we get it out
	testErr := errors.New("some error in the list operation")
	go func() {
		errsSource <- testErr
	}()
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for a value from innerWatchIntentKeys")
	case out := <-outCh:
		if out.Error == nil {
			t.Errorf("expected an error result: (%+v)", out)
		} else {
			if out.Error != testErr {
				t.Errorf("expected error to be %s but was %s", testErr, out.Error)
			}
		}

		if len(out.Results) != 0 {
			t.Error("expecte 0 results when receiving an error")
		}
	}

	// add a key and make sure we get 3 out
	go func() {
		pairsSource <- api.KVPairs{
			{
				Key:         "intent/some_node/mysql",
				ModifyIndex: 1800,
			},
			{
				Key:         fmt.Sprintf("intent/some_node/%s", uuid),
				ModifyIndex: 400,
			},
			{
				Key:         "intent/some_node/another_pod",
				ModifyIndex: 2000,
			},
		}
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for a value from innerWatchIntentKeys")
	case out := <-outCh:
		if out.Error != nil {
			t.Fatalf("error in intent key results: %s", out.Error)
		}

		keys := out.Results
		if len(keys) != 3 {
			t.Fatalf("expected 3 keys but there were %d", len(keys))
		}
	}
}

func TestWatchIntentKeysErrorsWhenMissingNodeName(t *testing.T) {
	// really we're going to call innerWatchIntentKeys because that allows us to
	// avoid calling consulutil.WatchPrefix which is already well-tested
	pairsSource := make(chan api.KVPairs)
	errsSource := make(chan error)

	quitCh := make(chan struct{})
	defer close(quitCh)
	watcher := fakeWatcher{
		outCh: pairsSource,
		errCh: errsSource,
	}
	outCh := innerWatchIntentKeys("", quitCh, watcher.WatchPrefix, panicLister{})

	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for value from innerWatchIntentKeys")
	case out := <-outCh:
		if out.Error == nil {
			t.Error("expected an error passing an empty node name")
		}
	}

	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for value from innerWatchIntentKeys")
	case _, ok := <-outCh:
		if ok {
			t.Error("expected output channel to be closed when passing an empty node name")
		}
	}
}

func testManifest(id types.PodID) manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID(id)
	return builder.GetManifest()
}
