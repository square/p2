// +build !race

package consul

import (
	"context"
	"fmt"
	"testing"

	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
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

func TestMutate(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	_, err := f.Store.SetPod(INTENT_TREE, "node1", testManifest("pod"))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := transaction.New(context.Background())
	defer cancel()

	err = f.Store.MutatePod(ctx, []types.NodeName{"node1", "node2"}, "pod", func(m manifest.Manifest) (manifest.Manifest, error) {
		builder := m.GetBuilder()
		builder.SetStatusPort(1000)
		return builder.GetManifest(), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, f.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	manifest, _, err := f.Store.Pod(INTENT_TREE, "node1", "pod")
	if err != nil {
		t.Fatal(err)
	}

	if manifest.GetStatusPort() != 1000 {
		marshaled, err := manifest.Marshal()
		if err != nil {
			t.Fatalf("Expected mutated manifest to have status port 1000. Manifest with status port %d can't be marshaled: %s", manifest.GetStatusPort(), err)
		} else {
			t.Fatalf("Expected mutated manifest to have status port 1000. Manifest: %s", marshaled)
		}
	}
}

func TestMutateError(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	_, err := f.Store.SetPod(INTENT_TREE, "node1", testManifest("pod"))
	if err != nil {
		t.Fatal(err)
	}

	err = f.Store.MutatePod(context.Background(), []types.NodeName{"node1"}, "pod", func(manifest.Manifest) (manifest.Manifest, error) {
		return nil, fmt.Errorf("no")
	})
	if err == nil {
		t.Fatal("expected an error if the mutation errors")
	}
}

func TestPodUniqueKeyFromConsulPath(t *testing.T) {
	type expectation struct {
		path string
		err  bool
		uuid types.PodUniqueKey
	}

	uuid := types.NewPodUUID()
	expectations := []expectation{
		{
			path: "intent/example.com/mysql",
			err:  false,
			uuid: "",
		},
		{
			path: "reality/example.com/mysql",
			err:  false,
			uuid: "",
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
			path: "hooks/all_hooks",
			err:  false,
			uuid: "",
		},
		{
			path: fmt.Sprintf("intent/example.com/%s", uuid),
			uuid: uuid,
			err:  false,
		},
	}

	for _, expectation := range expectations {
		podUniqueKey, err := PodUniqueKeyFromConsulPath(expectation.path)
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
	}
}

func TestAllPods(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	fakeConsulClient := fixture.Client

	// we can't use store.podStatusStore here because we use functions for
	// test purposes that are not on the PodStatusStore interface
	podStatusStore := podstatus.NewConsul(statusstore.NewConsul(fakeConsulClient), PreparerPodStatusNamespace)
	store := NewConsulStore(fakeConsulClient)

	// Add a new uuid pod (i.e. we expect an index rather than a manifest to be written to /intent)
	uuidKey, err := store.podStore.Schedule(testManifest("first_pod"), "node1")
	if err != nil {
		t.Fatal(err)
	}

	// Add a status entry for the pod
	err = podStatusStore.Set(uuidKey, podstatus.PodStatus{Manifest: "id: first_pod"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	// Write the /reality index for the pod
	err = store.podStore.WriteRealityIndex(ctx, uuidKey, "node1")
	if err != nil {
		t.Fatal(err)
	}
	err = transaction.MustCommit(ctx, store.client.KV())
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

func TestSetPodTxn(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()

	manifestToSet := testManifest("some_pod")
	err := f.Store.SetPodTxn(ctx, INTENT_TREE, "some_node", manifestToSet)
	if err != nil {
		t.Fatal(err)
	}

	// confirm manifest wasn't written yet since transaction wasn't committed
	_, _, err = f.Store.Pod(INTENT_TREE, "some_node", "some_pod")
	if err == nil {
		t.Fatal("expected an error when fetching a nonexistent manifest")
	}

	if err != pods.NoCurrentManifest {
		t.Fatalf("unexpected error %q, expected %q", err, pods.NoCurrentManifest)
	}

	err = transaction.MustCommit(ctx, f.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	manifest, _, err := f.Store.Pod(INTENT_TREE, "some_node", "some_pod")
	if err != nil {
		t.Fatal(err)
	}

	setManifestSHA, err := manifestToSet.SHA()
	if err != nil {
		t.Fatal(err)
	}

	fetchedManifestSHA, err := manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}

	if fetchedManifestSHA != setManifestSHA {
		t.Error("manifest set did not match manifest fetched")
	}
}

func TestDeletePodTxn(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	_, err := f.Store.SetPod(INTENT_TREE, "some_node", testManifest("some_pod"))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()

	err = f.Store.DeletePodTxn(ctx, INTENT_TREE, "some_node", "some_pod")
	if err != nil {
		t.Fatal(err)
	}

	// confirm it wasn't actually deleted yet because the transaction wasn't committed
	_, _, err = f.Store.Pod(INTENT_TREE, "some_node", "some_pod")
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, f.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// confirm manifest was deleted
	_, _, err = f.Store.Pod(INTENT_TREE, "some_node", "some_pod")
	if err == nil {
		t.Fatal("expected an error when fetching a nonexistent manifest")
	}

	if err != pods.NoCurrentManifest {
		t.Fatalf("unexpected error %q, expected %q", err, pods.NoCurrentManifest)
	}
}

func testManifest(id types.PodID) manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID(id)
	return builder.GetManifest()
}
