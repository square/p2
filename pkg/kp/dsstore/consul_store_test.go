package dsstore

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/util"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestCreate(t *testing.T) {
	consulStore := consulStoreWithFakeKV()
	createDaemonSet(consulStore, t)

	// Create a bad DaemonSet
	podID := store.PodID("")
	minHealth := 0
	clusterName := store.DaemonSetName("some_name")

	azLabel := store.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := store.NewBuilder()
	manifestBuilder.SetID("")

	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	if _, err := consulStore.Create(podManifest, minHealth, clusterName, selector, podID, timeout); err == nil {
		t.Error("Expected create to fail on bad pod id")
	}

	podID = store.PodID("pod_id")
	if _, err := consulStore.Create(podManifest, minHealth, clusterName, selector, podID, timeout); err == nil {
		t.Error("Expected create to fail on bad manifest pod id")
	}

	manifestBuilder = store.NewBuilder()
	manifestBuilder.SetID("different_pod_id")

	podManifest = manifestBuilder.GetManifest()
	if _, err := consulStore.Create(podManifest, minHealth, clusterName, selector, podID, timeout); err == nil {
		t.Error("Expected create to fail on pod id and manifest pod id mismatch")
	}
}

func createDaemonSet(consulStore *consulStore, t *testing.T) store.DaemonSet {
	podID := store.PodID("some_pod_id")
	minHealth := 0
	clusterName := store.DaemonSetName("some_name")

	azLabel := store.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := store.NewBuilder()
	manifestBuilder.SetID(podID)

	manifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ds, err := consulStore.Create(manifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	if ds.ID == "" {
		t.Error("daemon set should have an id")
	}

	Assert(t).AreNotEqual(ds.ID, "", "Daemon set should have an id")
	Assert(t).AreNotEqual(ds.PodID, "", "Daemon set should have a pod id")

	Assert(t).AreEqual(ds.PodID, podID, "Daemon set pod id was not set correctly")
	Assert(t).AreEqual(ds.MinHealth, minHealth, "Daemon set minimum health was not set correctly")
	Assert(t).AreEqual(ds.Name, clusterName, "Daemon set cluster name was not set correctly")
	Assert(t).IsFalse(ds.Disabled, "Daemon set disabled field was not set correctly")

	testLabels := klabels.Set{
		store.AvailabilityZoneLabel: azLabel.String(),
	}
	if matches := ds.NodeSelector.Matches(testLabels); !matches {
		t.Error("The daemon set has a bad node selector")
	}

	originalSHA, err := manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest")
	}
	getSHA, err := ds.Manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(originalSHA, getSHA, "Daemon set manifest not set correctly")

	return ds
}

func TestDelete(t *testing.T) {
	consulStore := consulStoreWithFakeKV()
	ds := createDaemonSet(consulStore, t)

	if err := consulStore.Delete("bad_id"); err != nil {
		t.Error("Expected no errors while deleting a daemon set that does not exist")
	}

	if err := consulStore.Delete(ds.ID); err != nil {
		t.Errorf("Unable to delete existing daemon set: %s", err)
	}

	if _, _, err := consulStore.Get(ds.ID); err == nil {
		t.Error("Expected to encounter an error while getting a deleted daemon set")
	}

	if err := consulStore.Delete(ds.ID); err != nil {
		t.Error("Expected no errors on while deleting a deleted daemon set")
	}
}

func TestGet(t *testing.T) {
	consulStore := consulStoreWithFakeKV()
	//
	// Create DaemonSet
	//
	podID := store.PodID("some_pod_id")
	minHealth := 0
	clusterName := store.DaemonSetName("some_name")

	azLabel := store.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := store.NewBuilder()
	manifestBuilder.SetID(podID)

	manifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ds, err := consulStore.Create(manifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	Assert(t).AreNotEqual(ds.ID, "", "Daemon set should have an id")

	//
	// Get DaemonSet and verify it is the same
	//
	getDS, _, err := consulStore.Get(ds.ID)
	if err != nil {
		t.Fatalf("Error retrieving created daemon set: %s", err)
	}

	Assert(t).AreNotEqual(getDS.ID, "", "Daemon set should have an id")
	Assert(t).AreNotEqual(getDS.PodID, "", "Daemon set should have a pod id")

	Assert(t).AreEqual(ds.ID, getDS.ID, "Daemon set should be equal ids")
	Assert(t).AreEqual(ds.PodID, getDS.PodID, "Daemon set should have equal pod ids")
	Assert(t).AreEqual(ds.MinHealth, getDS.MinHealth, "Daemon set should have equal minimum healths")
	Assert(t).AreEqual(ds.Name, getDS.Name, "Daemon set should have equal names")
	Assert(t).AreEqual(ds.Disabled, getDS.Disabled, "Daemon set should have same disabled fields")

	testLabels := klabels.Set{
		store.AvailabilityZoneLabel: azLabel.String(),
	}
	if matches := getDS.NodeSelector.Matches(testLabels); !matches {
		t.Error("The daemon set has a bad node selector")
	}

	originalSHA, err := manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest")
	}
	getSHA, err := getDS.Manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(originalSHA, getSHA, "Daemon set shas were not equal")

	// Invalid get opertaion
	_, _, err = consulStore.Get("bad_id")
	if err == nil {
		t.Error("Expected get operation to fail when getting a daemon set which does not exist")
	}
}

func TestList(t *testing.T) {
	consulStore := consulStoreWithFakeKV()

	// Create first DaemonSet
	firstPodID := store.PodID("some_pod_id")
	minHealth := 0
	clusterName := store.DaemonSetName("some_name")

	azLabel := store.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := store.NewBuilder()
	manifestBuilder.SetID(firstPodID)

	firstManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	firstDS, err := consulStore.Create(firstManifest, minHealth, clusterName, selector, firstPodID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	// Create second DaemonSet
	secondPodID := store.PodID("different_pod_id")

	manifestBuilder = store.NewBuilder()
	manifestBuilder.SetID(secondPodID)

	secondManifest := manifestBuilder.GetManifest()
	secondDS, err := consulStore.Create(secondManifest, minHealth, clusterName, selector, secondPodID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	daemonSetList, err := consulStore.List()
	if err != nil {
		t.Fatalf("Error getting list of daemon sets: %s", err)
	}

	Assert(t).AreEqual(len(daemonSetList), 2, "Unexpected number of daemon sets listed")

	for _, daemonSet := range daemonSetList {
		if daemonSet.ID == firstDS.ID {
			Assert(t).AreEqual(daemonSet.PodID, firstPodID, "Listed daemon set pod ids were not equal")

		} else if daemonSet.PodID == secondDS.PodID {
			Assert(t).AreEqual(daemonSet.PodID, secondPodID, "Listed daemon set pod ids were not equal")

		} else {
			t.Errorf("Unexpected daemon set listed: %v", daemonSet)
		}
	}
}

func TestMutate(t *testing.T) {
	consulStore := consulStoreWithFakeKV()

	podID := store.PodID("some_pod_id")
	minHealth := 0
	clusterName := store.DaemonSetName("some_name")

	azLabel := store.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := store.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ds, err := consulStore.Create(podManifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}
	//
	// Invalid mutates
	//
	errorMutator := func(dsToMutate store.DaemonSet) (store.DaemonSet, error) {
		return dsToMutate, util.Errorf("This is an error")
	}
	_, err = consulStore.MutateDS(ds.ID, errorMutator)
	if err == nil {
		t.Error("Expected error when mutator produces an error")
	}

	badIDMutator := func(dsToMutate store.DaemonSet) (store.DaemonSet, error) {
		dsToMutate.ID = ""
		return dsToMutate, nil
	}
	_, err = consulStore.MutateDS(ds.ID, badIDMutator)
	if err == nil {
		t.Error("Expected error when mutating daemon set ID")
	}

	badPodIDMutator := func(dsToMutate store.DaemonSet) (store.DaemonSet, error) {
		dsToMutate.PodID = ""
		return dsToMutate, nil
	}
	_, err = consulStore.MutateDS(ds.ID, badPodIDMutator)
	if err == nil {
		t.Error("Expected error when mutating daemon set PodID to mismatch manifest")
	}
	//
	// A valid mutate followed by validation
	//
	someOtherDisabled := !ds.Disabled
	someOtherMinHealth := 42
	someOtherName := store.DaemonSetName("some_other_name")
	someOtherPodID := store.PodID("some_other_pod_id")

	manifestBuilder = store.NewBuilder()
	manifestBuilder.SetID(someOtherPodID)
	someOtherManifest := manifestBuilder.GetManifest()

	someOtherAZLabel := store.AvailabilityZone("some_other_zone")
	someOtherSelector := klabels.Everything().
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{someOtherAZLabel.String()})

	goodMutator := func(dsToMutate store.DaemonSet) (store.DaemonSet, error) {
		dsToMutate.Disabled = someOtherDisabled
		dsToMutate.Manifest = someOtherManifest
		dsToMutate.MinHealth = someOtherMinHealth
		dsToMutate.Name = someOtherName
		dsToMutate.NodeSelector = someOtherSelector
		dsToMutate.PodID = someOtherPodID
		return dsToMutate, nil
	}
	someOtherDS, err := consulStore.MutateDS(ds.ID, goodMutator)
	if err != nil {
		t.Fatalf("Unable to mutate daemon set: %s", err)
	}

	Assert(t).AreEqual(someOtherDS.ID, ds.ID, "Daemon sets should be equal ids")
	Assert(t).AreEqual(someOtherDS.PodID, someOtherPodID, "Daemon sets should have equal pod ids")
	Assert(t).AreEqual(someOtherDS.MinHealth, someOtherMinHealth, "Daemon sets should have equal minimum healths")
	Assert(t).AreEqual(someOtherDS.Name, someOtherName, "Daemon sets should have equal names")
	Assert(t).AreEqual(someOtherDS.Disabled, someOtherDisabled, "Daemon sets should have same disabled fields")

	someOtherLabels := klabels.Set{
		store.AvailabilityZoneLabel: someOtherAZLabel.String(),
	}
	if matches := someOtherDS.NodeSelector.Matches(someOtherLabels); !matches {
		t.Error("The daemon set has a bad node selector")
	}

	someOtherSHA, err := someOtherManifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest")
	}
	dsSHA, err := someOtherDS.Manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(someOtherSHA, dsSHA, "Daemon set shas were not equal")

	//
	// Validate daemon set from a get function
	//
	getDS, _, err := consulStore.Get(ds.ID)
	if err != nil {
		t.Fatalf("Unable to get daemon set: %s", err)
	}
	Assert(t).AreEqual(getDS.ID, ds.ID, "Daemon sets should be equal ids")
	Assert(t).AreEqual(getDS.PodID, someOtherPodID, "Daemon sets should have equal pod ids")
	Assert(t).AreEqual(getDS.MinHealth, someOtherMinHealth, "Daemon sets should have equal minimum healths")
	Assert(t).AreEqual(getDS.Name, someOtherName, "Daemon sets should have equal names")
	Assert(t).AreEqual(getDS.Disabled, someOtherDisabled, "Daemon sets should have same disabled fields")

	someOtherLabels = klabels.Set{
		store.AvailabilityZoneLabel: someOtherAZLabel.String(),
	}
	if matches := getDS.NodeSelector.Matches(someOtherLabels); !matches {
		t.Error("The daemon set has a bad node selector")
	}

	someOtherSHA, err = someOtherManifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest")
	}
	dsSHA, err = getDS.Manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(someOtherSHA, dsSHA, "Daemon set shas were not equal")
}

func TestWatch(t *testing.T) {
	consulStore := consulStoreWithFakeKV()
	//
	// Create a new daemon set
	//
	podID := store.PodID("some_pod_id")
	minHealth := 0
	clusterName := store.DaemonSetName("some_name")

	azLabel := store.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := store.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ds, err := consulStore.Create(podManifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}
	//
	// Create another new daemon set
	//
	someOtherPodID := store.PodID("some_other_pod_id")

	manifestBuilder = store.NewBuilder()
	manifestBuilder.SetID(someOtherPodID)
	someOtherManifest := manifestBuilder.GetManifest()

	someOtherDS, err := consulStore.Create(someOtherManifest, minHealth, clusterName, selector, someOtherPodID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}
	//
	// Watch for changes
	//
	quitCh := make(chan struct{})
	inCh := consulStore.Watch(quitCh)
	defer close(quitCh)

	var watched WatchedDaemonSets
	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.Created), 2, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(watched.Updated), 0, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(watched.Deleted), 0, "Unexpected number of deletes watched")

	for _, watchedDS := range watched.Created {
		if watchedDS.ID == ds.ID {
			Assert(t).AreEqual(watchedDS.PodID, ds.PodID, "Daemon sets should have equal pod ids")
		} else if watchedDS.ID == someOtherDS.ID {
			Assert(t).AreEqual(watchedDS.PodID, someOtherDS.PodID, "Daemon sets should have equal pod ids")
		} else {
			t.Errorf("Expected to find id '%s' among watch results, but was not present", watchedDS.ID)
		}
	}

	//
	// Make sure Watch does not output any daemon sets something gets deleted
	//
	err = consulStore.Delete(someOtherDS.ID)
	if err != nil {
		t.Error("Unable to delete daemon set")
	}
	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.Created), 0, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(watched.Updated), 0, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(watched.Deleted), 1, "Unexpected number of deletes watched")

	Assert(t).AreEqual(someOtherDS.ID, watched.Deleted[0].ID, "Daemon sets should have equal ids")

	//
	// Make sure Watch outputs only 1 daemon set when something gets updated
	//
	mutator := func(dsToMutate store.DaemonSet) (store.DaemonSet, error) {
		dsToMutate.Disabled = !dsToMutate.Disabled
		return dsToMutate, nil
	}

	ds, err = consulStore.MutateDS(ds.ID, mutator)
	if err != nil {
		t.Fatalf("Unable to mutate daemon set: %s", err)
	}

	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.Created), 0, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(watched.Updated), 1, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(watched.Deleted), 0, "Unexpected number of deletes watched")

	Assert(t).AreEqual(ds.ID, watched.Updated[0].ID, "Daemon sets should have equal ids")
	Assert(t).AreEqual(ds.PodID, watched.Updated[0].PodID, "Daemon sets should have equal pod ids")
}

func TestWatchAll(t *testing.T) {
	consulStore := consulStoreWithFakeKV()
	//
	// Create a new daemon set
	//
	podID := store.PodID("some_pod_id")
	minHealth := 0
	clusterName := store.DaemonSetName("some_name")

	azLabel := store.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := store.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ds, err := consulStore.Create(podManifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}
	//
	// Create another new daemon set
	//
	someOtherPodID := store.PodID("some_other_pod_id")

	manifestBuilder = store.NewBuilder()
	manifestBuilder.SetID(someOtherPodID)
	someOtherManifest := manifestBuilder.GetManifest()

	someOtherDS, err := consulStore.Create(someOtherManifest, minHealth, clusterName, selector, someOtherPodID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}
	//
	// Watch for create and verify
	//
	quitCh := make(chan struct{})
	inCh := consulStore.WatchAll(quitCh, 0)
	defer close(quitCh)

	var watched WatchedDaemonSetList
	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.DaemonSets), 2, "Unexpected number of daemon sets watched")

	for _, watchedDS := range watched.DaemonSets {
		if watchedDS.ID == ds.ID {
			Assert(t).AreEqual(watchedDS.PodID, ds.PodID, "Daemon sets should have equal pod ids")
		} else if watchedDS.ID == someOtherDS.ID {
			Assert(t).AreEqual(watchedDS.PodID, someOtherDS.PodID, "Daemon sets should have equal pod ids")
		} else {
			t.Errorf("Expected to find id '%s' among watch results, but was not present", watchedDS.ID)
		}
	}

	//
	// Watch for delete and verify
	//
	err = consulStore.Delete(someOtherDS.ID)
	if err != nil {
		t.Error("Unable to delete daemon set")
	}
	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.DaemonSets), 1, "Unexpected number of daemon sets watched")
	Assert(t).AreEqual(ds.ID, watched.DaemonSets[0].ID, "Daemon sets should have equal ids")

	//
	// Watch for update and verify
	//
	mutator := func(dsToMutate store.DaemonSet) (store.DaemonSet, error) {
		dsToMutate.Disabled = !dsToMutate.Disabled
		return dsToMutate, nil
	}

	ds, err = consulStore.MutateDS(ds.ID, mutator)
	if err != nil {
		t.Fatalf("Unable to mutate daemon set: %s", err)
	}

	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.DaemonSets), 1, "Unexpected number of daemon sets watched")
	Assert(t).AreEqual(ds.ID, watched.DaemonSets[0].ID, "Daemon sets should have equal ids")
	Assert(t).AreEqual(ds.PodID, watched.DaemonSets[0].PodID, "Daemon sets should have equal pod ids")
}

func consulStoreWithFakeKV() *consulStore {
	return &consulStore{
		kv:      consulutil.NewFakeClient().KV(),
		logger:  logging.DefaultLogger,
		retries: 0,
	}
}
