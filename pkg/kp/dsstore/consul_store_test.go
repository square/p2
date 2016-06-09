package dsstore

import (
	"testing"

	"github.com/square/p2/pkg/util"

	. "github.com/anthonybishopric/gotcha"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestCreate(t *testing.T) {
	store := consulStoreWithFakeKV()
	createDaemonSet(store, t)

	// Create a bad DaemonSet
	podID := types.PodID("")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := pods.NewManifestBuilder()
	manifestBuilder.SetID("")

	manifest := manifestBuilder.GetManifest()

	if _, err := store.Create(manifest, minHealth, clusterName, selector, podID); err == nil {
		t.Error("Expected create to fail on bad pod id")
	}

	podID = types.PodID("pod_id")
	if _, err := store.Create(manifest, minHealth, clusterName, selector, podID); err == nil {
		t.Error("Expected create to fail on bad manifest pod id")
	}

	manifestBuilder = pods.NewManifestBuilder()
	manifestBuilder.SetID("different_pod_id")

	manifest = manifestBuilder.GetManifest()
	if _, err := store.Create(manifest, minHealth, clusterName, selector, podID); err == nil {
		t.Error("Expected create to fail on pod id and manifest pod id mismatch")
	}
}

func createDaemonSet(store *consulStore, t *testing.T) ds_fields.DaemonSet {
	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := pods.NewManifestBuilder()
	manifestBuilder.SetID(podID)

	manifest := manifestBuilder.GetManifest()

	ds, err := store.Create(manifest, minHealth, clusterName, selector, podID)
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
		pc_fields.AvailabilityZoneLabel: azLabel.String(),
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
	store := consulStoreWithFakeKV()
	ds := createDaemonSet(store, t)

	if err := store.Delete("bad_id"); err != nil {
		t.Error("Expected no errors while deleting a daemon set that does not exist")
	}

	if err := store.Delete(ds.ID); err != nil {
		t.Errorf("Unable to delete existing daemon set: %s", err)
	}

	if _, _, err := store.Get(ds.ID); err == nil {
		t.Error("Expected to encounter an error while getting a deleted daemon set")
	}

	if err := store.Delete(ds.ID); err != nil {
		t.Error("Expected no errors on while deleting a deleted daemon set")
	}
}

func TestGet(t *testing.T) {
	store := consulStoreWithFakeKV()
	//
	// Create DaemonSet
	//
	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := pods.NewManifestBuilder()
	manifestBuilder.SetID(podID)

	manifest := manifestBuilder.GetManifest()

	ds, err := store.Create(manifest, minHealth, clusterName, selector, podID)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	Assert(t).AreNotEqual(ds.ID, "", "Daemon set should have an id")

	//
	// Get DaemonSet and verify it is the same
	//
	getDS, _, err := store.Get(ds.ID)
	if err != nil {
		t.Fatalf("Error retrieving created daemon set: %s", err)
	}

	Assert(t).AreNotEqual(getDS.ID, "", "Daemon set should have an id")
	Assert(t).AreNotEqual(getDS.PodID, "", "Daemon set should have a pod id")

	Assert(t).AreEqual(ds.ID, getDS.ID, "Daemon set should be equal ids")
	Assert(t).AreEqual(ds.PodID, getDS.PodID, "Daemon set should have equal ids")
	Assert(t).AreEqual(ds.MinHealth, getDS.MinHealth, "Daemon set should have equal minimum healths")
	Assert(t).AreEqual(ds.Name, getDS.Name, "Daemon set should have equal names")
	Assert(t).AreEqual(ds.Disabled, getDS.Disabled, "Daemon set should have same disabled fields")

	testLabels := klabels.Set{
		pc_fields.AvailabilityZoneLabel: azLabel.String(),
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
	_, _, err = store.Get("bad_id")
	if err == nil {
		t.Error("Expected get operation to fail when getting a daemon set which does not exist")
	}
}

func TestList(t *testing.T) {
	store := consulStoreWithFakeKV()

	// Create first DaemonSet
	firstPodID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := pods.NewManifestBuilder()
	manifestBuilder.SetID(firstPodID)

	firstManifest := manifestBuilder.GetManifest()

	firstDS, err := store.Create(firstManifest, minHealth, clusterName, selector, firstPodID)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	// Create second DaemonSet
	secondPodID := types.PodID("different_pod_id")

	manifestBuilder = pods.NewManifestBuilder()
	manifestBuilder.SetID(secondPodID)

	secondManifest := manifestBuilder.GetManifest()
	secondDS, err := store.Create(secondManifest, minHealth, clusterName, selector, secondPodID)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	daemonSetList, err := store.List()
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
	store := consulStoreWithFakeKV()

	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := pods.NewManifestBuilder()
	manifestBuilder.SetID(podID)
	manifest := manifestBuilder.GetManifest()

	ds, err := store.Create(manifest, minHealth, clusterName, selector, podID)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}
	//
	// Invalid mutates
	//
	errorMutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		return dsToMutate, util.Errorf("This is an error")
	}
	_, err = store.MutateDS(ds.ID, errorMutator)
	if err == nil {
		t.Error("Expected error when mutator produces an error")
	}

	badIDMutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToMutate.ID = ""
		return dsToMutate, nil
	}
	_, err = store.MutateDS(ds.ID, badIDMutator)
	if err == nil {
		t.Error("Expected error when mutating daemon set ID")
	}

	badPodIDMutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToMutate.PodID = ""
		return dsToMutate, nil
	}
	_, err = store.MutateDS(ds.ID, badPodIDMutator)
	if err == nil {
		t.Error("Expected error when mutating daemon set PodID to mismatch manifest")
	}
	//
	// A valid mutate followed by validation
	//
	someOtherDisabled := !ds.Disabled
	someOtherMinHealth := 42
	someOtherName := ds_fields.ClusterName("some_other_name")
	someOtherPodID := types.PodID("some_other_pod_id")

	manifestBuilder = pods.NewManifestBuilder()
	manifestBuilder.SetID(someOtherPodID)
	someOtherManifest := manifestBuilder.GetManifest()

	someOtherAZLabel := pc_fields.AvailabilityZone("some_other_zone")
	someOtherSelector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{someOtherAZLabel.String()})

	goodMutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToMutate.Disabled = someOtherDisabled
		dsToMutate.Manifest = someOtherManifest
		dsToMutate.MinHealth = someOtherMinHealth
		dsToMutate.Name = someOtherName
		dsToMutate.NodeSelector = someOtherSelector
		dsToMutate.PodID = someOtherPodID
		return dsToMutate, nil
	}
	someOtherDS, err := store.MutateDS(ds.ID, goodMutator)
	if err != nil {
		t.Fatalf("Unable to mutate daemon set: %s", err)
	}

	Assert(t).AreEqual(someOtherDS.ID, ds.ID, "Daemon sets should be equal ids")
	Assert(t).AreEqual(someOtherDS.PodID, someOtherPodID, "Daemon sets should have equal ids")
	Assert(t).AreEqual(someOtherDS.MinHealth, someOtherMinHealth, "Daemon sets should have equal minimum healths")
	Assert(t).AreEqual(someOtherDS.Name, someOtherName, "Daemon sets should have equal names")
	Assert(t).AreEqual(someOtherDS.Disabled, someOtherDisabled, "Daemon sets should have same disabled fields")

	someOtherLabels := klabels.Set{
		pc_fields.AvailabilityZoneLabel: someOtherAZLabel.String(),
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
	getDS, _, err := store.Get(ds.ID)
	if err != nil {
		t.Fatalf("Unable to get daemon set: %s", err)
	}
	Assert(t).AreEqual(getDS.ID, ds.ID, "Daemon sets should be equal ids")
	Assert(t).AreEqual(getDS.PodID, someOtherPodID, "Daemon sets should have equal ids")
	Assert(t).AreEqual(getDS.MinHealth, someOtherMinHealth, "Daemon sets should have equal minimum healths")
	Assert(t).AreEqual(getDS.Name, someOtherName, "Daemon sets should have equal names")
	Assert(t).AreEqual(getDS.Disabled, someOtherDisabled, "Daemon sets should have same disabled fields")

	someOtherLabels = klabels.Set{
		pc_fields.AvailabilityZoneLabel: someOtherAZLabel.String(),
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

func consulStoreWithFakeKV() *consulStore {
	return &consulStore{
		kv:         kptest.NewFakeKV(),
		applicator: labels.NewFakeApplicator(),
		logger:     logging.DefaultLogger,
		retries:    0,
	}
}
