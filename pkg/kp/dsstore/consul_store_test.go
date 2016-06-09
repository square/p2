package dsstore

import (
	"testing"

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
		t.Errorf("Expected create to fail on bad pod id")
	}

	podID = types.PodID("pod_id")
	if _, err := store.Create(manifest, minHealth, clusterName, selector, podID); err == nil {
		t.Errorf("Expected create to fail on bad manifest pod id")
	}

	manifestBuilder = pods.NewManifestBuilder()
	manifestBuilder.SetID("different_pod_id")

	manifest = manifestBuilder.GetManifest()
	if _, err := store.Create(manifest, minHealth, clusterName, selector, podID); err == nil {
		t.Errorf("Expected create to fail on pod id and manifest pod id mismatch")
	}
}

func createDaemonSet(store *consulStore, t *testing.T) {
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
		t.Errorf("daemon set should have an id")
	}

	Assert(t).AreNotEqual(ds.ID, "", "Daemon set should have an id")
	Assert(t).AreNotEqual(ds.PodID, "", "Daemon set should have a pod id")

	Assert(t).AreEqual(ds.PodID, podID, "Daemon set pod id was not set correctly")
	Assert(t).AreEqual(ds.MinHealth, minHealth, "Daemon set minimum health was not set correctly")
	Assert(t).AreEqual(ds.Name, clusterName, "Daemon set cluster name was not set correctly")

	testLabels := klabels.Set{
		pc_fields.AvailabilityZoneLabel: azLabel.String(),
	}
	if matches := ds.NodeSelector.Matches(testLabels); !matches {
		t.Errorf("The daemon set has a bad node selector")
	}

	originalSHA, err := manifest.SHA()
	if err != nil {
		t.Errorf("Unable to retrieve SHA from manifest")
	}
	getSHA, err := ds.Manifest.SHA()
	if err != nil {
		t.Errorf("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(originalSHA, getSHA, "Daemon set manifest not set correctly")
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
		t.Errorf("Error retrieving created daemon set: %s", err)
	}

	Assert(t).AreNotEqual(getDS.ID, "", "Daemon set should have an id")
	Assert(t).AreNotEqual(getDS.PodID, "", "Daemon set should have a pod id")

	Assert(t).AreEqual(ds.ID, getDS.ID, "Daemon set should be equal ids")
	Assert(t).AreEqual(ds.PodID, getDS.PodID, "Daemon set should have equal ids")
	Assert(t).AreEqual(ds.MinHealth, getDS.MinHealth, "Daemon set should have equal minimum healths")
	Assert(t).AreEqual(ds.Name, getDS.Name, "Daemon set should have equal names")

	testLabels := klabels.Set{
		pc_fields.AvailabilityZoneLabel: azLabel.String(),
	}
	if matches := getDS.NodeSelector.Matches(testLabels); !matches {
		t.Errorf("The daemon set has a bad node selector")
	}

	originalSHA, err := manifest.SHA()
	if err != nil {
		t.Errorf("Unable to retrieve SHA from manifest")
	}
	getSHA, err := getDS.Manifest.SHA()
	if err != nil {
		t.Errorf("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(originalSHA, getSHA, "Daemon set shas were not equal")
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
			t.Fatalf("Unexpected daemon set listed: %v", daemonSet)
		}
	}
}

func consulStoreWithFakeKV() *consulStore {
	return &consulStore{
		kv:         kptest.NewFakeKV(),
		applicator: labels.NewFakeApplicator(),
		logger:     logging.DefaultLogger,
		retries:    0,
	}
}
