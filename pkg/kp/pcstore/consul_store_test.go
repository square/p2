package pcstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"

	"github.com/Sirupsen/logrus"
	"github.com/rcrowley/go-metrics"
	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestCreate(t *testing.T) {
	store := consulStoreWithFakeKV()
	createPodCluster(store, t)
}

func TestMutate(t *testing.T) {
	store := consulStoreWithFakeKV()
	oldPc := createPodCluster(store, t)

	// After creating the pod cluster, we now update it using a mutator function
	newPodID := types.PodID("pod_id-diff")
	newAz := fields.AvailabilityZone("us-west-diff")
	newClusterName := fields.ClusterName("cluster_name_diff")

	newSelector := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{newPodID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{newAz.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{newClusterName.String()})

	newAnnotations := fields.Annotations(map[string]interface{}{
		"bar": "foo",
	})

	intendedPC := fields.PodCluster{
		ID:               oldPc.ID,
		PodID:            newPodID,
		AvailabilityZone: newAz,
		Name:             newClusterName,
		PodSelector:      newSelector,
		Annotations:      newAnnotations,
	}

	mutator := func(pc fields.PodCluster) (fields.PodCluster, error) {
		pc.PodID = intendedPC.PodID
		pc.AvailabilityZone = intendedPC.AvailabilityZone
		pc.Name = intendedPC.Name
		pc.PodSelector = intendedPC.PodSelector
		pc.Annotations = intendedPC.Annotations
		return pc, nil
	}

	_, err := store.MutatePC(intendedPC.ID, mutator)
	if err != nil {
		t.Fatalf("Unable to update pod cluster: %s", err)
	}

	newPC, err := store.Get(intendedPC.ID)
	if err != nil {
		t.Fatalf("Unable to find pod cluster: %s", err)
	}

	if newPC.ID == "" {
		t.Errorf("pod cluster should have an id")
	}

	if newPC.PodID == "" {
		t.Errorf("pod cluster should have a pod id")
	}

	if newPC.ID != oldPc.ID {
		t.Errorf("id should not have been updated. Wanted '%s' got '%s'", oldPc.ID, newPC.ID)
	}

	if newPC.PodID != newPodID {
		t.Errorf("pod id wasn't properly updated. Wanted '%s' got '%s'", newPodID, newPC.PodID)
	}

	if newPC.AvailabilityZone != newAz {
		t.Errorf("availability zone wasn't properly updated. Wanted '%s' got '%s'", newAz, newPC.AvailabilityZone)
	}

	if newPC.Name != newClusterName {
		t.Errorf("cluster name wasn't properly updated. Wanted '%s' got '%s'", newClusterName, newPC.Name)
	}

	newTestLabels := klabels.Set{
		fields.PodIDLabel:            newPodID.String(),
		fields.AvailabilityZoneLabel: newAz.String(),
		fields.ClusterNameLabel:      newClusterName.String(),
	}

	if matches := newPC.PodSelector.Matches(newTestLabels); !matches {
		t.Errorf("the pod cluster has a bad pod selector")
	}

	if newPC.Annotations["bar"] != "foo" {
		t.Errorf("Annotations didn't match expected")
	} else if _, ok := newPC.Annotations["foo"]; ok {
		t.Errorf("Annotations didn't match expected")
	}
}

func createPodCluster(store *consulStore, t *testing.T) fields.PodCluster {
	podID := types.PodID("pod_id")
	az := fields.AvailabilityZone("us-west")
	clusterName := fields.ClusterName("cluster_name")

	selector := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	annotations := fields.Annotations(map[string]interface{}{
		"foo": "bar",
	})

	session := kptest.NewSession()
	pc, err := store.Create(podID, az, clusterName, selector, annotations, session)
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

	if pc.ID == "" {
		t.Errorf("pod cluster should have an id")
	}

	if pc.PodID == "" {
		t.Errorf("pod cluster should have a pod id")
	}

	if pc.PodID != podID {
		t.Errorf("pod id wasn't properly set. Wanted '%s' got '%s'", podID, pc.PodID)
	}

	if pc.AvailabilityZone != az {
		t.Errorf("availability zone wasn't properly set. Wanted '%s' got '%s'", az, pc.AvailabilityZone)
	}

	if pc.Name != clusterName {
		t.Errorf("cluster name wasn't properly set. Wanted '%s' got '%s'", clusterName, pc.Name)
	}

	testLabels := klabels.Set{
		fields.PodIDLabel:            podID.String(),
		fields.AvailabilityZoneLabel: az.String(),
		fields.ClusterNameLabel:      clusterName.String(),
	}

	if matches := pc.PodSelector.Matches(testLabels); !matches {
		t.Errorf("the pod cluster has a bad pod selector")
	}

	if pc.Annotations["foo"] != "bar" {
		t.Errorf("Annotations didn't match expected")
	}
	return pc
}

func TestLabelsOnCreate(t *testing.T) {
	store := consulStoreWithFakeKV()
	podID := types.PodID("pod_id")
	az := fields.AvailabilityZone("us-west")
	clusterName := fields.ClusterName("cluster_name")

	selector := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	annotations := fields.Annotations(map[string]interface{}{
		"foo": "bar",
	})

	pc, err := store.Create(podID, az, clusterName, selector, annotations, kptest.NewSession())
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

	matches, err := store.applicator.GetMatches(selector, labels.PC)
	if err != nil {
		t.Fatalf("Unable to check for label match on new pod cluster: %s", err)
	}

	if len(matches) != 1 {
		t.Errorf("Expected one pod cluster to match label selector")
	}

	if fields.ID(matches[0].ID) != pc.ID {
		t.Errorf("The pod cluster selector didn't match the new pod cluster")
	}
}

func TestGet(t *testing.T) {
	store := consulStoreWithFakeKV()
	podID := types.PodID("pod_id")
	az := fields.AvailabilityZone("us-west")
	clusterName := fields.ClusterName("cluster_name")

	selector := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	annotations := fields.Annotations(map[string]interface{}{
		"foo": "bar",
	})

	// Create a pod cluster
	pc, err := store.Create(podID, az, clusterName, selector, annotations, kptest.NewSession())
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

	pc, err = store.Get(pc.ID)
	if err != nil {
		t.Fatalf("Unable to get pod cluster: %s", err)
	}

	if pc.ID == "" {
		t.Errorf("pod cluster should have an id")
	}

	if pc.PodID == "" {
		t.Errorf("pod cluster should have a pod id")
	}

	if pc.PodID != podID {
		t.Errorf("pod id wasn't properly set. Wanted '%s' got '%s'", podID, pc.PodID)
	}

	if pc.AvailabilityZone != az {
		t.Errorf("availability zone wasn't properly set. Wanted '%s' got '%s'", az, pc.AvailabilityZone)
	}

	if pc.Name != clusterName {
		t.Errorf("cluster name wasn't properly set. Wanted '%s' got '%s'", clusterName, pc.Name)
	}

	testLabels := klabels.Set{
		fields.PodIDLabel:            podID.String(),
		fields.AvailabilityZoneLabel: az.String(),
		fields.ClusterNameLabel:      clusterName.String(),
	}

	if matches := pc.PodSelector.Matches(testLabels); !matches {
		t.Errorf("the pod cluster has a bad pod selector")
	}

	if pc.Annotations["foo"] != "bar" {
		t.Errorf("Annotations didn't match expected")
	}

	found, err := store.FindWhereLabeled(podID, az, clusterName)
	if err != nil {
		t.Errorf("Could not retrieve labeled pods: %v", err)
	}

	if len(found) != 1 {
		t.Errorf("Found incorrect number of labeled pods, expected 1: %v", len(found))
	}

	if found[0].ID != pc.ID {
		t.Errorf("Didn't find the right pod cluster: %v vs %v", found[0].ID, pc.ID)
	}
}

func TestDelete(t *testing.T) {
	store := consulStoreWithFakeKV()
	podID := types.PodID("pod_id")
	az := fields.AvailabilityZone("us-west")
	clusterName := fields.ClusterName("cluster_name")

	selector := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	annotations := fields.Annotations(map[string]interface{}{
		"foo": "bar",
	})

	// Create a pod cluster
	pc, err := store.Create(podID, az, clusterName, selector, annotations, kptest.NewSession())
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

	pc, err = store.Get(pc.ID)
	if err != nil {
		t.Fatalf("Unable to get pod cluster: %s", err)
	}

	err = store.Delete(pc.ID)
	if err != nil {
		t.Fatalf("Unexpected error deleting pod cluster: %s", err)
	}

	_, err = store.Get(pc.ID)
	if err == nil {
		t.Fatalf("Should have gotten an error fetching a deleted pod cluster")
	}

	if !IsNotExist(err) {
		t.Errorf("The error should have been a pocstore.IsNotExist but was '%s'", err)
	}

	labels, err := store.applicator.GetLabels(labels.PC, pc.ID.String())
	if err != nil {
		t.Fatalf("Got error when trying to confirm label deletion: %s", err)
	}

	if len(labels.Labels) != 0 {
		t.Errorf("Labels were not deleted along with the pod cluster")
	}
}

func TestList(t *testing.T) {
	store := consulStoreWithFakeKV()
	podID := types.PodID("pod_id")
	az := fields.AvailabilityZone("us-west")
	clusterName := fields.ClusterName("cluster_name")

	selector := klabels.Everything()

	annotations := fields.Annotations(map[string]interface{}{
		"foo": "bar",
	})

	// Create a pod cluster
	pc, err := store.Create(podID, az, clusterName, selector, annotations, kptest.NewSession())
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

	// Create another one
	pc2, err := store.Create(podID+"2", az, clusterName, selector, annotations, kptest.NewSession())
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

	// Now test List() and make sure we get both back
	pcs, err := store.List()
	if err != nil {
		t.Fatalf("Unable to list pod clusters: %s", err)
	}

	if len(pcs) != 2 {
		t.Fatalf("Expected 2 results but there were %d", len(pcs))
	}

	for _, foundPC := range pcs {
		found := false
		for _, expectedPC := range []fields.ID{pc.ID, pc2.ID} {
			if foundPC.ID == expectedPC {
				found = true
			}
		}

		if !found {
			t.Errorf("Didn't find one of the pod clusters in the list")
		}
	}
}

func TestWatch(t *testing.T) {
	store := consulStoreWithFakeKV()
	podID := types.PodID("pod_id")
	az := fields.AvailabilityZone("us-west")
	clusterName := fields.ClusterName("cluster_name")

	selector := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	annotations := fields.Annotations(map[string]interface{}{
		"foo": "bar",
	})

	var watched WatchedPodClusters
	session := kptest.NewSession()
	pc, err := store.Create(podID, az, clusterName, selector, annotations, session)
	if err != nil {
		t.Fatalf("Unable to create first pod cluster: %s", err)
	}

	pc2, err := store.Create(podID, "us-east", clusterName, selector, annotations, session)
	if err != nil {
		t.Fatalf("Unable to create second pod cluster: %s", err)
	}

	quit := make(chan struct{})
	defer close(quit)
	watch := store.Watch(quit)

	select {
	case watchedPC := <-watch:
		watched = watchedPC
	case <-time.After(5 * time.Second):
		t.Fatal("nothing on the channel")
	}

	if len(watched.Clusters) != 2 {
		t.Fatalf("Expected to get two watched PodClusters, but did not: got %v", len(watched.Clusters))
	}

	expectedIDs := []fields.ID{pc.ID, pc2.ID}
	for _, id := range expectedIDs {
		found := false
		for _, pc := range watched.Clusters {
			if id == pc.ID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find id '%s' among watch results, but was not present", id)
		}
	}
}

func TestWatchPodCluster(t *testing.T) {
	store := consulStoreWithFakeKV()
	pod := fields.ID("pod_id")
	podID := types.PodID("pod_id")
	az := fields.AvailabilityZone("us-west")
	clusterName := fields.ClusterName("cluster_name")

	selector := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{pod.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	annotations := fields.Annotations(map[string]interface{}{
		"foo": "bar",
	})

	session := kptest.NewSession()
	pc, err := store.Create(podID, az, clusterName, selector, annotations, session)
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

	quit := make(chan struct{})
	watch := store.WatchPodCluster(pc.ID, quit)

	var watched *WatchedPodCluster
	select {
	case watchedPC := <-watch:
		watched = &watchedPC
	case <-time.After(5 * time.Second):
		quit <- struct{}{}
		t.Fatal("nothing on the channel")
	}

	if watched == nil {
		t.Fatalf("Expected to get a watched PodCluster, but did not")
	}

	if watched.PodCluster.ID != pc.ID {
		t.Fatalf("Expected watched PodCluster to match %s Pod Cluster ID. Instead was %s", pc.ID, watched.PodCluster.ID)
	}
}

func TestZipPodClusterResults(t *testing.T) {
	store := consulStore{}

	previous := WatchedPodClusters{
		[]*fields.PodCluster{
			{
				ID:    fields.ID("abc123"),
				PodID: types.PodID("vvv"),
				Name:  "old name 1",
			},
			{
				ID:    fields.ID("def456"),
				PodID: types.PodID("xxx"),
				Name:  "old name 2",
			},
		},
		nil,
	}

	current := WatchedPodClusters{
		[]*fields.PodCluster{
			{
				ID:    fields.ID("abc123"),
				PodID: types.PodID("vvv"),
				Name:  "new name 1",
			},

			{
				ID:    fields.ID("987fed"),
				PodID: types.PodID("zzz"),
				Name:  "new name 3",
			},
		},
		nil,
	}

	zipped := store.zipResults(current, previous)

	if len(zipped) != 3 {
		t.Errorf("Unexpected number of clusters in zipped results: %v", len(zipped))
	}

	updated := zipped[fields.ID("abc123")]
	if updated.previous == nil || updated.current == nil {
		t.Fatalf("Either (%v) or (%v) is nil, but neither should be", updated.previous, updated.current)
	}

	if updated.current.Name != "new name 1" {
		t.Errorf("%v was not the right name for the current cluster", updated.current.Name)
	}

	if updated.previous.Name != "old name 1" {
		t.Errorf("%v was not the right name for the previous cluster", updated.previous.Name)
	}

	onlyOld := zipped[fields.ID("def456")]
	if onlyOld.current != nil {
		t.Error("cluster onlyOld should not have had a current cluster")
	}

	if onlyOld.previous == nil {
		t.Fatalf("the previous cluster should not have been nil")
	}

	if onlyOld.previous.Name != "old name 2" {
		t.Errorf("The old name %v was wrong for the cluster", onlyOld.previous.Name)
	}

	onlyNew := zipped[fields.ID("987fed")]
	if onlyNew.previous != nil {
		t.Error("cluster onlyNew should not have had a previous cluster")
	}

	if onlyNew.current == nil {
		t.Fatalf("the current cluster should not have been nil")
	}

	if onlyNew.current.Name != "new name 3" {
		t.Errorf("The old name %v was wrong for the cluster", onlyNew.current.Name)
	}
}

type fakeSync struct {
	syncedCluster *fields.PodCluster
	syncedPods    []labels.Labeled
}

type fakeSyncer struct {
	initial []fields.ID
	synced  chan fakeSync
	deleted chan fakeSync
	ignore  bool
}

func (f *fakeSyncer) SyncCluster(cluster *fields.PodCluster, pods []labels.Labeled) error {
	if f.ignore {
		fmt.Printf("fake: Ignoring update for %v/%v\n", cluster, pods)
		return nil
	}
	f.synced <- fakeSync{
		syncedCluster: cluster,
		syncedPods:    pods,
	}
	return nil
}

func (f *fakeSyncer) DeleteCluster(id fields.ID) error {
	f.deleted <- fakeSync{
		syncedCluster: &fields.PodCluster{ID: id},
	}
	return nil
}

func (f *fakeSyncer) GetInitialClusters() ([]fields.ID, error) {
	return f.initial, nil
}

func (f *fakeSyncer) Type() ConcreteSyncerType {
	return "fake_syncer"
}

// this test simulates creating, updating, and deleting a pod cluster.
// the update step will change the pod selector and should result in a
// different pod ID being returned.
func TestConcreteSyncer(t *testing.T) {
	store := consulStoreWithFakeKV()
	store.logger.Logger.Level = logrus.DebugLevel

	store.applicator.SetLabel(labels.POD, "1234-123-123-1234", "color", "red")
	store.applicator.SetLabel(labels.POD, "abcd-abc-abc-abcd", "color", "blue")

	syncer := &fakeSyncer{
		[]fields.ID{},
		make(chan fakeSync),
		make(chan fakeSync),
		false,
	}

	change := podClusterChange{
		previous: nil,
		current: &fields.PodCluster{
			ID:               fields.ID("abc123"),
			PodID:            types.PodID("vvv"),
			AvailabilityZone: fields.AvailabilityZone("west"),
			Name:             "production",
			PodSelector:      klabels.Everything().Add("color", klabels.EqualsOperator, []string{"red"}),
		},
	}

	changes := make(chan podClusterChange)
	go store.handlePCUpdates(syncer, changes, metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)))

	select {
	case changes <- change:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to write change to handlePCChange")
	}

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to read from the syncer")
	case sync := <-syncer.synced:
		if sync.syncedCluster == nil {
			t.Fatal("unexpectedly didn't get a cluster on the sync channel")
		}
		if sync.syncedCluster.ID != change.current.ID {
			t.Fatalf("got unexpected synced cluster %v", sync.syncedCluster.ID)
		}
		if len(sync.syncedPods) != 1 {
			t.Fatalf("got unexpected number of synced pods with cluster: %v", len(sync.syncedPods))
		}
		if sync.syncedPods[0].ID != "1234-123-123-1234" {
			t.Fatalf("got unexpected pod ID from labeled pods sync: %v", sync.syncedPods[0].ID)
		}
	}

	// now we send a new update that changes the pod cluster's target pod from the red one to the blue one.
	// (from 1234-123-123-1234 to abcd-abc-abc-abcd )
	change = podClusterChange{
		previous: change.current,
		current: &fields.PodCluster{
			ID:               fields.ID("abc123"),
			PodID:            types.PodID("vvv"),
			AvailabilityZone: fields.AvailabilityZone("west"),
			Name:             "production",
			PodSelector:      klabels.Everything().Add("color", klabels.EqualsOperator, []string{"blue"}),
		},
	}

	select {
	case changes <- change:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to write change to handlePCChange")
	}

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to read from the syncer")
	case sync := <-syncer.synced:
		if sync.syncedCluster == nil {
			t.Fatal("unexpectedly didn't get a cluster on the sync channel")
		}
		if sync.syncedCluster.ID != change.current.ID {
			t.Fatalf("got unexpected synced cluster %v", sync.syncedCluster.ID)
		}
		if len(sync.syncedPods) != 1 {
			t.Fatalf("got unexpected number of synced pods with cluster: %v", len(sync.syncedPods))
		}
		if sync.syncedPods[0].ID != "abcd-abc-abc-abcd" {
			t.Fatalf("got unexpected pod ID from labeled pods sync: %v", sync.syncedPods[0].ID)
		}
	}

	// appear to have deleted the cluster
	change.previous = change.current
	change.current = nil

	select {
	case changes <- change:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to write deletion change to handlePCChange")
	}

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to read from the syncer")
	case sync := <-syncer.deleted:
		if sync.syncedCluster == nil {
			t.Fatal("unexpectedly didn't get a cluster on the sync channel")
		}
		if sync.syncedCluster.ID != change.previous.ID {
			t.Fatalf("got unexpected synced cluster %v", sync.syncedCluster.ID)
		}
	}

	close(changes)
}

func TestConcreteSyncerWithPrevious(t *testing.T) {
	store := consulStoreWithFakeKV()
	store.logger.Logger.Level = logrus.DebugLevel

	store.applicator.SetLabel(labels.POD, "1234-123-123-1234", "color", "red")
	store.applicator.SetLabel(labels.POD, "abcd-abc-abc-abcd", "color", "blue")

	syncer := &fakeSyncer{
		[]fields.ID{},
		make(chan fakeSync),
		make(chan fakeSync),
		false,
	}

	// Previous == current, simulates a concrete syncer starting up
	change := podClusterChange{
		previous: &fields.PodCluster{
			ID:               fields.ID("abc123"),
			PodID:            types.PodID("vvv"),
			AvailabilityZone: fields.AvailabilityZone("west"),
			Name:             "production",
			PodSelector:      klabels.Everything().Add("color", klabels.EqualsOperator, []string{"red"}),
		},
		current: &fields.PodCluster{
			ID:               fields.ID("abc123"),
			PodID:            types.PodID("vvv"),
			AvailabilityZone: fields.AvailabilityZone("west"),
			Name:             "production",
			PodSelector:      klabels.Everything().Add("color", klabels.EqualsOperator, []string{"red"}),
		},
	}

	changes := make(chan podClusterChange)
	go store.handlePCUpdates(syncer, changes, metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)))

	select {
	case changes <- change:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to write change to handlePCChange")
	}

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to read from the syncer")
	case sync := <-syncer.synced:
		if sync.syncedCluster == nil {
			t.Fatal("unexpectedly didn't get a cluster on the sync channel")
		}
		if sync.syncedCluster.ID != change.current.ID {
			t.Fatalf("got unexpected synced cluster %v", sync.syncedCluster.ID)
		}
		if len(sync.syncedPods) != 1 {
			t.Fatalf("got unexpected number of synced pods with cluster: %v", len(sync.syncedPods))
		}
		if sync.syncedPods[0].ID != "1234-123-123-1234" {
			t.Fatalf("got unexpected pod ID from labeled pods sync: %v", sync.syncedPods[0].ID)
		}
	}

	// now we send a new update that changes the pod cluster's target pod from the red one to the blue one.
	// (from 1234-123-123-1234 to abcd-abc-abc-abcd )
	change = podClusterChange{
		previous: change.current,
		current: &fields.PodCluster{
			ID:               fields.ID("abc123"),
			PodID:            types.PodID("vvv"),
			AvailabilityZone: fields.AvailabilityZone("west"),
			Name:             "production",
			PodSelector:      klabels.Everything().Add("color", klabels.EqualsOperator, []string{"blue"}),
		},
	}

	select {
	case changes <- change:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to write change to handlePCChange")
	}

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to read from the syncer")
	case sync := <-syncer.synced:
		if sync.syncedCluster == nil {
			t.Fatal("unexpectedly didn't get a cluster on the sync channel")
		}
		if sync.syncedCluster.ID != change.current.ID {
			t.Fatalf("got unexpected synced cluster %v", sync.syncedCluster.ID)
		}
		if len(sync.syncedPods) != 1 {
			t.Fatalf("got unexpected number of synced pods with cluster: %v", len(sync.syncedPods))
		}
		if sync.syncedPods[0].ID != "abcd-abc-abc-abcd" {
			t.Fatalf("got unexpected pod ID from labeled pods sync: %v", sync.syncedPods[0].ID)
		}
	}

	// appear to have deleted the cluster
	change.previous = change.current
	change.current = nil

	select {
	case changes <- change:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to write deletion change to handlePCChange")
	}

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to read from the syncer")
	case sync := <-syncer.deleted:
		if sync.syncedCluster == nil {
			t.Fatal("unexpectedly didn't get a cluster on the sync channel")
		}
		if sync.syncedCluster.ID != change.previous.ID {
			t.Fatalf("got unexpected synced cluster %v", sync.syncedCluster.ID)
		}
	}

	close(changes)
}
func TestInitialClusters(t *testing.T) {
	store := consulStoreWithFakeKV()

	syncer := &fakeSyncer{
		[]fields.ID{"abc-123"},
		make(chan fakeSync),
		make(chan fakeSync),
		false,
	}

	clusters, err := store.getInitialClusters(syncer)

	if err != nil {
		t.Fatalf("Did not expect error to occur getting clusters: %v", err)
	}

	if len(clusters.Clusters) != 1 {
		t.Fatalf("Got unexpected number of clusters (%v)", len(clusters.Clusters))
	}

	if clusters.Clusters[0].ID != fields.ID("abc-123") {
		t.Fatalf("Got unexpected initial cluster %v", clusters.Clusters[0].ID)
	}
}

func TestLockForSync(t *testing.T) {
	id := fields.ID("abc123")
	store := consulStoreWithFakeKV()
	syncerType := ConcreteSyncerType("some_syncer")
	session := kptest.NewSession()

	unlocker, err := store.LockForSync(id, syncerType, session)
	if err != nil {
		t.Fatalf("Unexpected error locking pod cluster for sync: %s", err)
	}

	_, err = store.LockForSync(id, syncerType, session)
	if err == nil {
		t.Fatal("Expected an error locking the same cluster for the same syncer type, but there wasn't one")
	} else {
		if !consulutil.IsAlreadyLocked(err) {
			t.Errorf("Expected error to be an already locked error, was %s", err)
		}
	}

	err = unlocker.Unlock()
	if err != nil {
		t.Errorf("Error unlocking the sync lock: %s", err)
	}

	_, err = store.LockForSync(id, syncerType, session)
	if err != nil {
		t.Fatalf("Unexpected error re-locking pod cluster for sync: %s", err)
	}
}

func TestClosedChangeChannelResultsInTermination(t *testing.T) {
	store := consulStoreWithFakeKV()

	syncer := &fakeSyncer{
		[]fields.ID{"abc123"},
		make(chan fakeSync),
		make(chan fakeSync),
		false,
	}

	changes := make(chan podClusterChange)
	close(changes)

	closed := make(chan struct{})

	go func() {
		store.handlePCUpdates(syncer, changes, metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)))
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting to observe a terminated PC update routine")
	}
}

type RecordingSyncer struct {
	InitialClusters []fields.ID

	SyncClusterCalls   []fields.ID
	DeleteClusterCalls []fields.ID

	// Used to signal when SyncCluster is called so tests can have non-racey
	// timeouts
	SyncSignal chan<- struct{}
}

func (r *RecordingSyncer) GetInitialClusters() ([]fields.ID, error) {
	return r.InitialClusters, nil
}

func (r *RecordingSyncer) SyncCluster(pc *fields.PodCluster, labeledPods []labels.Labeled) error {
	r.SyncClusterCalls = append(r.SyncClusterCalls, pc.ID)
	if r.SyncSignal != nil {
		r.SyncSignal <- struct{}{}
	}
	return nil
}

func (r *RecordingSyncer) DeleteCluster(id fields.ID) error {
	r.DeleteClusterCalls = append(r.DeleteClusterCalls, id)
	return nil
}

func (r *RecordingSyncer) Type() ConcreteSyncerType {
	return "recording"
}

func TestWatchAndSync(t *testing.T) {
	store := consulStoreWithFakeKV()
	quit := make(chan struct{})
	defer close(quit)
	syncSignal := make(chan struct{})
	defer close(syncSignal)

	example := examplePodCluster()
	pc1, err := store.Create(
		example.PodID,
		example.AvailabilityZone,
		"name1",
		example.PodSelector,
		example.Annotations,
		kptest.NewSession(),
	)
	if err != nil {
		t.Fatalf("Couldn't create test pod cluster: %s", err)
	}

	pc2, err := store.Create(
		example.PodID,
		example.AvailabilityZone,
		"name2",
		example.PodSelector,
		example.Annotations,
		kptest.NewSession(),
	)
	if err != nil {
		t.Fatalf("Couldn't create test pod cluster: %s", err)
	}

	// Include one of the 2 clusters as initial
	syncer := &RecordingSyncer{
		InitialClusters: []fields.ID{pc1.ID},
		SyncSignal:      syncSignal,
	}

	go func() {
		err := store.WatchAndSync(syncer, quit)
		if err != nil {
			t.Fatalf("Couldn't start WatchAndSync(): %s", err)
		}
	}()

	expectedSyncCount := 2
	actualSyncCount := 0
	for actualSyncCount < expectedSyncCount {
		select {
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for sync to happen, there were %d syncs: %s", actualSyncCount, syncer.SyncClusterCalls)
		case <-syncSignal:
			actualSyncCount++
		}
	}

	for _, expectedID := range []fields.ID{pc1.ID, pc2.ID} {
		found := false
		for _, id := range syncer.SyncClusterCalls {
			if id == expectedID {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("Expected sync to be called for %s but it wasn't. Called for %s", expectedID, syncer.SyncClusterCalls)
		}
	}
}

func consulStoreWithFakeKV() *consulStore {
	return &consulStore{
		kv:         consulutil.NewFakeClient().KV(),
		applicator: labels.NewFakeApplicator(),
		logger:     logging.DefaultLogger,
	}
}

func examplePodCluster() fields.PodCluster {
	podId := "slug"
	availabilityZone := "us-west"
	clusterName := "production"

	return fields.PodCluster{
		PodID:            types.PodID(podId),
		AvailabilityZone: fields.AvailabilityZone(availabilityZone),
		Name:             fields.ClusterName(clusterName),
		PodSelector:      klabels.Everything(),
		Annotations:      fields.Annotations{},
	}
}
