package pcstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

func TestCreate(t *testing.T) {
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

	session := kptest.NewSession()
	pc, err := store.Create(podID, az, clusterName, selector, annotations, session)
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

	if pc.ID == "" {
		t.Errorf("pod cluster should have an id")
	}

	if pc.PodID == "" {
		t.Errorf("pod cluster should have an id")
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
		t.Errorf("pod cluster should have an id")
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

	if watched.Clusters[0].ID != pc.ID {
		t.Fatalf("Expected first watched PodCluster to match %s Pod Cluster ID. Instead was %s", pc.ID, watched.Clusters[0].ID)
	}

	if watched.Clusters[1].ID != pc2.ID {
		t.Fatalf("Expected second watched PodCluster to match %s Pod Cluster ID. Instead was %s", pc2.ID, watched.Clusters[1].ID)
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

func (f *fakeSyncer) DeleteCluster(cluster *fields.PodCluster) error {
	f.deleted <- fakeSync{
		syncedCluster: cluster,
	}
	return nil
}

func (f *fakeSyncer) drainSyncedAndIgnore() {
	f.ignore = true
	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			panic("Couldn't drain syncer channels")
		case synced := <-f.synced:
			fmt.Printf("fake: Draining result for %v\n", synced)
		default:
			return
		}
	}
}

func (f *fakeSyncer) resume() {
	f.ignore = false
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
	go store.handlePCUpdates(syncer, changes)

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

	syncer.drainSyncedAndIgnore()

	select {
	case changes <- change:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out trying to write change to handlePCChange")
	}

	syncer.resume()

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

	syncer.drainSyncedAndIgnore()

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

func consulStoreWithFakeKV() *consulStore {
	return &consulStore{
		kv:         kptest.NewFakeKV(),
		applicator: labels.NewFakeApplicator(),
		logger:     logging.DefaultLogger,
	}
}
