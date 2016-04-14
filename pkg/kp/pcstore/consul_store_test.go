package pcstore

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"

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

	quit := make(chan struct{})
	watch := store.Watch(quit)

	var watched *WatchedPodCluster
	session := kptest.NewSession()
	pc, err := store.Create(podID, az, clusterName, selector, annotations, session)
	if err != nil {
		t.Fatalf("Unable to create pod cluster: %s", err)
	}

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

func consulStoreWithFakeKV() *consulStore {
	return &consulStore{
		kv:         kptest.NewFakeKV(),
		applicator: labels.NewFakeApplicator(),
	}
}
