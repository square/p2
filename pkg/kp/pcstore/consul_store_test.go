package pcstore

import (
	"testing"

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

	pc, err := store.Create(podID, az, clusterName, selector, annotations)
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

	pc, err := store.Create(podID, az, clusterName, selector, annotations)
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
	pc, err := store.Create(podID, az, clusterName, selector, annotations)
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
}

func consulStoreWithFakeKV() *consulStore {
	return &consulStore{
		kv:         kptest.NewFakeKV(),
		applicator: labels.NewFakeApplicator(),
	}
}
