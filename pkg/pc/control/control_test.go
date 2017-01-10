package control

import (
	"encoding/json"
	"testing"

	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/store/consul/consultest"
	"github.com/square/p2/pkg/store/consul/pcstore"
	"github.com/square/p2/pkg/store/consul/pcstore/pcstoretest"
	"github.com/square/p2/pkg/types"
	"k8s.io/kubernetes/pkg/labels"
)

func TestCreate(t *testing.T) {
	testAZ := fields.AvailabilityZone("west-coast")
	testCN := fields.ClusterName("test")
	testPodID := types.PodID("pod")
	selector := labels.Everything().
		Add(fields.PodIDLabel, labels.EqualsOperator, []string{testPodID.String()}).
		Add(fields.AvailabilityZoneLabel, labels.EqualsOperator, []string{testAZ.String()}).
		Add(fields.ClusterNameLabel, labels.EqualsOperator, []string{testCN.String()})
	session := consultest.NewSession()
	pcstore := pcstoretest.NewFake()

	pcController := NewPodCluster(testAZ, testCN, testPodID, pcstore, selector, session)

	annotations := map[string]string{
		"load_balancer_info": "totally",
		"pager_information":  "555-111-2222",
	}

	buf, err := json.Marshal(annotations)
	if err != nil {
		t.Errorf("json marshal error: %v", err)
	}

	var testAnnotations fields.Annotations
	if err := json.Unmarshal(buf, &testAnnotations); err != nil {
		t.Errorf("json unmarshal error: %v", err)
	}

	pc, err := pcController.Create(fields.Annotations(testAnnotations))
	if err != nil {
		t.Errorf("got error during creation: %v", err)
	}
	if pc.ID == "" {
		t.Error("got empty pc ID")
	}

	if pc.PodID != testPodID {
		t.Errorf("Expected to get %s, got: %v", pc.PodID, testPodID)
	}

	if pc.Name != testCN {
		t.Errorf("Expected to get %s, got: %v", testCN, pc.Name)
	}

	if pc.AvailabilityZone != testAZ {
		t.Errorf("Expected to get %s, got: %v", testAZ, pc.AvailabilityZone)
	}

	if pc.PodSelector.String() != selector.String() {
		t.Errorf("Expected to get %s, got: %v", selector, pc.PodSelector)
	}

	if pc.Annotations["load_balancer_info"] != testAnnotations["load_balancer_info"] {
		t.Errorf("Expected to get %s, got: %v", testAnnotations, pc.Annotations)
	}

	if pc.Annotations["pager_information"] != testAnnotations["pager_information"] {
		t.Errorf("Expected to get %s, got: %v", testAnnotations, pc.Annotations)
	}
}

func TestUpdate(t *testing.T) {
	testAZ := fields.AvailabilityZone("west-coast")
	testCN := fields.ClusterName("test")
	testPodID := types.PodID("pod")
	selector := labels.Everything().
		Add(fields.PodIDLabel, labels.EqualsOperator, []string{testPodID.String()}).
		Add(fields.AvailabilityZoneLabel, labels.EqualsOperator, []string{testAZ.String()}).
		Add(fields.ClusterNameLabel, labels.EqualsOperator, []string{testCN.String()})
	session := consultest.NewSession()
	pcstore := pcstoretest.NewFake()

	pcController := NewPodCluster(testAZ, testCN, testPodID, pcstore, selector, session)

	var annotations = map[string]string{
		"load_balancer_info": "totally",
		"pager_information":  "555-111-2222",
	}

	buf, err := json.Marshal(annotations)
	if err != nil {
		t.Errorf("json marshal error: %v", err)
	}

	var testAnnotations fields.Annotations
	if err := json.Unmarshal(buf, &testAnnotations); err != nil {
		t.Errorf("json unmarshal error: %v", err)
	}

	pc, err := pcController.Create(fields.Annotations(testAnnotations))
	if err != nil {
		t.Fatalf("Unable to create pod cluster due to: %v", err)
	}

	newAnnotations := map[string]string{
		"pager_information": "555-111-2222",
		"priority":          "1001",
	}

	buf, err = json.Marshal(newAnnotations)
	if err != nil {
		t.Errorf("json marshal error: %v", err)
	}

	var newTestAnnotations fields.Annotations
	if err := json.Unmarshal(buf, &newTestAnnotations); err != nil {
		t.Errorf("json unmarshal error: %v", err)
	}

	pc, err = pcController.Update(newTestAnnotations)
	if err != nil {
		t.Fatalf("Got error updating PC annotations: %v", err)
	}

	if pc.Annotations["pager_information"] != newAnnotations["pager_information"] {
		t.Errorf("Got unexpected pager_information. Expected %s, got %s", newAnnotations["pager_information"], pc.Annotations["pager_information"])
	}

	if pc.Annotations["priority"] != newAnnotations["priority"] {
		t.Errorf("Got unexpected priority. Expected %s, got %s", newAnnotations["priority"], pc.Annotations["priority"])
	}

	if pc.Annotations["load_balancer_info"] != nil {
		t.Errorf("Expected to erase old annotation field. Instead we have: %s", pc.Annotations["load_balancer_info"])
	}
}

func TestPodClusterFromID(t *testing.T) {
	testAZ := fields.AvailabilityZone("west-coast")
	testCN := fields.ClusterName("test")
	testPodID := types.PodID("pod")
	selector := labels.Everything().
		Add(fields.PodIDLabel, labels.EqualsOperator, []string{testPodID.String()}).
		Add(fields.AvailabilityZoneLabel, labels.EqualsOperator, []string{testAZ.String()}).
		Add(fields.ClusterNameLabel, labels.EqualsOperator, []string{testCN.String()})
	session := consultest.NewSession()
	fakePCStore := pcstoretest.NewFake()

	pcControllerFromLabels := NewPodCluster(testAZ, testCN, testPodID, fakePCStore, selector, session)
	pc, err := pcControllerFromLabels.Create(fields.Annotations{})
	if err != nil {
		t.Fatal(err)
	}
	pcControllerFromLabels = nil

	pcControllerFromID := NewPodClusterFromID(pc.ID, session, fakePCStore)
	retrievedPC, err := pcControllerFromID.Get()
	if err != nil {
		t.Fatal(err)
	}
	if pc.ID != retrievedPC.ID {
		t.Errorf("Did not get correct PC back from datastore, expected %s, got %s.\n%v", pc.ID, retrievedPC.ID, retrievedPC)
	}

	errs := pcControllerFromID.Delete()
	if len(errs) > 0 {
		t.Fatalf("%v", errs)
	}

	notFoundPC, err := pcControllerFromID.Get()
	if err != pcstore.NoPodCluster {
		t.Errorf("Expected to get pcstore.NoPodCluster, but got %v", err)
	}

	if notFoundPC.ID != "" {
		t.Errorf("Expected to not find PC but found %v", notFoundPC)
	}
}
