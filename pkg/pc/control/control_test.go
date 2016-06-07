package control

import (
	"encoding/json"
	"testing"

	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/kp/pcstore/pcstoretest"
	"github.com/square/p2/pkg/pc/fields"
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
	session := kptest.NewSession()
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
