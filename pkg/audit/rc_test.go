package audit

import (
	"encoding/json"
	"reflect"
	"testing"

	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
)

func TestRCRetargetingEventDetails(t *testing.T) {
	podID := types.PodID("some_pod_id")
	clusterName := pc_fields.ClusterName("some_cluster_name")
	az := pc_fields.AvailabilityZone("some_availability_zone")
	nodes := []types.NodeName{"node1", "node2"}

	detailsJSON, err := NewRCRetargetingEventDetails(podID, az, clusterName, nodes)
	if err != nil {
		t.Fatal(err)
	}

	var details RCRetargetingDetails
	err = json.Unmarshal(detailsJSON, &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.PodID != podID {
		t.Errorf("expected pod id to be %s but was %s", podID, details.PodID)
	}

	if details.AvailabilityZone != az {
		t.Errorf("expected availability zone to be %s but was %s", az, details.AvailabilityZone)
	}

	if details.ClusterName != clusterName {
		t.Errorf("expected cluster name to be %s but was %s", clusterName, details.ClusterName)
	}

	if !reflect.DeepEqual(details.Nodes, nodes) {
		t.Errorf("expected node list to be %s but was %s", nodes, details.Nodes)
	}
}
