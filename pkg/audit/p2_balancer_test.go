package audit

import (
	"encoding/json"
	"testing"

	pcfields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
)

func TestP2BalancerInProgressDetails(t *testing.T) {
	jobID := JobID("some_job_id")
	podID := types.PodID("some_pod_id")
	az := pcfields.AvailabilityZone("some_az")
	cn := pcfields.ClusterName("some_cn")
	oldNode := types.NodeName("unhealthy_node")
	newNode := types.NodeName("node_with_capacity")

	jsonMessage, err := NewP2BalancerInProgressDetails(
		jobID,
		podID,
		az,
		cn,
		oldNode,
		newNode,
	)
	if err != nil {
		t.Fatal(err)
	}

	var details P2BalancerInProgressDetails
	err = json.Unmarshal(jsonMessage, &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.JobID != jobID {
		t.Errorf("expected Job ID to be %q but was %q", jobID, details.JobID)
	}
	if details.PodID != podID {
		t.Errorf("expected pod ID to be %q but was %q", podID, details.PodID)
	}
	if details.AvailabilityZone != az {
		t.Errorf("expected availability zone to be %q but was %q", az, details.AvailabilityZone)
	}
	if details.ClusterName != cn {
		t.Errorf("expected cluster name to be %q but was %q", cn, details.ClusterName)
	}
	if details.OldNode != oldNode {
		t.Errorf("expected old node to be %q but was %q", oldNode, details.OldNode)
	}
	if details.NewNode != newNode {
		t.Errorf("expected new node to be %q but was %q", newNode, details.NewNode)
	}
}

func TestP2BalancerCompletionDetails(t *testing.T) {
	jobID := JobID("some_job_id")
	podID := types.PodID("some_pod_id")
	az := pcfields.AvailabilityZone("some_az")
	cn := pcfields.ClusterName("some_cn")
	oldNode := types.NodeName("unhealthy_node")
	newNode := types.NodeName("node_with_capacity")

	jsonMessage, err := NewP2BalancerCompletionDetails(
		jobID,
		podID,
		az,
		cn,
		oldNode,
		newNode,
	)
	if err != nil {
		t.Fatal(err)
	}

	var details P2BalancerCompletionDetails
	err = json.Unmarshal(jsonMessage, &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.JobID != jobID {
		t.Errorf("expected Job ID to be %q but was %q", jobID, details.JobID)
	}
	if details.PodID != podID {
		t.Errorf("expected pod ID to be %q but was %q", podID, details.PodID)
	}
	if details.AvailabilityZone != az {
		t.Errorf("expected availability zone to be %q but was %q", az, details.AvailabilityZone)
	}
	if details.ClusterName != cn {
		t.Errorf("expected cluster name to be %q but was %q", cn, details.ClusterName)
	}
	if details.OldNode != oldNode {
		t.Errorf("expected old node to be %q but was %q", oldNode, details.OldNode)
	}
	if details.NewNode != newNode {
		t.Errorf("expected new node to be %q but was %q", newNode, details.NewNode)
	}
}

func TestP2BalancerFailureDetails(t *testing.T) {
	jobID := JobID("some_job_id")
	podID := types.PodID("some_pod_id")
	az := pcfields.AvailabilityZone("some_az")
	cn := pcfields.ClusterName("some_cn")
	oldNode := types.NodeName("unhealthy_node")
	newNode := types.NodeName("node_with_capacity")
	errorMessage := P2BalancerErrorMessage("some_error_message")

	jsonMessage, err := NewP2BalancerFailureDetails(
		jobID,
		podID,
		az,
		cn,
		oldNode,
		newNode,
		errorMessage,
	)
	if err != nil {
		t.Fatal(err)
	}

	var details P2BalancerFailureDetails
	err = json.Unmarshal(jsonMessage, &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.JobID != jobID {
		t.Errorf("expected Job ID to be %q but was %q", jobID, details.JobID)
	}
	if details.PodID != podID {
		t.Errorf("expected pod ID to be %q but was %q", podID, details.PodID)
	}
	if details.AvailabilityZone != az {
		t.Errorf("expected availability zone to be %q but was %q", az, details.AvailabilityZone)
	}
	if details.ClusterName != cn {
		t.Errorf("expected cluster name to be %q but was %q", cn, details.ClusterName)
	}
	if details.OldNode != oldNode {
		t.Errorf("expected old node to be %q but was %q", oldNode, details.OldNode)
	}
	if details.NewNode != newNode {
		t.Errorf("expected new node to be %q but was %q", newNode, details.NewNode)
	}
	if details.ErrorMessage != errorMessage {
		t.Errorf("expected error message to be %q but was %q", errorMessage, details.ErrorMessage)
	}
}
