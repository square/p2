package audit

import (
	"encoding/json"
	"testing"

	pcfields "github.com/square/p2/pkg/pc/fields"
	rcfields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/types"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestNewNodeTransferStartDetails(t *testing.T) {
	nodeTransferID := rcstatus.NodeTransferID("some_node_transfer_id")
	rcID := rcfields.ID("some_rc_id")
	nodeSelector := klabels.Everything().Add("node", klabels.EqualsOperator, []string{"good"})
	oldNode := types.NodeName("broken_node")
	newNode := types.NodeName("functional_node")
	replicaCount := 8000
	podID := types.PodID("some_pod_id")
	az := pcfields.AvailabilityZone("some_az")
	cn := pcfields.ClusterName("some_cn")

	jsonMessage, err := NewNodeTransferStartDetails(
		nodeTransferID,
		rcID,
		podID,
		az,
		cn,
		nodeSelector,
		oldNode,
		newNode,
		replicaCount,
	)
	if err != nil {
		t.Fatal(err)
	}

	var details NodeTransferStartDetails
	err = json.Unmarshal(jsonMessage, &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.NodeTransferID != nodeTransferID {
		t.Errorf("expected nodeTransferID ID to be %q but was %q", nodeTransferID, details.NodeTransferID)
	}
	if details.ReplicationControllerID != rcID {
		t.Errorf("expected rc ID to be %q but was %q", rcID, details.ReplicationControllerID)
	}
	if details.ReplicationControllerID != rcID {
		t.Errorf("expected rc ID to be %q but was %q", rcID, details.ReplicationControllerID)
	}
	if details.RCNodeSelector != nodeSelector.String() {
		t.Errorf("expected node selector to be %q but was %q", nodeSelector.String(), details.RCNodeSelector)
	}
	if details.OldNode != oldNode {
		t.Errorf("expected old node to be %q but was %q", oldNode, details.OldNode)
	}
	if details.NewNode != newNode {
		t.Errorf("expected new node to be %q but was %q", newNode, details.NewNode)
	}
	if details.ReplicaCount != replicaCount {
		t.Errorf("expected replica count to be %q but was %q", replicaCount, details.ReplicaCount)
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
}

func TestNewNodeTransferCompletionDetails(t *testing.T) {
	nodeTransferID := rcstatus.NodeTransferID("some_node_transfer_id")
	rcID := rcfields.ID("some_rc_id")
	nodeSelector := klabels.Everything().Add("node", klabels.EqualsOperator, []string{"good"})
	oldNode := types.NodeName("broken_node")
	newNode := types.NodeName("functional_node")
	replicaCount := 8000
	podID := types.PodID("some_pod_id")
	az := pcfields.AvailabilityZone("some_az")
	cn := pcfields.ClusterName("some_cn")

	jsonMessage, err := NewNodeTransferCompletionDetails(
		nodeTransferID,
		rcID,
		podID,
		az,
		cn,
		nodeSelector,
		oldNode,
		newNode,
		replicaCount,
	)
	if err != nil {
		t.Fatal(err)
	}

	var details NodeTransferCompletionDetails
	err = json.Unmarshal(jsonMessage, &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.NodeTransferID != nodeTransferID {
		t.Errorf("expected nodeTransferID ID to be %q but was %q", nodeTransferID, details.NodeTransferID)
	}
	if details.ReplicationControllerID != rcID {
		t.Errorf("expected rc ID to be %q but was %q", rcID, details.ReplicationControllerID)
	}
	if details.RCNodeSelector != nodeSelector.String() {
		t.Errorf("expected node selector to be %q but was %q", nodeSelector.String(), details.RCNodeSelector)
	}
	if details.OldNode != oldNode {
		t.Errorf("expected old node to be %q but was %q", oldNode, details.OldNode)
	}
	if details.NewNode != newNode {
		t.Errorf("expected new node to be %q but was %q", newNode, details.NewNode)
	}
	if details.ReplicaCount != replicaCount {
		t.Errorf("expected replica count to be %q but was %q", replicaCount, details.ReplicaCount)
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
}

func TestNewNodeTransferRollbackDetails(t *testing.T) {
	nodeTransferID := rcstatus.NodeTransferID("some_node_transfer_id")
	rcID := rcfields.ID("some_rc_id")
	nodeSelector := klabels.Everything().Add("node", klabels.EqualsOperator, []string{"good"})
	oldNode := types.NodeName("broken_node")
	newNode := types.NodeName("functional_node")
	replicaCount := 8000
	rollbackReason := RollbackReason("they told me not to!")
	podID := types.PodID("some_pod_id")
	az := pcfields.AvailabilityZone("some_az")
	cn := pcfields.ClusterName("some_cn")

	jsonMessage, err := NewNodeTransferRollbackDetails(
		nodeTransferID,
		rcID,
		podID,
		az,
		cn,
		nodeSelector,
		oldNode,
		newNode,
		replicaCount,
		rollbackReason,
	)
	if err != nil {
		t.Fatal(err)
	}

	var details NodeTransferRollbackDetails
	err = json.Unmarshal(jsonMessage, &details)
	if err != nil {
		t.Fatal(err)
	}

	if details.NodeTransferID != nodeTransferID {
		t.Errorf("expected nodeTransferID ID to be %q but was %q", nodeTransferID, details.NodeTransferID)
	}
	if details.ReplicationControllerID != rcID {
		t.Errorf("expected rc ID to be %q but was %q", rcID, details.ReplicationControllerID)
	}
	if details.RCNodeSelector != nodeSelector.String() {
		t.Errorf("expected node selector to be %q but was %q", nodeSelector.String(), details.RCNodeSelector)
	}
	if details.OldNode != oldNode {
		t.Errorf("expected old node to be %q but was %q", oldNode, details.OldNode)
	}
	if details.NewNode != newNode {
		t.Errorf("expected new node to be %q but was %q", newNode, details.NewNode)
	}
	if details.ReplicaCount != replicaCount {
		t.Errorf("expected replica count to be %q but was %q", replicaCount, details.ReplicaCount)
	}
	if details.RollbackReason != rollbackReason {
		t.Errorf("expected rollback reason to be %q but was %q", rollbackReason, details.RollbackReason)
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
}
