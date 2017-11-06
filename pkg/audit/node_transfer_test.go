package audit

import (
	"encoding/json"
	"testing"

	rcfields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/types"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestNewNodeTransferStartDetails(t *testing.T) {
	rcID := rcfields.ID("some_rc_id")
	nodeSelector := klabels.Everything().Add("node", klabels.EqualsOperator, []string{"good"})
	oldNode := types.NodeName("broken_node")
	newNode := types.NodeName("functional_node")
	replicaCount := 8000

	jsonMessage, err := NewNodeTransferStartDetails(
		rcID,
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
}

func TestNewNodeTransferCompletionDetails(t *testing.T) {
	rcID := rcfields.ID("some_rc_id")
	nodeSelector := klabels.Everything().Add("node", klabels.EqualsOperator, []string{"good"})
	oldNode := types.NodeName("broken_node")
	newNode := types.NodeName("functional_node")
	replicaCount := 8000

	jsonMessage, err := NewNodeTransferCompletionDetails(
		rcID,
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
}

func TestNewNodeTransferRollbackDetails(t *testing.T) {
	rcID := rcfields.ID("some_rc_id")
	nodeSelector := klabels.Everything().Add("node", klabels.EqualsOperator, []string{"good"})
	oldNode := types.NodeName("broken_node")
	newNode := types.NodeName("functional_node")
	replicaCount := 8000
	rollbackReason := RollbackReason("they told me not to!")

	jsonMessage, err := NewNodeTransferRollbackDetails(
		rcID,
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
}
