package audit

import (
	"encoding/json"

	pcfields "github.com/square/p2/pkg/pc/fields"
	rcfields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"
)

type RollbackReason string

const (
	// NodeTransferStartEvent denotes the start of a node transfer
	NodeTransferStartEvent EventType = "NODE_TRANSFER_START"

	// NodeTransferCompletionEvent denotes the successful completion of a
	// node transfer
	NodeTransferCompletionEvent EventType = "NODE_TRANSFER_COMPLETION"

	// NodeTransferRollbackEvent denotes a node transfer being rolled back
	// due to unrecoverable errors or cancellation
	NodeTransferRollbackEvent EventType = "NODE_TRANSFER_ROLLBACK"
)

type CommonNodeTransferDetails struct {
	// NodeTransferID is a uuid that will be the same for all audit log records
	// associated with the same node transfer.  It can be used to match up
	// "start" events with "rollback" or "completion" events
	NodeTransferID rcstatus.NodeTransferID `json:"node_transfer_id"`

	// ReplicationControllerID is the ID of the replication controller that
	// started the node transfer
	ReplicationControllerID rcfields.ID `json:"replication_controller_id"`

	// PodID denotes the pod ID of the pod cluster that the RC belongs to
	PodID types.PodID `json:"pod_id"`

	// AvailabilityZone is the availability zone of the pod cluster that
	// the RC belongs to
	AvailabilityZone pcfields.AvailabilityZone `json:"availability_zone"`

	// ClusterName is the name of the pod cluster that the RC belongs to
	ClusterName pcfields.ClusterName `json:"cluster_name"`

	// RCNodeSelector is the node selector the RC had when the event was
	// created.  This is a klabels.Selector represented as a string because
	// that type does not cleanly marshal into JSON without some tricks
	RCNodeSelector string `json:"replication_controller_node_selector"`

	// OldNode denotes the node that is no longer eligible and should have
	// its pod transferred off of it
	OldNode types.NodeName `json:"old_node"`

	// NewNode denotes the node returned by the scheduler to which a pod is
	// being transferred
	NewNode types.NodeName `json:"new_node"`

	// ReplicaCount denotes the replica count of the RC at the time the
	// node transfer was started
	ReplicaCount int `json:"replica_count"`
}

type NodeTransferStartDetails struct {
	CommonNodeTransferDetails
}

type NodeTransferCompletionDetails struct {
	CommonNodeTransferDetails
}

type NodeTransferRollbackDetails struct {
	CommonNodeTransferDetails

	// RollbackReason indicates why the node transfer was rolled back
	RollbackReason RollbackReason `json:"rollback_reason"`
}

func NewNodeTransferStartDetails(
	nodeTransferID rcstatus.NodeTransferID,
	rcID rcfields.ID,
	podID types.PodID,
	availabilityZone pcfields.AvailabilityZone,
	clusterName pcfields.ClusterName,
	nodeSelector klabels.Selector,
	oldNode types.NodeName,
	newNode types.NodeName,
	replicaCount int,
) (json.RawMessage, error) {
	details := NodeTransferStartDetails{
		CommonNodeTransferDetails: commonNodeTransferDetails(
			nodeTransferID,
			rcID,
			podID,
			availabilityZone,
			clusterName,
			nodeSelector,
			oldNode,
			newNode,
			replicaCount,
		),
	}

	jsonBytes, err := json.Marshal(details)
	if err != nil {
		return nil, util.Errorf("could not marshal node transfer start details as JSON: %s", err)
	}

	return json.RawMessage(jsonBytes), nil
}

func NewNodeTransferCompletionDetails(
	nodeTransferID rcstatus.NodeTransferID,
	rcID rcfields.ID,
	podID types.PodID,
	availabilityZone pcfields.AvailabilityZone,
	clusterName pcfields.ClusterName,
	nodeSelector klabels.Selector,
	oldNode types.NodeName,
	newNode types.NodeName,
	replicaCount int,
) (json.RawMessage, error) {
	details := NodeTransferCompletionDetails{
		CommonNodeTransferDetails: commonNodeTransferDetails(
			nodeTransferID,
			rcID,
			podID,
			availabilityZone,
			clusterName,
			nodeSelector,
			oldNode,
			newNode,
			replicaCount,
		),
	}

	jsonBytes, err := json.Marshal(details)
	if err != nil {
		return nil, util.Errorf("could not marshal node transfer completion details as JSON: %s", err)
	}

	return json.RawMessage(jsonBytes), nil
}

func NewNodeTransferRollbackDetails(
	nodeTransferID rcstatus.NodeTransferID,
	rcID rcfields.ID,
	podID types.PodID,
	availabilityZone pcfields.AvailabilityZone,
	clusterName pcfields.ClusterName,
	nodeSelector klabels.Selector,
	oldNode types.NodeName,
	newNode types.NodeName,
	replicaCount int,
	rollbackReason RollbackReason,
) (json.RawMessage, error) {
	details := NodeTransferRollbackDetails{
		CommonNodeTransferDetails: commonNodeTransferDetails(
			nodeTransferID,
			rcID,
			podID,
			availabilityZone,
			clusterName,
			nodeSelector,
			oldNode,
			newNode,
			replicaCount,
		),
		RollbackReason: rollbackReason,
	}

	jsonBytes, err := json.Marshal(details)
	if err != nil {
		return nil, util.Errorf("could not marshal node transfer rollback details as JSON: %s", err)
	}

	return json.RawMessage(jsonBytes), nil
}

func commonNodeTransferDetails(
	nodeTransferID rcstatus.NodeTransferID,
	rcID rcfields.ID,
	podID types.PodID,
	availabilityZone pcfields.AvailabilityZone,
	clusterName pcfields.ClusterName,
	nodeSelector klabels.Selector,
	oldNode types.NodeName,
	newNode types.NodeName,
	replicaCount int,
) CommonNodeTransferDetails {
	return CommonNodeTransferDetails{
		NodeTransferID:          nodeTransferID,
		ReplicationControllerID: rcID,
		PodID:            podID,
		AvailabilityZone: availabilityZone,
		ClusterName:      clusterName,
		RCNodeSelector:   nodeSelector.String(),
		OldNode:          oldNode,
		NewNode:          newNode,
		ReplicaCount:     replicaCount,
	}
}
