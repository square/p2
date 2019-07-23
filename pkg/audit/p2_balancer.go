package audit

import (
	"encoding/json"

	"github.com/square/p2/pkg/util"

	pcfields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
)

type P2BalancerErrorMessage string
type JobID string

const (
	// P2BalancerInProgressEvent denotes the start of a P2 Balancer pod move
	P2BalancerInProgressEvent EventType = "P2_BALANCER_START"

	// P2BalancerCompletionEvent denotes the successful completion of a
	// pod move
	P2BalancerCompletionEvent EventType = "P2_BALANCER_COMPLETION"

	// P2BalancerFailureEvent denotes a pod move failure due
	// to unrecoverable errors
	P2BalancerFailureEvent EventType = "P2_BALANCER_FAILURE"
)

type CommonP2BalancerDetails struct {
	// JobID is a uuid that will be the same for all audit log records
	// associated with the same pod move. It can be used to match up
	// "in progress" events with "failure" or "completion" events
	JobID JobID `json:"job_id"`

	// PodID denotes the pod ID of the pod cluster that the RC belongs to
	PodID types.PodID `json:"pod_id"`

	// AvailabilityZone is the availability zone of the pod cluster that
	// the RC belongs to
	AvailabilityZone pcfields.AvailabilityZone `json:"availability_zone"`

	// ClusterName is the name of the pod cluster that the RC belongs to
	ClusterName pcfields.ClusterName `json:"cluster_name"`

	// OldNode denotes the node that is above healthy threshold
	// and needs a pod moved off of it
	OldNode types.NodeName `json:"old_node"`

	// NewNode denotes the node that has resource capacity for a pod to be moved onto it
	NewNode types.NodeName `json:"new_node"`
}

type P2BalancerInProgressDetails struct {
	CommonP2BalancerDetails
}

type P2BalancerCompletionDetails struct {
	CommonP2BalancerDetails
}

type P2BalancerFailureDetails struct {
	CommonP2BalancerDetails

	// This will be set to the error message at the point of failure
	ErrorMessage P2BalancerErrorMessage `json:"error_message"`
}

func NewP2BalancerInProgressDetails(
	jobID JobID,
	podID types.PodID,
	availabilityZone pcfields.AvailabilityZone,
	clusterName pcfields.ClusterName,
	oldNode types.NodeName,
	newNode types.NodeName,
) (json.RawMessage, error) {
	details := P2BalancerInProgressDetails{
		CommonP2BalancerDetails: commonP2BalancerDetails(
			jobID,
			podID,
			availabilityZone,
			clusterName,
			oldNode,
			newNode,
		),
	}

	jsonBytes, err := json.Marshal(details)
	if err != nil {
		return nil, util.Errorf("could not marshal p2 balancer pod move in progress details as JSON: %s", err)
	}

	return json.RawMessage(jsonBytes), nil
}

func NewP2BalancerCompletionDetails(
	jobID JobID,
	podID types.PodID,
	availabilityZone pcfields.AvailabilityZone,
	clusterName pcfields.ClusterName,
	oldNode types.NodeName,
	newNode types.NodeName,
) (json.RawMessage, error) {
	details := P2BalancerCompletionDetails{
		CommonP2BalancerDetails: commonP2BalancerDetails(
			jobID,
			podID,
			availabilityZone,
			clusterName,
			oldNode,
			newNode,
		),
	}

	jsonBytes, err := json.Marshal(details)
	if err != nil {
		return nil, util.Errorf("could not marshal p2 balancer pod move completion details as JSON: %s", err)
	}

	return json.RawMessage(jsonBytes), nil
}

func NewP2BalancerFailureDetails(
	jobID JobID,
	podID types.PodID,
	availabilityZone pcfields.AvailabilityZone,
	clusterName pcfields.ClusterName,
	oldNode types.NodeName,
	newNode types.NodeName,
	errorMessage P2BalancerErrorMessage,
) (json.RawMessage, error) {
	details := P2BalancerFailureDetails{
		CommonP2BalancerDetails: commonP2BalancerDetails(
			jobID,
			podID,
			availabilityZone,
			clusterName,
			oldNode,
			newNode,
		),
		ErrorMessage: errorMessage,
	}

	jsonBytes, err := json.Marshal(details)
	if err != nil {
		return nil, util.Errorf("could not marshal p2 balancer pod move failure details as JSON: %s", err)
	}

	return json.RawMessage(jsonBytes), nil
}

func commonP2BalancerDetails(
	jobID JobID,
	podID types.PodID,
	availabilityZone pcfields.AvailabilityZone,
	clusterName pcfields.ClusterName,
	oldNode types.NodeName,
	newNode types.NodeName,
) CommonP2BalancerDetails {
	return CommonP2BalancerDetails{
		JobID:            jobID,
		PodID:            podID,
		AvailabilityZone: availabilityZone,
		ClusterName:      clusterName,
		OldNode:          oldNode,
		NewNode:          newNode,
	}
}
