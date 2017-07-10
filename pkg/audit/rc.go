package audit

import (
	"encoding/json"

	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

const (
	// RcRetargetingEvent represents events in which an RC changes the set of
	// nodes that it is targeting. This can be used to log the set of nodes that
	// an RC or pod cluster manages over time
	RCRetargetingEvent EventType = "REPLICATION_CONTROLLER_RETARGET"
)

type RCRetargetingDetails struct {
	PodID            types.PodID                `json:"pod_id"`
	AvailabilityZone pc_fields.AvailabilityZone `json:"availability_zone"`
	ClusterName      pc_fields.ClusterName      `json:"cluster_name"`
	Nodes            []types.NodeName           `json:"nodes"`
}

func NewRCRetargetingEventDetails(
	podID types.PodID,
	az pc_fields.AvailabilityZone,
	name pc_fields.ClusterName,
	nodes []types.NodeName,
) (json.RawMessage, error) {
	details := RCRetargetingDetails{
		PodID:            podID,
		AvailabilityZone: az,
		ClusterName:      name,
		Nodes:            nodes,
	}

	bytes, err := json.Marshal(details)
	if err != nil {
		return nil, util.Errorf("could not marshal rc retargeting details as json: %s", err)
	}

	return json.RawMessage(bytes), nil
}
