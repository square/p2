package audit

import (
	"encoding/json"

	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

const (
	RUCreationEvent EventType = "ROLLING_UPDATE_CREATION"
)

type RUCreationDetails struct {
	PodID            types.PodID             `json:"pod_id"`
	AvailabilityZone fields.AvailabilityZone `json:"availability_zone"`
	ClusterName      fields.ClusterName      `json:"cluster_name"`
	Deployer         string                  `json:"deployer"`
	Manifest         string                  `json:"manifest"`
}

func NewRUCreationEventDetails(
	podID types.PodID,
	az fields.AvailabilityZone,
	name fields.ClusterName,
	deployer string,
	manifest manifest.Manifest,
) (json.RawMessage, error) {
	manifestBytes, err := manifest.Marshal()
	if err != nil {
		return nil, err
	}

	details := RUCreationDetails{
		PodID:            podID,
		AvailabilityZone: az,
		ClusterName:      name,
		Deployer:         deployer,
		Manifest:         string(manifestBytes),
	}

	bytes, err := json.Marshal(details)
	if err != nil {
		return nil, util.Errorf("could not marshal ru creation details as json: %s", err)
	}

	return json.RawMessage(bytes), nil
}
