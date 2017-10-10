package rcstatus

import (
	"encoding/json"

	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

type Status struct {
	NodeTransfer *NodeTransfer `json:"node_transfer"`
}

type NodeTransfer struct {
	OldNode types.NodeName `json:"old_node"`
	NewNode types.NodeName `json:"new_node"`
}

func rawStatusToStatus(rawStatus statusstore.Status) (Status, error) {
	var status Status

	err := json.Unmarshal(rawStatus.Bytes(), &status)
	if err != nil {
		return Status{}, util.Errorf("Could not unmarshal raw status as rc status: %s", err)
	}

	return status, nil
}

func statusToRawStatus(status Status) (statusstore.Status, error) {
	bytes, err := json.Marshal(status)
	if err != nil {
		return statusstore.Status{}, util.Errorf("Could not marshal rc status as json bytes: %s", err)
	}

	return statusstore.Status(bytes), nil
}
