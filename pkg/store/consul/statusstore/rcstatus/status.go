package rcstatus

import (
	"encoding/json"

	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

type RCStatus struct {
	NodeTransfer NodeTransfer `json:"node_transfer"`
}

type NodeTransfer struct {
	OldNode types.NodeName `json:"old_node"`
	NewNode types.NodeName `json:"new_node"`
}

func statusToRCStatus(rawStatus statusstore.Status) (RCStatus, error) {
	var rcStatus RCStatus

	err := json.Unmarshal(rawStatus.Bytes(), &rcStatus)
	if err != nil {
		return RCStatus{}, util.Errorf("Could not unmarshal raw status as rc status: %s", err)
	}

	return rcStatus, nil
}

func rcStatusToStatus(rcStatus RCStatus) (statusstore.Status, error) {
	bytes, err := json.Marshal(rcStatus)
	if err != nil {
		return statusstore.Status{}, util.Errorf("Could not marshal rc status as json bytes: %s", err)
	}

	return statusstore.Status(bytes), nil
}
