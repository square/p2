package podstatus

import (
	"encoding/json"
	"time"

	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/util"
)

type PodState string

func (p PodState) String() string { return string(p) }

const (
	// Signifies that the pod has been launched
	PodLaunched PodState = "launched"

	// Signifies that the pod has been unscheduled and removed from the machine
	PodRemoved PodState = "removed"

	// PodFailed denotes a pod that failed. The definition of "failed" is
	// complex as there are potentially many processes spawned by a pod,
	// some of which may be configured to restart on failure. As a result
	// of this, it is the responsibility of whatever system scheduled a pod
	// in the first place to mark a pod as failed. It is not done within P2
	// itself. This constant is only defined for convenience.
	PodFailed PodState = "failed"
)

// Encapsulates information relating to the exit of a process.
type ExitStatus struct {
	ExitTime   time.Time `json:"time"`
	ExitCode   int       `json:"exit_code"`
	ExitStatus int       `json:"exit_status"`
}

// Encapsulates information regarding the state of a process. Currently only
// information about the last exit is exposed.
type ProcessStatus struct {
	LaunchableID launch.LaunchableID `json:"launchable_id"`
	EntryPoint   string              `json:"entry_point"`
	LastExit     *ExitStatus         `json:"last_exit"`
}

// Encapsulates the state of all processes running in a pod.
type PodStatus struct {
	ProcessStatuses []ProcessStatus `json:"process_status"`
	PodStatus       PodState        `json:"status"`

	// String representing the pod manifest for the running pod. Will be
	// empty if it hasn't yet been launched
	Manifest string `json:"manifest"`
}

func statusToPodStatus(rawStatus statusstore.Status) (PodStatus, error) {
	var podStatus PodStatus

	err := json.Unmarshal(rawStatus.Bytes(), &podStatus)
	if err != nil {
		return PodStatus{}, util.Errorf("Could not unmarshal raw status as pod status: %s", err)
	}

	return podStatus, nil
}

func podStatusToStatus(podStatus PodStatus) (statusstore.Status, error) {
	bytes, err := json.Marshal(podStatus)
	if err != nil {
		return statusstore.Status{}, util.Errorf("Could not marshal pod status as json bytes: %s", err)
	}

	return statusstore.Status(bytes), nil
}
