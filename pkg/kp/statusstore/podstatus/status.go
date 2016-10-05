package podstatus

import (
	"encoding/json"
	"time"

	"github.com/square/p2/pkg/kp/statusstore"
	"github.com/square/p2/pkg/util"
)

type FinishState string
type ServiceState string
type PodState string

const (
	// Indicates the state of a completion of a service
	Successful FinishState = "successful"
	Failed     FinishState = "failed"

	// Indicates the state of a service irrespective of success. The reason for
	// this separation is that a service in P2 might have an aggressive restart
	// policy making "success" and "failure" not very useful states as they are
	// momentary.
	Pending  ServiceState = "pending"
	Running  ServiceState = "running"
	Finished ServiceState = "finished"

	PodLaunched PodState = "launched"
	PodPending  PodState = "pending"
	PodRemoved  PodState = "removed"
)

// Encapsulates information relating to the exit of a service.
type ExitStatus struct {
	ExitTime time.Time   `json:"time"`
	Status   FinishState `json:"state"`
	ExitCode int         `json:"exit_code"`
}

// Encapsulates information regarding the state of a service. This includes
// information about a service that might still be running as well as its most
// recent exit. This is because many services under P2 are always restarted
// after exit by runit, and both the status of the most recent exit as well as
// the current status of the service are useful to expose
type ServiceStatus struct {
	Name     string       `json:"name"`
	State    ServiceState `json:"state"`
	LastExit *ExitStatus  `json:"last_exit"`
}

// Encapsulates the state of all services running in a pod.
type PodStatus struct {
	ServiceStatus []ServiceStatus `json:"service_status"`
	PodStatus     PodState        `json:"status"`

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
