package podstatus

import (
	"time"
)

type FinishState string
type ServiceState string

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
}
