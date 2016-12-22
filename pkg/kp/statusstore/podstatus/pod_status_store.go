package podstatus

import (
	"github.com/square/p2/pkg/kp/podstore"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/store"

	"github.com/hashicorp/consul/api"
)

// The Store interface provides easy CRUD functionality for pod status.
// Implementations of the interface will wrap the statusstore.Store interface
// which is generic since the structure of status data for each resource type
// is different
type Store interface {
	Get(store.PodUniqueKey) (PodStatus, *api.QueryMeta, error)

	// Blocks until the watched status key has an index >= waitIndex
	WaitForStatus(podUniqueKey store.PodUniqueKey, waitIndex uint64) (PodStatus, *api.QueryMeta, error)

	GetStatusFromIndex(podstore.PodIndex) (PodStatus, *api.QueryMeta, error)
	Set(store.PodUniqueKey, PodStatus) error
	CAS(key store.PodUniqueKey, status PodStatus, modifyIndex uint64) error
	MutateStatus(key store.PodUniqueKey, mutator func(PodStatus) (PodStatus, error)) error
	SetLastExit(podUniqueKey store.PodUniqueKey, launchableID launch.LaunchableID, entryPoint string, exitStatus ExitStatus) error
}
