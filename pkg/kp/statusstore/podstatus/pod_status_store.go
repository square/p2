package podstatus

import (
	"github.com/square/p2/pkg/types"

	"github.com/hashicorp/consul/api"
)

// The Store interface provides easy CRUD functionality for pod status.
// Implementations of the interface will wrap the statusstore.Store interface
// which is generic since the structure of status data for each resource type
// is different
type Store interface {
	Get(types.PodUniqueKey) (PodStatus, *api.QueryMeta, error)
	Set(types.PodUniqueKey, PodStatus) error
}
