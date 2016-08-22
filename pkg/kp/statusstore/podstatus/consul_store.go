package podstatus

import (
	"github.com/square/p2/pkg/kp/statusstore"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

type consulStore struct {
	statusStore statusstore.Store

	// The consul implementation statusstore.Store formats keys like
	// /status/<resource-type>/<resource-id>/<namespace>. The namespace
	// portion is useful if multiple subsystems need to record their
	// own view of a resource.
	namespace statusstore.Namespace
}

func NewConsul(statusStore statusstore.Store, namespace statusstore.Namespace) Store {
	return &consulStore{
		statusStore: statusStore,
		namespace:   namespace,
	}
}

func (c *consulStore) Get(key types.PodUniqueKey) (PodStatus, error) {
	if key.ID == "" {
		return PodStatus{}, util.Errorf("Cannot retrieve status for a pod with an empty uuid")
	}

	status, err := c.statusStore.GetStatus(statusstore.POD, statusstore.ResourceID(key.ID), c.namespace)
	if err != nil {
		return PodStatus{}, err
	}

	return statusToPodStatus(status)
}

func (c *consulStore) Set(key types.PodUniqueKey, status PodStatus) error {
	if key.ID == "" {
		return util.Errorf("Could not set status for pod with empty uuid")
	}

	rawStatus, err := podStatusToStatus(status)
	if err != nil {
		return err
	}

	return c.statusStore.SetStatus(statusstore.POD, statusstore.ResourceID(key.ID), c.namespace, rawStatus)
}
