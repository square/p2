package podstatus

import (
	"github.com/square/p2/pkg/kp/statusstore"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
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

func (c *consulStore) Get(key types.PodUniqueKey) (PodStatus, *api.QueryMeta, error) {
	if key.ID == "" {
		return PodStatus{}, nil, util.Errorf("Cannot retrieve status for a pod with an empty uuid")
	}

	status, queryMeta, err := c.statusStore.GetStatus(statusstore.POD, statusstore.ResourceID(key.ID), c.namespace)
	if err != nil {
		return PodStatus{}, queryMeta, err
	}

	podStatus, err := statusToPodStatus(status)
	if err != nil {
		return PodStatus{}, queryMeta, err
	}

	return podStatus, queryMeta, nil
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
