package rcstatus

import (
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

type ConsulStore struct {
	statusStore statusstore.Store

	// The consul implementation statusstore.Store formats keys like
	// /status/<resource-type>/<resource-id>/<namespace>. The namespace
	// portion is useful if multiple subsystems need to record their
	// own view of a resource.
	namespace statusstore.Namespace
}

func NewConsul(statusStore statusstore.Store, namespace statusstore.Namespace) ConsulStore {
	return ConsulStore{
		statusStore: statusStore,
		namespace:   namespace,
	}
}

func (c ConsulStore) Get(rcID fields.ID) (RCStatus, *api.QueryMeta, error) {
	if rcID == "" {
		return RCStatus{}, nil, util.Errorf("Provided replication controller ID was empty")
	}

	status, queryMeta, err := c.statusStore.GetStatus(statusstore.RC, statusstore.ResourceID(rcID), c.namespace)
	if err != nil {
		return RCStatus{}, queryMeta, err
	}

	rcStatus, err := statusToRCStatus(status)
	if err != nil {
		return RCStatus{}, queryMeta, err
	}

	return rcStatus, queryMeta, nil
}

func (c ConsulStore) Set(rcID fields.ID, status RCStatus) error {
	if rcID == "" {
		return util.Errorf("Provided replication controller ID was empty")
	}

	rawStatus, err := rcStatusToStatus(status)
	if err != nil {
		return err
	}

	return c.statusStore.SetStatus(statusstore.RC, statusstore.ResourceID(rcID), c.namespace, rawStatus)
}

func (c ConsulStore) Delete(rcID fields.ID) error {
	if rcID == "" {
		return util.Errorf("Provided replication controller ID was empty")
	}

	return c.statusStore.DeleteStatus(statusstore.RC, statusstore.ResourceID(rcID.String()), c.namespace)
}
