package rcstatus

import (
	"context"

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

func (c ConsulStore) Get(rcID fields.ID) (Status, *api.QueryMeta, error) {
	if rcID == "" {
		return Status{}, nil, util.Errorf("Provided replication controller ID was empty")
	}

	rawStatus, queryMeta, err := c.statusStore.GetStatus(statusstore.RC, statusstore.ResourceID(rcID), c.namespace)
	if err != nil {
		return Status{}, queryMeta, err
	}

	status, err := rawStatusToStatus(rawStatus)
	if err != nil {
		return Status{}, queryMeta, err
	}

	return status, queryMeta, nil
}

func (c ConsulStore) Set(rcID fields.ID, status Status) error {
	if rcID == "" {
		return util.Errorf("Provided replication controller ID was empty")
	}

	rawStatus, err := statusToRawStatus(status)
	if err != nil {
		return err
	}

	return c.statusStore.SetStatus(statusstore.RC, statusstore.ResourceID(rcID), c.namespace, rawStatus)
}

func (c ConsulStore) CASTxn(ctx context.Context, rcID fields.ID, modifyIndex uint64, status Status) error {
	if rcID == "" {
		return util.Errorf("Provided replication controller ID was empty")
	}

	rawStatus, err := statusToRawStatus(status)
	if err != nil {
		return err
	}

	return c.statusStore.CASStatus(ctx, statusstore.RC, statusstore.ResourceID(rcID), c.namespace, rawStatus, modifyIndex)
}
