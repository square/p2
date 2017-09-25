package daemonsetstatus

import (
	"context"
	"encoding/json"

	dsfields "github.com/square/p2/pkg/ds/fields"
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

func (c ConsulStore) Get(dsID dsfields.ID) (Status, *api.QueryMeta, error) {
	if dsID == "" {
		return Status{}, nil, util.Errorf("provided daemon set ID was empty")
	}

	status, queryMeta, err := c.statusStore.GetStatus(statusstore.DS, statusstore.ResourceID(dsID), c.namespace)
	if err != nil {
		return Status{}, queryMeta, err
	}

	dsStatus, err := statusToDSStatus(status)
	if err != nil {
		return Status{}, queryMeta, err
	}

	return dsStatus, queryMeta, nil
}

func (c ConsulStore) CASTxn(ctx context.Context, dsID dsfields.ID, modifyIndex uint64, status Status) error {
	rawStatus, err := dsStatusToStatus(status)
	if err != nil {
		return err
	}

	return c.statusStore.CASStatus(ctx, statusstore.DS, statusstore.ResourceID(dsID), c.namespace, rawStatus, modifyIndex)
}

func (c ConsulStore) SetTxn(ctx context.Context, dsID dsfields.ID, status Status) error {
	rawStatus, err := dsStatusToStatus(status)
	if err != nil {
		return err
	}

	return c.statusStore.SetTxn(ctx, statusstore.DS, statusstore.ResourceID(dsID), c.namespace, rawStatus)
}

func statusToDSStatus(rawStatus statusstore.Status) (Status, error) {
	var dsStatus Status

	err := json.Unmarshal(rawStatus.Bytes(), &dsStatus)
	if err != nil {
		return Status{}, util.Errorf("Could not unmarshal raw status as daemon set status: %s", err)
	}

	return dsStatus, nil
}

func dsStatusToStatus(dsStatus Status) (statusstore.Status, error) {
	bytes, err := json.Marshal(dsStatus)
	if err != nil {
		return statusstore.Status{}, util.Errorf("Could not marshal pod status as json bytes: %s", err)
	}

	return statusstore.Status(bytes), nil
}
