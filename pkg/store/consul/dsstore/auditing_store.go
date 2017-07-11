package dsstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"
)

type AuditLogStore interface {
	Create(ctx context.Context, eventType audit.EventType, eventDetails json.RawMessage) error
}

func NewAuditingStore(innerStore *ConsulStore, auditLogStore AuditLogStore) AuditingStore {
	return AuditingStore{
		innerStore:    innerStore,
		auditLogStore: auditLogStore,
	}
}

// AuditingStore is a wrapper around a ConsulStore that will produce audit logs
// for the actions taken.
type AuditingStore struct {
	innerStore    *ConsulStore
	auditLogStore AuditLogStore
}

func (a AuditingStore) Create(
	ctx context.Context,
	manifest manifest.Manifest,
	minHealth int,
	name fields.ClusterName,
	nodeSelector klabels.Selector,
	podID types.PodID,
	timeout time.Duration,
	user string,
) (fields.DaemonSet, error) {
	ds, err := a.innerStore.Create(ctx, manifest, minHealth, name, nodeSelector, podID, timeout)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	details, err := audit.NewDaemonSetDetails(ds, user)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	err = a.auditLogStore.Create(ctx, audit.DSCreatedEvent, details)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("could not create audit log record for daemon set creation: %s", err)
	}

	return ds, nil
}

func (a AuditingStore) Disable(
	ctx context.Context,
	id fields.ID,
	user string,
) (fields.DaemonSet, error) {
	ds, err := a.innerStore.DisableTxn(ctx, id)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	details, err := audit.NewDaemonSetDetails(ds, user)
	if err != nil {
		return fields.DaemonSet{}, err
	}
	err = a.auditLogStore.Create(ctx, audit.DSDisabledEvent, details)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("could not create audit log record for daemon set disable: %s", err)
	}

	return ds, nil
}

func (a AuditingStore) Enable(
	ctx context.Context,
	id fields.ID,
	user string,
) (fields.DaemonSet, error) {
	ds, err := a.innerStore.EnableTxn(ctx, id)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	details, err := audit.NewDaemonSetDetails(ds, user)
	if err != nil {
		return fields.DaemonSet{}, err
	}
	err = a.auditLogStore.Create(ctx, audit.DSEnabledEvent, details)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("could not create audit log record for daemon set enable: %s", err)
	}

	return ds, nil
}

func (a AuditingStore) UpdateManifest(
	ctx context.Context,
	id fields.ID,
	manifest manifest.Manifest,
	user string,
) (fields.DaemonSet, error) {
	mutator := func(ds fields.DaemonSet) (fields.DaemonSet, error) {
		ds.Manifest = manifest
		return ds, nil
	}

	ds, err := a.innerStore.MutateDSTxn(ctx, id, mutator)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	details, err := audit.NewDaemonSetDetails(ds, user)
	if err != nil {
		return fields.DaemonSet{}, err
	}
	err = a.auditLogStore.Create(ctx, audit.DSManifestUpdatedEvent, details)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("could not create audit log record for daemon set manifest update: %s", err)
	}

	return ds, nil
}

func (a AuditingStore) UpdateNodeSelector(
	ctx context.Context,
	id fields.ID,
	nodeSelector klabels.Selector,
	user string,
) (fields.DaemonSet, error) {
	mutator := func(ds fields.DaemonSet) (fields.DaemonSet, error) {
		ds.NodeSelector = nodeSelector
		return ds, nil
	}

	ds, err := a.innerStore.MutateDSTxn(ctx, id, mutator)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	details, err := audit.NewDaemonSetDetails(ds, user)
	if err != nil {
		return fields.DaemonSet{}, err
	}
	err = a.auditLogStore.Create(ctx, audit.DSNodeSelectorUpdatedEvent, details)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("could not create audit log record for daemon set node selector update: %s", err)
	}

	return ds, nil
}

func (a AuditingStore) Delete(
	ctx context.Context,
	id fields.ID,
	user string,
) error {
	// fetch the daemon set first for auditing purposes
	ds, _, err := a.innerStore.Get(id)
	if err != nil {
		// this is breaking a bit from the way the inner store does things; it
		// allows you to delete a daemon set that doesn't exist without error.
		// Here we make a different decision because the user might expect an audit
		// log record to be created if there was no error, so instead we forbid
		// deleting a daemon set that does not exist
		return util.Errorf("couldn't delete daemon set: could not fetch daemon set for auditing purposes: %s", err)
	}

	err = a.innerStore.DeleteTxn(ctx, id)
	if err != nil {
		return err
	}

	details, err := audit.NewDaemonSetDetails(ds, user)
	if err != nil {
		return err
	}
	err = a.auditLogStore.Create(ctx, audit.DSDeletedEvent, details)
	if err != nil {
		return util.Errorf("could not create audit log record for daemon set deletion: %s", err)
	}

	return nil
}

func (a AuditingStore) UpdateMinHealth(
	ctx context.Context,
	id fields.ID,
	minHealth int,
	user string,
) (fields.DaemonSet, error) {
	mutator := func(ds fields.DaemonSet) (fields.DaemonSet, error) {
		ds.MinHealth = minHealth
		return ds, nil
	}

	ds, err := a.innerStore.MutateDSTxn(ctx, id, mutator)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	details, err := audit.NewDaemonSetDetails(ds, user)
	if err != nil {
		return fields.DaemonSet{}, err
	}
	err = a.auditLogStore.Create(ctx, audit.DSModifiedEvent, details)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("could not create audit log record for daemon set min health update: %s", err)
	}

	return ds, nil
}
