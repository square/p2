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
