package auditlogstore

import (
	"encoding/json"
	"path"
	"time"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
)

const auditLogTree string = "audit_logs"

// ConsulStore is responsible for storage and retrieval of AuditLog JSON
// objects. It operates a little differently than the other consul store types
// in this project because the creation of an audit record is always expected
// to be a part of a transaction. As a result, it doesn't actually need access
// to a consul client to create records, it will simply save them to a
// transaction object.
type ConsulStore struct {
}

func (ConsulStore) Create(
	txn *transaction.Tx,
	eventType audit.EventType,
	eventDetails json.RawMessage,
) error {
	auditLog := audit.AuditLog{
		EventType:    eventType,
		EventDetails: &eventDetails,
		Timestamp:    time.Now(),
	}
	auditLogBytes, err := json.Marshal(auditLog)
	if err != nil {
		return util.Errorf("could not create audit log record: %s", err)
	}

	return txn.Add(api.KVTxnOp{
		Verb:  string(api.KVSet),
		Key:   computeKey(audit.ID(uuid.New())),
		Value: auditLogBytes,
	})
}

func computeKey(id audit.ID) string {
	return path.Join(auditLogTree, id.String())
}
