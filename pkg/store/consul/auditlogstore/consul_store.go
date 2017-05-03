package auditlogstore

import (
	"encoding/json"
	"path"
	"time"

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
	txn *api.KVTxnOps,
	eventType EventType,
	eventDetails json.RawMessage,
) error {
	auditLog := AuditLog{
		EventType:    eventType,
		EventDetails: &eventDetails,
		Timestamp:    time.Now(),
	}
	auditLogBytes, err := json.Marshal(auditLog)
	if err != nil {
		return util.Errorf("could not create audit log record: %s", err)
	}

	*txn = append(*txn, &api.KVTxnOp{
		Verb:  string(api.KVSet),
		Key:   computeKey(ID(uuid.New())),
		Value: auditLogBytes,
	})

	return nil
}
func (ConsulStore) Delete(
	txn *api.KVTxnOps,
	id ID,
) error {
	if uuid.Parse(id.String()) == nil {
		return util.Errorf("%s is not a valid audit log ID", id)
	}

	*txn = append(*txn, &api.KVTxnOp{
		Verb: api.KVDelete,
		Key:  computeKey(id),
	})
	return nil
}

func computeKey(id ID) string {
	return path.Join(auditLogTree, id.String())
}
