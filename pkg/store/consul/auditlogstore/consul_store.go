package auditlogstore

import (
	"encoding/json"
	"path"
	"strings"
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
	consulKV ConsulKV
}

type ConsulKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

func NewConsulStore(consulKV ConsulKV) ConsulStore {
	return ConsulStore{
		consulKV: consulKV,
	}
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
func (ConsulStore) Delete(
	txn *transaction.Tx,
	id audit.ID,
) error {
	if uuid.Parse(id.String()) == nil {
		return util.Errorf("%s is not a valid audit log ID", id)
	}

	return txn.Add(api.KVTxnOp{
		Verb: api.KVDelete,
		Key:  computeKey(id),
	})
}

func (c ConsulStore) List() (map[audit.ID]audit.AuditLog, error) {
	pairs, _, err := c.consulKV.List(auditLogTree+"/", nil)
	if err != nil {
		return nil, util.Errorf("could not list audit log records: %s", err)
	}

	ret := make(map[audit.ID]audit.AuditLog)
	for _, pair := range pairs {
		id, err := idFromKey(pair.Key)
		if err != nil {
			return nil, err
		}

		al, err := auditLogFromPair(pair)
		if err != nil {
			return nil, util.Errorf("could not convert value of audit log %s to audit log struct: %s", pair.Key, err)
		}
		ret[id] = al
	}

	return ret, nil
}

func auditLogFromPair(pair *api.KVPair) (audit.AuditLog, error) {
	var al audit.AuditLog
	err := json.Unmarshal(pair.Value, &al)
	if err != nil {
		return audit.AuditLog{}, err
	}

	return al, nil
}

func computeKey(id audit.ID) string {
	return path.Join(auditLogTree, id.String())
}

func idFromKey(key string) (audit.ID, error) {
	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		return "", util.Errorf("%s did not match expected key format", key)
	}

	if parts[0] != auditLogTree {
		return "", util.Errorf("%s did not match expected key format", key)
	}

	if uuid.Parse(parts[1]) == nil {
		return "", util.Errorf("%s from audit log key %s did not parse as a UUID", parts[1], key)
	}

	return audit.ID(parts[1]), nil
}
