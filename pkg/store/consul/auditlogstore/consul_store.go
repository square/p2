package auditlogstore

import (
	"encoding/json"
	"path"
	"strings"
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

func (c ConsulStore) List() (map[ID]AuditLog, error) {
	pairs, _, err := c.consulKV.List(auditLogTree+"/", nil)
	if err != nil {
		return nil, util.Errorf("could not list audit log records: %s", err)
	}

	ret := make(map[ID]AuditLog)
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

func auditLogFromPair(pair *api.KVPair) (AuditLog, error) {
	var al AuditLog
	err := json.Unmarshal(pair.Value, &al)
	if err != nil {
		return AuditLog{}, err
	}

	return al, nil
}

func computeKey(id ID) string {
	return path.Join(auditLogTree, id.String())
}

func idFromKey(key string) (ID, error) {
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

	return ID(parts[1]), nil
}
