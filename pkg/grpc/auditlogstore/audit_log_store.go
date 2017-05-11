package auditlogstore

import (
	audit_log_protos "github.com/square/p2/pkg/grpc/auditlogstore/protos"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"

	"golang.org/x/net/context"
)

type AuditLogStore interface {
	List() (map[audit.ID]audit.AuditLog, error)
	Delete(
		txn *transaction.Tx,
		id audit.ID,
	) error
}

type store struct {
	auditLogStore AuditLogStore
	logger        logging.Logger
}

func New(auditLogStore AuditLogStore, logger logging.Logger) audit_log_protos.P2AuditLogStoreServer {
	return store{
		auditLogStore: auditLogStore,
		logger:        logger,
	}
}

func (s store) List(context.Context, *audit_log_protos.ListRequest) (*audit_log_protos.ListResponse, error) {
	return nil, util.Errorf("not yet implemented")
}

func (s store) Delete(context.Context, *audit_log_protos.DeleteRequest) (*audit_log_protos.DeleteResponse, error) {
	return nil, util.Errorf("not yet implemented")
}
