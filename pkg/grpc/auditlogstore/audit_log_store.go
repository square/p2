package auditlogstore

import (
	"time"

	"github.com/square/p2/pkg/audit"
	audit_log_protos "github.com/square/p2/pkg/grpc/auditlogstore/protos"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

func (s store) List(_ context.Context, _ *audit_log_protos.ListRequest) (*audit_log_protos.ListResponse, error) {
	records, err := s.auditLogStore.List()
	if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "error listing audit log records: %s", err)
	}

	ret := make(map[string]*audit_log_protos.AuditLog)
	for id, al := range records {
		protoRecord := rawAuditLogToProtoAuditLog(al)
		ret[id.String()] = &protoRecord
	}
	return &audit_log_protos.ListResponse{
		AuditLogs: ret,
	}, nil
}

func (s store) Delete(context.Context, *audit_log_protos.DeleteRequest) (*audit_log_protos.DeleteResponse, error) {
	return nil, util.Errorf("not yet implemented")
}

func rawAuditLogToProtoAuditLog(al audit.AuditLog) audit_log_protos.AuditLog {
	return audit_log_protos.AuditLog{
		EventType:     al.EventType.String(),
		EventDetails:  string(*al.EventDetails),
		Timestamp:     al.Timestamp.Format(time.RFC3339),
		SchemaVersion: int64(al.SchemaVersion.Int()),
	}
}
