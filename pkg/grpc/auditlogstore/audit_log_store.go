package auditlogstore

import (
	"context"
	"time"

	"github.com/square/p2/pkg/audit"
	audit_log_protos "github.com/square/p2/pkg/grpc/auditlogstore/protos"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/transaction"

	grpccontext "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type AuditLogStore interface {
	List() (map[audit.ID]audit.AuditLog, error)
	Delete(
		ctx context.Context,
		id audit.ID,
	) error
}

type store struct {
	auditLogStore AuditLogStore
	logger        logging.Logger
	txner         transaction.Txner
}

func New(auditLogStore AuditLogStore, logger logging.Logger, txner transaction.Txner) audit_log_protos.P2AuditLogStoreServer {
	return store{
		auditLogStore: auditLogStore,
		logger:        logger,
		txner:         txner,
	}
}

func (s store) List(_ grpccontext.Context, _ *audit_log_protos.ListRequest) (*audit_log_protos.ListResponse, error) {
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

func (s store) Delete(ctx grpccontext.Context, req *audit_log_protos.DeleteRequest) (*audit_log_protos.DeleteResponse, error) {
	if len(req.GetAuditLogIds()) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "no audit log IDs were specified for deletion")
	}

	// This is due to a constraint of consul transactions. They're limited to 64 operations per transaction.
	if len(req.GetAuditLogIds()) > 64 {
		return nil, grpc.Errorf(codes.InvalidArgument, "no more than 64 audit log records may be deleted at a time, but request was made for %d", len(req.GetAuditLogIds()))
	}

	ctx, cancelFunc := transaction.New(ctx)
	var err error
	for _, id := range req.GetAuditLogIds() {
		err = s.auditLogStore.Delete(ctx, audit.ID(id))
		if err != nil {
			return nil, grpc.Errorf(codes.Unavailable, "error queueing up audit log deletions in a transaction: %s", err)
		}
	}

	err = transaction.Commit(ctx, cancelFunc, s.txner)
	if err != nil {
		return nil, grpc.Errorf(codes.Unavailable, "error committing audit log deletion transaction: %s", err)
	}

	return nil, nil
}

func rawAuditLogToProtoAuditLog(al audit.AuditLog) audit_log_protos.AuditLog {
	return audit_log_protos.AuditLog{
		EventType:     al.EventType.String(),
		EventDetails:  string(*al.EventDetails),
		Timestamp:     al.Timestamp.Format(time.RFC3339),
		SchemaVersion: int64(al.SchemaVersion.Int()),
	}
}
