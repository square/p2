package client

import (
	"context"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/grpc/auditlogstore"
	auditlogstore_protos "github.com/square/p2/pkg/grpc/auditlogstore/protos"

	"google.golang.org/grpc"
)

type Client struct {
	client auditlogstore_protos.P2AuditLogStoreClient
}

func New(conn *grpc.ClientConn) Client {
	return Client{
		client: auditlogstore_protos.NewP2AuditLogStoreClient(conn),
	}
}

func (c Client) List(ctx context.Context) (map[audit.ID]audit.AuditLog, error) {
	resp, err := c.client.List(ctx, new(auditlogstore_protos.ListRequest), grpc.MaxCallRecvMsgSize(1024*1024*1024*10))
	if err != nil {
		return nil, err
	}

	ret := make(map[audit.ID]audit.AuditLog)
	for id, al := range resp.GetAuditLogs() {
		ret[audit.ID(id)], err = auditlogstore.ProtoAuditLogToRawAuditLog(*al)
		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func (c Client) Delete(ctx context.Context, idsToDelete []audit.ID) error {
	req := &auditlogstore_protos.DeleteRequest{}
	for _, id := range idsToDelete {
		req.AuditLogIds = append(req.AuditLogIds, id.String())
	}

	_, err := c.client.Delete(ctx, req)
	if err != nil {
		return err
	}

	return nil
}
