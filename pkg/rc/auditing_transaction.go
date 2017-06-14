package rc

import (
	"context"

	"github.com/square/p2/pkg/audit"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

type auditingTransaction struct {
	ctx           context.Context
	nodes         map[types.NodeName]struct{}
	auditLogStore AuditLogStore
	podID         types.PodID
	az            pc_fields.AvailabilityZone
	cn            pc_fields.ClusterName
}

type scheduledNodesKey struct{}

func (rc *replicationController) newAuditingTransaction(
	ctx context.Context,
	startingNodes []types.NodeName,
) (*auditingTransaction, func()) {
	annotatedContext := context.WithValue(ctx, scheduledNodesKey{}, startingNodes)
	ctx, cancelFunc := transaction.New(annotatedContext)

	startingNodeMap := make(map[types.NodeName]struct{})
	for _, node := range startingNodes {
		startingNodeMap[node] = struct{}{}
	}

	rc.mu.Lock()
	manifest := rc.Manifest
	podLabels := rc.PodLabels
	rc.mu.Unlock()
	return &auditingTransaction{
		ctx:           ctx,
		nodes:         startingNodeMap,
		auditLogStore: rc.auditLogStore,
		podID:         manifest.ID(),
		az:            pc_fields.AvailabilityZone(podLabels[types.AvailabilityZoneLabel]),
		cn:            pc_fields.ClusterName(podLabels[types.ClusterNameLabel]),
	}, cancelFunc
}

func (a *auditingTransaction) Context() context.Context {
	return a.ctx
}

func (a *auditingTransaction) Nodes() []types.NodeName {
	var nodes []types.NodeName
	for node, _ := range a.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (a *auditingTransaction) AddNode(node types.NodeName) {
	a.nodes[node] = struct{}{}
}

func (a *auditingTransaction) RemoveNode(node types.NodeName) {
	delete(a.nodes, node)
}

// Commit adds one final operation to the underlying consul transaction to
// create an audit log record with the set of nodes that already have been
// scheduled and nodes that will be scheduled as a part of this transaction by
// the RC. Then it commits the transaction
func (a *auditingTransaction) Commit(txner transaction.Txner) error {
	details, err := audit.NewRCRetargetingEventDetails(
		a.podID,
		a.az,
		a.cn,
		a.Nodes(),
	)
	if err != nil {
		return err
	}

	err = a.auditLogStore.Create(
		a.ctx,
		audit.RCRetargetingEvent,
		details,
	)
	if err != nil {
		return util.Errorf("could not add rc retargeting audit log to context: %s", err)
	}

	return transaction.Commit(a.ctx, txner)
}
