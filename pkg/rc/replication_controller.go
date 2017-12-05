package rc

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/audit"
	grpc_scheduler "github.com/square/p2/pkg/grpc/scheduler/client"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	pcfields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
)

const (
	// This label is applied to pods owned by an RC.
	RCIDLabel = "replication_controller_id"
)

type ReplicationController interface {
	// WatchDesires causes the replication controller to watch for any changes to its desired state.
	// It is expected that a replication controller is aware of a backing rcstore against which to perform this watch.
	// Upon seeing any changes, the replication controller schedules or unschedules pods to meet the desired state.
	// This spawns a goroutine that performs the watch and returns a channel on which errors are sent.
	// The caller must consume from the error channel.
	// Failure to do so blocks the replication controller from meeting desires.
	// Send a struct{} on the quit channel to stop the goroutine.
	// The error channel will be closed in response.
	WatchDesires(quit <-chan struct{}) <-chan error

	// CurrentPods() returns all pods managed by this replication controller.
	CurrentPods() (types.PodLocations, error)
}

// A Scheduler decides what nodes are appropriate for a pod to run on.
// It potentially takes into account considerations such as existing load on the nodes,
// label selectors, and more.
type Scheduler interface {
	// EligibleNodes returns the nodes that this RC may schedule the manifest on
	EligibleNodes(manifest.Manifest, klabels.Selector) ([]types.NodeName, error)

	// AllocateNodes() can be called by the RC when it needs more nodes to
	// schedule on than EligibleNodes() returns. It will return the newly
	// allocated nodes which will also appear in subsequent EligibleNodes()
	// calls
	AllocateNodes(manifest manifest.Manifest, nodeSelector klabels.Selector, allocationCount int) ([]types.NodeName, error)

	// DeallocateNodes() indicates to the scheduler that the RC has unscheduled
	// the pod from these nodes, meaning the scheduler can free the
	// resource reservations
	DeallocateNodes(nodeSelector klabels.Selector, nodes []types.NodeName) error
}

var _ Scheduler = &scheduler.ApplicatorScheduler{}
var _ Scheduler = &grpc_scheduler.Client{}

// These methods are the same as the methods of the same name in consul.Store.
// Replication controllers have no need of any methods other than these.
type consulStore interface {
	SetPodTxn(
		ctx context.Context,
		podPrefix consul.PodPrefix,
		nodeName types.NodeName,
		manifest manifest.Manifest,
	) error

	Pod(
		podPrefix consul.PodPrefix,
		nodeName types.NodeName,
		podId types.PodID,
	) (manifest.Manifest, time.Duration, error)

	DeletePodTxn(
		ctx context.Context,
		podPrefix consul.PodPrefix,
		nodeName types.NodeName,
		manifestID types.PodID,
	) error

	NewUnmanagedSession(session, name string) consul.Session
}

// nodeTransfer encapsulates the information required to perform and cancel a node transfer
// off an ineligible node
type nodeTransfer struct {
	newNode types.NodeName
	oldNode types.NodeName
	id      rcstatus.NodeTransferID
	quit    chan struct{}

	// rollbackReason indicates why the quit channel was closed. Closers of
	// the quit channel should set this first for audit logging purposes
	rollbackReason audit.RollbackReason
}

type replicationController struct {
	rcID fields.ID

	logger logging.Logger

	consulStore   consulStore
	consulClient  consulutil.ConsulClient
	rcStatusStore rcstatus.ConsulStore
	auditLogStore AuditLogStore
	txner         transaction.Txner
	rcWatcher     ReplicationControllerWatcher
	scheduler     Scheduler
	podApplicator Labeler
	alerter       alerting.Alerter
	healthChecker checker.ConsulHealthChecker

	nodeTransfer nodeTransfer

	// nodeTransferMu protects access to nodeTransfer because it is used
	// across goroutines
	nodeTransferMu sync.Mutex
}

type ReplicationControllerWatcher interface {
	Watch(rcID fields.ID, quit <-chan struct{}) (<-chan fields.RC, <-chan error)
}

func New(
	rcID fields.ID,
	consulStore consulStore,
	consulClient consulutil.ConsulClient,
	rcStatusStore rcstatus.ConsulStore,
	auditLogStore AuditLogStore,
	txner transaction.Txner,
	rcWatcher ReplicationControllerWatcher,
	scheduler Scheduler,
	podApplicator Labeler,
	logger logging.Logger,
	alerter alerting.Alerter,
	healthChecker checker.ConsulHealthChecker,
) ReplicationController {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &replicationController{
		rcID: rcID,

		logger:        logger,
		consulStore:   consulStore,
		consulClient:  consulClient,
		rcStatusStore: rcStatusStore,
		auditLogStore: auditLogStore,
		txner:         txner,
		rcWatcher:     rcWatcher,
		scheduler:     scheduler,
		podApplicator: podApplicator,
		alerter:       alerter,
		healthChecker: healthChecker,
	}
}

func (rc *replicationController) WatchDesires(quit <-chan struct{}) <-chan error {
	rcChanges, errInChannel := rc.rcWatcher.Watch(rc.rcID, quit)

	errOutChannel := make(chan error)
	channelsClosed := make(chan struct{})

	// When seeing any changes, try to meet them.
	// If meeting produces any error, send it on the output error channel.
	go func() {
		defer func() {
			rc.nodeTransferMu.Lock()
			if rc.nodeTransfer.quit != nil {
				rc.nodeTransfer.rollbackReason = "RC farm quitting"
				close(rc.nodeTransfer.quit)
			}
			rc.nodeTransferMu.Unlock()
		}()
		// Fetch the RC's status to find out of a node transfer is underway
		// from a different incarnation of this RC

		needToFetchStatus := true
		var status rcstatus.Status
		var err error
		for needToFetchStatus {
			status, _, err = rc.rcStatusStore.Get(rc.rcID)
			switch {
			case err == nil:
				needToFetchStatus = false
			case statusstore.IsNoStatus(err):
				needToFetchStatus = false
			case err != nil:
				errOutChannel <- err
				rc.logger.WithError(err).Errorln("cannot begin servicing RC because status could not be fetched")
				time.Sleep(1 * time.Second)
			}
		}

		if status.NodeTransfer != nil {
			rc.nodeTransferMu.Lock()
			rc.nodeTransfer.oldNode = status.NodeTransfer.OldNode
			rc.nodeTransfer.newNode = status.NodeTransfer.NewNode
			rc.nodeTransfer.id = status.NodeTransfer.ID
			rc.nodeTransferMu.Unlock()
		}

		for rcFields := range rcChanges {
			err := rc.meetDesires(rcFields)
			if err != nil {
				errOutChannel <- err
			}
		}
		channelsClosed <- struct{}{}
	}()

	// When seeing any errors, forward them to the output error channel.
	// (Notice that two goroutines may be writing to the output error channel)
	go func() {
		for err := range errInChannel {
			errOutChannel <- err
		}
		channelsClosed <- struct{}{}
	}()

	// If both channels I'm watching are closed,
	// which would happen when my caller requests a quit
	// (the quit gets forwarded to the rc watcher)
	// close my output channel.
	go func() {
		<-channelsClosed
		<-channelsClosed
		close(channelsClosed)
		close(errOutChannel)
	}()

	return errOutChannel
}

func (rc *replicationController) meetDesires(rcFields fields.RC) error {
	rc.logger.NoFields().Infof("Handling RC update: desired replicas %d, disabled %v", rcFields.ReplicasDesired, rcFields.Disabled)

	current, err := rc.CurrentPods()
	if err != nil {
		return err
	}
	eligible, err := rc.eligibleNodes(rcFields)
	if err != nil {
		return err
	}

	rc.logger.NoFields().Infof("Currently on nodes %s", current)

	nodesChanged := false
	switch {
	case rcFields.ReplicasDesired > len(current):
		rc.nodeTransferMu.Lock()
		newNode := rc.nodeTransfer.newNode
		rc.nodeTransferMu.Unlock()

		if newNode != "" {
			// Remove nodeTransfer.newNode from eligible during a node transfer
			// because if it is scheduled again below in addPods(), the rc
			// will have one less node than desired. Waiting for the next loop
			// for this case again will add a node, but is a bad experience.
			for i, v := range eligible {
				if v == newNode {
					eligible = append(eligible[:i], eligible[i+1:]...)
					break
				}
			}
		}
		err := rc.addPods(rcFields, current, eligible)
		if err != nil {
			return err
		}
		nodesChanged = true
	case len(current) > rcFields.ReplicasDesired:
		err := rc.removePods(rcFields, current, eligible)
		if err != nil {
			return err
		}
		nodesChanged = true
	default:
		rc.logger.NoFields().Debugln("Taking no action")
	}

	if nodesChanged {
		current, err = rc.CurrentPods()
		if err != nil {
			return err
		}
	}

	ineligible := rc.checkForIneligible(current, eligible)
	if len(ineligible) > 0 {
		rc.logger.Infof("Ineligible nodes: %s found", ineligible)
		err := rc.transferNodes(rcFields, current, ineligible)
		if err != nil {
			return err
		}
	}

	return rc.ensureConsistency(rcFields, current.Nodes(), eligible)
}

func (rc *replicationController) addPods(rcFields fields.RC, current types.PodLocations, eligible []types.NodeName) error {
	if rcFields.Disabled {
		return nil
	}

	currentNodes := current.Nodes()

	// TODO: With Docker or runc we would not be constrained to running only once per node.
	// So it may be the case that we need to make the Scheduler interface smarter and use it here.
	possible := types.NewNodeSet(eligible...).Difference(types.NewNodeSet(currentNodes...))

	// Users want deterministic ordering of nodes being populated to a new
	// RC. Move nodes in sorted order by hostname to achieve this
	possibleSorted := possible.ListNodes()
	toSchedule := rcFields.ReplicasDesired - len(currentNodes)

	rc.logger.NoFields().Infof("Need to schedule %d nodes out of %s", toSchedule, possible)

	txn, cancelFunc := rc.newAuditingTransaction(context.Background(), rcFields, currentNodes)
	defer func() {
		// we write the defer this way so that reassignments to cancelFunc
		// are noticed and the final value is called
		cancelFunc()
	}()

	for i := 0; i < toSchedule; i++ {
		// create a new context for every 5 nodes. This is done to make
		// sure we're safely under the 64 operation limit imposed by
		// consul on transactions. This shouldn't be necessary after
		// https://github.com/hashicorp/consul/issues/2921 is resolved
		if i%5 == 0 && i > 0 {
			ok, resp, err := txn.Commit(rc.txner)
			switch {
			case err != nil:
				return err
			case !ok:
				return util.Errorf("could not schedule pods due to transaction violation: %s", transaction.TxnErrorsToString(resp.Errors))
			}

			cancelFunc()
			txn, cancelFunc = rc.newAuditingTransaction(context.Background(), rcFields, txn.Nodes())
		}
		if len(possibleSorted) < i+1 {
			errMsg := fmt.Sprintf(
				"Not enough nodes to meet desire: %d replicas desired, %d currentNodes, %d eligible. Scheduled on %d nodes instead.",
				rcFields.ReplicasDesired, len(currentNodes), len(eligible), i,
			)
			err := rc.alerter.Alert(rc.alertInfo(rcFields, errMsg), alerting.LowUrgency)
			if err != nil {
				rc.logger.WithError(err).Errorln("Unable to send alert")
			}

			// commit any queued operations
			ok, resp, txnErr := txn.Commit(rc.txner)
			switch {
			case txnErr != nil:
				return txnErr
			case !ok:
				return util.Errorf("could not schedule pods due to transaction violation: %s", transaction.TxnErrorsToString(resp.Errors))
			}

			return util.Errorf(errMsg)
		}
		scheduleOn := possibleSorted[i]

		err := rc.schedule(txn, rcFields, scheduleOn)
		if err != nil {
			return err
		}
	}

	ok, resp, err := txn.Commit(rc.txner)
	switch {
	case err != nil:
		return err
	case !ok:
		return util.Errorf("could not schedule pods due to transaction violation: %s", transaction.TxnErrorsToString(resp.Errors))
	}

	return nil
}

// Generates an alerting.AlertInfo struct. Includes information relevant to
// debugging an RC. Attempts to include the hostname the RC is running on as
// well
func (rc *replicationController) alertInfo(rcFields fields.RC, msg string) alerting.AlertInfo {
	hostname, _ := os.Hostname()

	return alerting.AlertInfo{
		Description: msg,
		IncidentKey: rcFields.ID.String(),
		Details: struct {
			RCID         string `json:"rc_id"`
			Hostname     string `json:"hostname"`
			PodId        string `json:"pod_id"`
			NodeSelector string `json:"node_selector"`
		}{
			RCID:         rcFields.ID.String(),
			Hostname:     hostname,
			PodId:        rcFields.Manifest.ID().String(),
			NodeSelector: rcFields.NodeSelector.String(),
		},
	}
}

func (rc *replicationController) removePods(rcFields fields.RC, current types.PodLocations, eligible []types.NodeName) error {
	if rcFields.Disabled {
		return nil
	}

	currentNodes := current.Nodes()

	// If we need to downsize the number of nodes, prefer any in current that are not eligible anymore.
	// TODO: evaluate changes to 'eligible' more frequently
	ineligible := types.NewNodeSet(currentNodes...).Difference(types.NewNodeSet(eligible...))
	rest := types.NewNodeSet(currentNodes...).Difference(ineligible)
	toUnschedule := len(current) - rcFields.ReplicasDesired
	rc.logger.NoFields().Infof("Need to unschedule %d nodes out of %s", toUnschedule, current)

	txn, cancelFunc := rc.newAuditingTransaction(context.Background(), rcFields, currentNodes)
	defer func() {
		cancelFunc()
	}()
	for i := 0; i < toUnschedule; i++ {
		// create a new context for every 5 nodes. This is done to make
		// sure we're safely under the 64 operation limit imposed by
		// consul on transactions. This shouldn't be necessary after
		// https://github.com/hashicorp/consul/issues/2921 is resolved
		if i%5 == 0 && i > 0 {
			ok, resp, err := txn.Commit(rc.txner)
			switch {
			case err != nil:
				return err
			case !ok:
				return util.Errorf("could not schedule pods due to transaction violation: %s", transaction.TxnErrorsToString(resp.Errors))
			}

			cancelFunc()
			txn, cancelFunc = rc.newAuditingTransaction(context.Background(), rcFields, txn.Nodes())
		}

		unscheduleFrom, ok := ineligible.PopAny()
		if !ok {
			var ok bool
			unscheduleFrom, ok = rest.PopAny()
			if !ok {
				// This should be mathematically impossible unless replicasDesired was negative
				// commit any queued operations
				ok, resp, txnErr := txn.Commit(rc.txner)
				switch {
				case txnErr != nil:
					return txnErr
				case !ok:
					return util.Errorf("could not schedule pods due to transaction violation: %s", transaction.TxnErrorsToString(resp.Errors))
				}

				return util.Errorf(
					"Unable to unschedule enough nodes to meet replicas desired: %d replicas desired, %d current.",
					rcFields.ReplicasDesired, len(current),
				)
			}
		}

		rc.nodeTransferMu.Lock()
		oldNodeInNodeTransfer := rc.nodeTransfer.oldNode
		newNodeInNodeTransfer := rc.nodeTransfer.newNode
		rc.nodeTransferMu.Unlock()

		if unscheduleFrom == oldNodeInNodeTransfer || unscheduleFrom == newNodeInNodeTransfer {
			// We don't want to unschedule the old node in a node
			// transfer until the new node is healthy, to guarantee that
			// we don't reduce cluster health as a result of the
			// transfer.  Therefore exclude this node from consideration,
			// even if it means keeping the current node count larger
			// than the replicas_desired count. Once the node transfer is
			// finished (and rc.nodeTransfer.oldNode becomes empty) we
			// will unschedule this node
			rc.logger.Infof("exempting %s from being unscheduled because it is part of a node transfer", unscheduleFrom)
			continue
		}

		err := rc.unschedule(txn, rcFields, unscheduleFrom)
		if err != nil {
			return err
		}
	}

	ok, resp, err := txn.Commit(rc.txner)
	switch {
	case err != nil:
		return err
	case !ok:
		return util.Errorf("could not schedule pods due to transaction violation: %s", transaction.TxnErrorsToString(resp.Errors))
	}

	return nil
}

func (rc *replicationController) ensureConsistency(rcFields fields.RC, current []types.NodeName, eligible []types.NodeName) error {
	manifest := rcFields.Manifest

	eligibleCurrent := types.NewNodeSet(current...).Intersection(types.NewNodeSet(eligible...)).ListNodes()

	manifestSHA, err := manifest.SHA()
	if err != nil {
		return err
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer func() {
		cancelFunc()
	}()
	for i, node := range eligibleCurrent {
		// create a new context for every 5 nodes. This is done to make
		// sure we're safely under the 64 operation limit imposed by
		// consul on transactions. This shouldn't be necessary after
		// https://github.com/hashicorp/consul/issues/2921 is resolved
		if i%5 == 0 && i > 0 {
			ok, resp, err := transaction.Commit(ctx, rc.txner)
			switch {
			case err != nil:
				return err
			case !ok:
				return util.Errorf("could not schedule pods due to transaction violation: %s", transaction.TxnErrorsToString(resp.Errors))
			}

			cancelFunc()
			ctx, cancelFunc = transaction.New(context.Background())
		}
		intent, _, err := rc.consulStore.Pod(consul.INTENT_TREE, node, manifest.ID())
		if err != nil && err != pods.NoCurrentManifest {
			return err
		}
		var intentSHA string
		if intent != nil {
			intentSHA, err = intent.SHA()
			if err != nil {
				rc.logger.WithError(err).WithField("node", node).Warn("Could not hash manifest to determine consistency of intent")
			}
			if intentSHA == manifestSHA {
				continue
			}
		}

		rc.logger.WithField("node", node).WithField("intentManifestSHA", intentSHA).Info("Found inconsistency in scheduled manifest")

		if err := rc.scheduleNoAudit(ctx, rcFields, node); err != nil {
			cancelFunc()
			return err
		}
	}

	ok, resp, err := transaction.Commit(ctx, rc.txner)
	switch {
	case err != nil:
		return err
	case !ok:
		return util.Errorf("could not schedule pods due to transaction violation: %s", transaction.TxnErrorsToString(resp.Errors))
	}

	return nil
}

func (rc *replicationController) checkForIneligible(current types.PodLocations, eligible []types.NodeName) []types.NodeName {
	// Check that the RC doesn't have any current nodes that are ineligible.
	var ineligibleCurrent []types.NodeName
	for _, currentPod := range current {
		found := false
		for _, eligibleNode := range eligible {
			if eligibleNode == currentPod.Node {
				found = true
				break
			}
		}

		if !found {
			ineligibleCurrent = append(ineligibleCurrent, currentPod.Node)
		}
	}

	return ineligibleCurrent
}

func (rc *replicationController) eligibleNodes(rcFields fields.RC) ([]types.NodeName, error) {
	return rc.scheduler.EligibleNodes(rcFields.Manifest, rcFields.NodeSelector)
}

// CurrentPods returns all pods managed by an RC with the given ID.
func CurrentPods(rcid fields.ID, labeler LabelMatcher) (types.PodLocations, error) {
	selector := klabels.Everything().Add(RCIDLabel, klabels.EqualsOperator, []string{rcid.String()})

	podMatches, err := labeler.GetMatches(selector, labels.POD)
	if err != nil {
		return nil, err
	}

	result := make(types.PodLocations, len(podMatches))
	for i, podMatch := range podMatches {
		// ID will be something like <nodename>/<podid>.
		node, podID, err := labels.NodeAndPodIDFromPodLabel(podMatch)
		if err != nil {
			return nil, err
		}
		result[i].Node = node
		result[i].PodID = podID
	}
	return result, nil
}

func (rc *replicationController) CurrentPods() (types.PodLocations, error) {
	return CurrentPods(rc.rcID, rc.podApplicator)
}

// computePodLabels() computes the set of pod labels that should be applied to
// every pod scheduled by this RC. The labels include a combination of user
// requested ones and automatic ones
func (rc *replicationController) computePodLabels(rcFields fields.RC) map[string]string {
	ret := make(map[string]string)
	// user-requested labels.
	for k, v := range rcFields.PodLabels {
		ret[k] = v
	}

	// our reserved labels (pod id and replication controller id)
	ret[rcstore.PodIDLabel] = rcFields.Manifest.ID().String()
	ret[RCIDLabel] = rcFields.ID.String()

	return ret
}

func (rc *replicationController) schedule(txn *auditingTransaction, rcFields fields.RC, node types.NodeName) error {
	err := rc.scheduleNoAudit(txn.Context(), rcFields, node)
	if err != nil {
		return err
	}

	txn.AddNode(node)
	return nil
}

func (rc *replicationController) scheduleNoAudit(ctx context.Context, rcFields fields.RC, node types.NodeName) error {
	rc.logger.NoFields().Infof("Scheduling on %s", node)
	labelKey := labels.MakePodLabelKey(node, rcFields.Manifest.ID())

	err := rc.podApplicator.SetLabelsTxn(ctx, labels.POD, labelKey, rc.computePodLabels(rcFields))
	if err != nil {
		return err
	}

	return rc.consulStore.SetPodTxn(ctx, consul.INTENT_TREE, node, rcFields.Manifest)
}

func (rc *replicationController) unschedule(txn *auditingTransaction, rcFields fields.RC, node types.NodeName) error {
	rc.logger.NoFields().Infof("Unscheduling from %s", node)
	err := rc.consulStore.DeletePodTxn(txn.Context(), consul.INTENT_TREE, node, rcFields.Manifest.ID())
	if err != nil {
		return err
	}

	labelsToSet := rc.computePodLabels(rcFields)
	var keysToRemove []string
	for k, _ := range labelsToSet {
		keysToRemove = append(keysToRemove, k)
	}

	labelKey := labels.MakePodLabelKey(node, rcFields.Manifest.ID())

	err = rc.podApplicator.RemoveLabelsTxn(txn.Context(), labels.POD, labelKey, keysToRemove)
	if err != nil {
		return err
	}

	txn.RemoveNode(node)
	return nil
}

func (rc *replicationController) transferNodes(rcFields fields.RC, current types.PodLocations, ineligible []types.NodeName) error {
	if rc.nodeTransfer.quit != nil {
		// a node transfer to replace the ineligible node is already in progress
		return nil
	}

	rc.logger.Infoln("Starting node transfer")

	if rcFields.AllocationStrategy != fields.DynamicStrategy {
		errMsg := fmt.Sprintf("static strategy RC has scheduled %d ineligible nodes: %s", len(ineligible), ineligible)
		err := rc.alerter.Alert(rc.alertInfo(rcFields, errMsg), alerting.LowUrgency)
		if err != nil {
			rc.logger.WithError(err).Errorln("Unable to send alert")
		}
		// We don't return an error here because this is the current behavior for static
		// strategy RCs: only page
		return nil
	}

	status, _, err := rc.rcStatusStore.Get(rc.rcID)
	if err != nil && !statusstore.IsNoStatus(err) {
		return err
	}

	var newNode, oldNode types.NodeName
	var nodeTransferID rcstatus.NodeTransferID
	if status.NodeTransfer == nil {
		nodeTransferID = rcstatus.NodeTransferID(uuid.New())
		newNode, oldNode, err = rc.updateAllocations(rcFields, ineligible, nodeTransferID)
		if err != nil {
			return err
		}
	} else {
		newNode = status.NodeTransfer.NewNode
		oldNode = status.NodeTransfer.OldNode
		nodeTransferID = status.NodeTransfer.ID
	}

	rc.nodeTransferMu.Lock()
	rc.nodeTransfer.newNode = newNode
	rc.nodeTransfer.oldNode = oldNode
	rc.nodeTransfer.id = nodeTransferID
	rc.nodeTransferMu.Unlock()

	err = rc.scheduleNewNodeForTransfer(rcFields, newNode, current)
	if err != nil {
		return util.Errorf("Could not schedule with session: %s", err)
	}

	rc.nodeTransferMu.Lock()
	rc.nodeTransfer.quit = make(chan struct{})
	rc.nodeTransferMu.Unlock()

	go rc.doBackgroundNodeTransfer(rcFields)

	return nil
}

func (rc *replicationController) updateAllocations(rcFields fields.RC, ineligible []types.NodeName, nodeTransferID rcstatus.NodeTransferID) (types.NodeName, types.NodeName, error) {
	if len(ineligible) < 1 {
		return "", "", util.Errorf("Need at least one ineligible node to transfer from, had 0")
	}

	nodesRequested := 1 // We only support one node transfer at a time right now
	newNodes, err := rc.scheduler.AllocateNodes(rcFields.Manifest, rcFields.NodeSelector, nodesRequested)
	if err != nil || len(newNodes) < 1 {
		errMsg := fmt.Sprintf("Unable to allocate nodes over grpc: %s", err)
		err := rc.alerter.Alert(rc.alertInfo(rcFields, errMsg), alerting.LowUrgency)
		if err != nil {
			rc.logger.WithError(err).Errorln("Unable to send alert")
		}

		return "", "", util.Errorf(errMsg)
	}

	newNode := newNodes[0]
	rc.logger.Infof("Allocated node %s for transfer", newNode)

	oldNode := ineligible[0]
	err = rc.scheduler.DeallocateNodes(rcFields.NodeSelector, []types.NodeName{oldNode})
	if err != nil {
		return "", "", util.Errorf("Could not deallocate from %s: %s", oldNode, err)
	}
	rc.logger.Infof("Deallocated ineligible node %s", oldNode)

	status := rcstatus.Status{
		NodeTransfer: &rcstatus.NodeTransfer{
			OldNode: oldNode,
			NewNode: newNode,
			ID:      nodeTransferID,
		},
	}

	auditLogDetails, err := audit.NewNodeTransferStartDetails(
		nodeTransferID,
		rc.rcID,
		rcFields.Manifest.ID(),
		pcfields.AvailabilityZone(rcFields.PodLabels[pcfields.AvailabilityZoneLabel]),
		pcfields.ClusterName(rcFields.PodLabels[pcfields.ClusterNameLabel]),
		rcFields.NodeSelector,
		oldNode,
		newNode,
		rcFields.ReplicasDesired,
	)
	if err != nil {
		return "", "", util.Errorf("could not generate node transfer start audit log details: %s", err)
	}

	writeCtx, writeCancel := transaction.New(context.Background())
	defer writeCancel()

	err = rc.auditLogStore.Create(
		writeCtx,
		audit.NodeTransferStartEvent,
		auditLogDetails,
	)
	if err != nil {
		return "", "", util.Errorf("could not add audit log record to node transfer start transaction: %s", err)
	}

	err = rc.rcStatusStore.CASTxn(writeCtx, rc.rcID, 0, status)
	if err != nil {
		return "", "", util.Errorf("Could not write new node to store: %s", err)
	}

	err = transaction.MustCommit(writeCtx, rc.txner)
	if err != nil {
		return "", "", util.Errorf("could not commit transaction to update RC status with node transfer information: %s", err)
	}

	return newNode, oldNode, nil
}

func (rc *replicationController) scheduleNewNodeForTransfer(rcFields fields.RC, newNode types.NodeName, current types.PodLocations) error {
	rc.logger.NoFields().Infof("Scheduling %s as part of node transfer", newNode)

	key, err := consul.PodPath(consul.INTENT_TREE, newNode, rcFields.Manifest.ID())
	if err != nil {
		return err
	}

	auditingTransaction, cancel := rc.newAuditingTransaction(context.Background(), rcFields, current.Nodes())
	defer cancel()

	// delete-cas with an index of 0 is equivalent to "check-not-exists" which
	// is introduced in a later consul version than we're using
	err = transaction.Add(auditingTransaction.Context(), api.KVTxnOp{
		Verb:  string(api.KVDeleteCAS),
		Key:   key,
		Index: 0,
	})

	err = rc.schedule(auditingTransaction, rcFields, newNode)
	if err != nil {
		return err
	}

	ok, _, err := auditingTransaction.Commit(rc.txner)
	switch {
	case err != nil:
		return err
	case !ok:
		// This means that our DeleteCAS failed, which means another RC
		// scheduled the new node already, or a past incarnation of
		// this RC already scheduled it. Either of these situations are
		// fine, proceed onwards
		rc.logger.Infof("New node %s of node transfer was already scheduled, proceeding without scheduling", newNode)
	}

	return nil
}

func (rc *replicationController) doBackgroundNodeTransfer(rcFields fields.RC) {
	ok, rollbackReason := rc.watchHealth(rcFields)
	if ok {
		err := rc.finishTransfer(rcFields)
		if err == nil {
			rc.logger.Infof("Node transfer from %s to %s complete.", rc.nodeTransfer.oldNode, rc.nodeTransfer.newNode)
			return
		}

		rc.logger.WithError(err).Errorln("could not do final node transfer transaction, rolling back")
	} else {
		rc.logger.Infof("Node transfer from %s to %s was canceled: %s.", rc.nodeTransfer.oldNode, rc.nodeTransfer.newNode, rollbackReason)
	}
}

func (rc *replicationController) watchHealth(rcFields fields.RC) (bool, audit.RollbackReason) {
	rc.logger.Infof("Watching health on %s", rc.nodeTransfer.newNode)

	healthQuitCh := make(chan struct{})
	defer close(healthQuitCh)

	// these channels are closed by WatchPodOnNode
	resultCh, errCh := rc.healthChecker.WatchPodOnNode(rc.nodeTransfer.newNode, rcFields.Manifest.ID(), healthQuitCh)

	for {
		select {
		case err := <-errCh:
			if err != nil {
				rc.logger.WithError(err).Errorln("Node transfer health checker sent error")
			} else {
				rc.logger.Errorln("Node transfer health checker sent nil error")
			}
		case currentHealth := <-resultCh:
			if currentHealth.Status == health.Passing {
				rc.logger.Infof("New transfer node %s health now passing", rc.nodeTransfer.newNode)
				return true, ""
			}
		case <-rc.nodeTransfer.quit:
			return false, rc.nodeTransfer.rollbackReason
		case <-time.After(5 * time.Minute):
			err := "watchHealth routine timed out waiting for health result"
			rc.logger.Errorln(err)
			return false, audit.RollbackReason(err)
		}
	}
}

func (rc *replicationController) finishTransfer(rcFields fields.RC) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// technically there's a race here with the roll loop changing the node
	// membership which could make the audit log records incorrect, but
	// that's a small price to pay
	current, err := rc.CurrentPods()
	if err != nil {
		return err
	}

	txn, cancelFunc := rc.newAuditingTransaction(ctx, rcFields, current.Nodes())
	defer cancelFunc()

	rc.nodeTransferMu.Lock()
	oldNode := rc.nodeTransfer.oldNode
	newNode := rc.nodeTransfer.newNode
	nodeTransferID := rc.nodeTransfer.id
	rc.nodeTransferMu.Unlock()

	err = rc.unschedule(txn, rcFields, oldNode)
	if err != nil {
		return err
	}

	auditLogDetails, err := audit.NewNodeTransferCompletionDetails(
		nodeTransferID,
		rc.rcID,
		rcFields.Manifest.ID(),
		pcfields.AvailabilityZone(rcFields.PodLabels[pcfields.AvailabilityZoneLabel]),
		pcfields.ClusterName(rcFields.PodLabels[pcfields.ClusterNameLabel]),
		rcFields.NodeSelector,
		oldNode,
		newNode,
		rcFields.ReplicasDesired,
	)
	if err != nil {
		return util.Errorf("could not generate node transfer completion audit log details: %s", err)
	}

	err = rc.auditLogStore.Create(
		txn.Context(),
		audit.NodeTransferCompletionEvent,
		auditLogDetails,
	)
	if err != nil {
		return util.Errorf("could not add audit log record to node transfer completion transaction: %s", err)
	}

	err = rc.rcStatusStore.DeleteTxn(txn.Context(), rc.rcID)
	if err != nil {
		return util.Errorf("could not add RC status deletion to node transfer completion transaction: %s", err)
	}

	ok, resp, err := txn.CommitWithRetries(rc.txner)
	switch {
	case err != nil:
		err := util.Errorf(
			"RC status deletion and audit log record returned err after timeout: %s",
			err,
		)
		rc.logger.WithError(err).Errorln("could not finalize node transfer")
		alertErr := rc.alerter.Alert(rc.alertInfo(rcFields, err.Error()), alerting.LowUrgency)
		if alertErr != nil {
			rc.logger.WithError(alertErr).Errorln("Unable to send alert")
		}
		return err
	case !ok:
		// This doesn't make sense because there are no "check" conditions in the transaction
		err := util.Errorf(
			"Transaction violation trying to delete RC status",
			transaction.TxnErrorsToString(resp.Errors),
		)
		rc.logger.WithError(err).Errorln("could not finalize node transfer")
		err = rc.alerter.Alert(rc.alertInfo(rcFields, err.Error()), alerting.LowUrgency)
		if err != nil {
			rc.logger.WithError(err).Errorln("Unable to send alert")
		}
		return nil
	}

	rc.nodeTransferMu.Lock()
	rc.nodeTransfer = nodeTransfer{}
	rc.nodeTransferMu.Unlock()
	return nil
}

func (rc *replicationController) createRollbackTransferRecord(
	ctx context.Context,
	rcFields fields.RC,
	rollbackReason audit.RollbackReason,
) error {
	rc.nodeTransferMu.Lock()
	nodeTransferID := rc.nodeTransfer.id
	oldNode := rc.nodeTransfer.oldNode
	newNode := rc.nodeTransfer.newNode
	rc.nodeTransferMu.Unlock()

	auditLogDetails, err := audit.NewNodeTransferRollbackDetails(
		nodeTransferID,
		rc.rcID,
		rcFields.Manifest.ID(),
		pcfields.AvailabilityZone(rcFields.PodLabels[pcfields.AvailabilityZoneLabel]),
		pcfields.ClusterName(rcFields.PodLabels[pcfields.ClusterNameLabel]),
		rcFields.NodeSelector,
		oldNode,
		newNode,
		rcFields.ReplicasDesired,
		rollbackReason,
	)
	if err != nil {
		return util.Errorf("could not generate audit log details for node transfer rollback: %s", err)
	}

	err = rc.auditLogStore.Create(
		ctx,
		audit.NodeTransferRollbackEvent,
		auditLogDetails,
	)
	if err != nil {
		return util.Errorf("could not add audit log record to node transfer rollback transaction: %s", err)
	}

	rc.nodeTransferMu.Lock()
	rc.nodeTransfer = nodeTransfer{}
	rc.nodeTransferMu.Unlock()

	return nil
}
