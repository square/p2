package rc

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	grpc_scheduler "github.com/square/p2/pkg/grpc/scheduler/client"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
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
)

const (
	// This label is applied to pods owned by an RC.
	RCIDLabel = "replication_controller_id"
)

type ReplicationController interface {
	ID() fields.ID

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
	newNode       types.NodeName
	oldNode       types.NodeName
	quit          chan struct{}
	cancelSession context.CancelFunc
	unlocker      consul.TxnUnlocker
}

// replicationController wraps a fields.RC with information required to manage the RC.
// Note: the fields.RC might be mutated during this struct's lifetime, so a mutex is
// used to synchronize access to it
type replicationController struct {
	fields.RC
	mu sync.Mutex

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
	nodeTransfer  nodeTransfer
}

type ReplicationControllerWatcher interface {
	Watch(rc *fields.RC, mu *sync.Mutex, quit <-chan struct{}) (<-chan struct{}, <-chan error)
}

func New(
	fields fields.RC,
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
		RC: fields,

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
		nodeTransfer:  nodeTransfer{},
	}
}

func (rc *replicationController) ID() fields.ID {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.RC.ID
}

func (rc *replicationController) WatchDesires(quit <-chan struct{}) <-chan error {
	desiresChanged, errInChannel := rc.rcWatcher.Watch(&rc.RC, &rc.mu, quit)

	errOutChannel := make(chan error)
	channelsClosed := make(chan struct{})

	// When seeing any changes, try to meet them.
	// If meeting produces any error, send it on the output error channel.
	go func() {
		for range desiresChanged {
			err := rc.meetDesires()
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

func (rc *replicationController) meetDesires() error {
	rc.logger.NoFields().Infof("Handling RC update: desired replicas %d, disabled %v", rc.ReplicasDesired, rc.Disabled)

	// If we're disabled, quit an in progress node transfer (if any) and no-op
	// (it's a normal possibility to be disabled)
	if rc.Disabled {
		rc.mu.Lock()
		if rc.nodeTransfer.quit != nil {
			close(rc.nodeTransfer.quit)
		}
		rc.mu.Unlock()
		return nil
	}

	current, err := rc.CurrentPods()
	if err != nil {
		return err
	}
	eligible, err := rc.eligibleNodes()
	if err != nil {
		return err
	}

	rc.logger.NoFields().Infof("Currently on nodes %s", current)

	nodesChanged := false
	switch {
	case rc.ReplicasDesired > len(current):
		if rc.nodeTransfer.quit != nil {
			// Remove nodeTransfer.newNode from eligible during a node transfer
			// because if it is scheduled again below in addPods(), the rc
			// will have one less node than desired. Waiting for the next loop
			// for this case again will add a node, but is a bad experience.
			for i, v := range eligible {
				if v == rc.nodeTransfer.newNode {
					eligible = append(eligible[:i], eligible[i+1:]...)
					break
				}
			}
		}
		err := rc.addPods(current, eligible)
		if err != nil {
			return err
		}
		nodesChanged = true
	case len(current) > rc.ReplicasDesired:
		rc.mu.Lock()
		if rc.nodeTransfer.quit != nil {
			// stop the node transfer; additional node is no longer needed
			close(rc.nodeTransfer.quit)
		}
		rc.mu.Unlock()
		err := rc.removePods(current, eligible)
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
		rc.logger.Infof("Ineligible nodes: %s found, starting node transfer", ineligible)
		err := rc.transferNodes(ineligible)
		if err != nil {
			return err
		}
	}

	return rc.ensureConsistency(current.Nodes(), eligible)
}

func (rc *replicationController) addPods(current types.PodLocations, eligible []types.NodeName) error {
	currentNodes := current.Nodes()

	// TODO: With Docker or runc we would not be constrained to running only once per node.
	// So it may be the case that we need to make the Scheduler interface smarter and use it here.
	possible := types.NewNodeSet(eligible...).Difference(types.NewNodeSet(currentNodes...))

	// Users want deterministic ordering of nodes being populated to a new
	// RC. Move nodes in sorted order by hostname to achieve this
	possibleSorted := possible.ListNodes()
	toSchedule := rc.ReplicasDesired - len(currentNodes)

	rc.logger.NoFields().Infof("Need to schedule %d nodes out of %s", toSchedule, possible)

	txn, cancelFunc := rc.newAuditingTransaction(context.Background(), currentNodes)
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
			txn, cancelFunc = rc.newAuditingTransaction(context.Background(), txn.Nodes())
		}
		if len(possibleSorted) < i+1 {
			errMsg := fmt.Sprintf(
				"Not enough nodes to meet desire: %d replicas desired, %d currentNodes, %d eligible. Scheduled on %d nodes instead.",
				rc.ReplicasDesired, len(currentNodes), len(eligible), i,
			)
			err := rc.alerter.Alert(rc.alertInfo(errMsg), alerting.LowUrgency)
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

		err := rc.schedule(txn, scheduleOn)
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
func (rc *replicationController) alertInfo(msg string) alerting.AlertInfo {
	hostname, _ := os.Hostname()

	rcID := rc.ID()
	rc.mu.Lock()
	manifest := rc.Manifest
	nodeSelector := rc.NodeSelector
	rc.mu.Unlock()
	return alerting.AlertInfo{
		Description: msg,
		IncidentKey: rcID.String(),
		Details: struct {
			RCID         string `json:"rc_id"`
			Hostname     string `json:"hostname"`
			PodId        string `json:"pod_id"`
			NodeSelector string `json:"node_selector"`
		}{
			RCID:         rcID.String(),
			Hostname:     hostname,
			PodId:        manifest.ID().String(),
			NodeSelector: nodeSelector.String(),
		},
	}
}

func (rc *replicationController) removePods(current types.PodLocations, eligible []types.NodeName) error {
	currentNodes := current.Nodes()

	// If we need to downsize the number of nodes, prefer any in current that are not eligible anymore.
	// TODO: evaluate changes to 'eligible' more frequently
	preferred := types.NewNodeSet(currentNodes...).Difference(types.NewNodeSet(eligible...))
	rest := types.NewNodeSet(currentNodes...).Difference(preferred)
	toUnschedule := len(current) - rc.ReplicasDesired
	rc.logger.NoFields().Infof("Need to unschedule %d nodes out of %s", toUnschedule, current)

	txn, cancelFunc := rc.newAuditingTransaction(context.Background(), currentNodes)
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
			txn, cancelFunc = rc.newAuditingTransaction(context.Background(), txn.Nodes())
		}

		unscheduleFrom, ok := preferred.PopAny()
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
					rc.ReplicasDesired, len(current),
				)
			}
		}
		err := rc.unschedule(txn, unscheduleFrom)
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

func (rc *replicationController) ensureConsistency(current []types.NodeName, eligible []types.NodeName) error {
	rc.mu.Lock()
	manifest := rc.Manifest
	rc.mu.Unlock()

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

		if err := rc.scheduleNoAudit(ctx, node); err != nil {
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

func (rc *replicationController) eligibleNodes() ([]types.NodeName, error) {
	rc.mu.Lock()
	manifest := rc.Manifest
	nodeSelector := rc.NodeSelector
	rc.mu.Unlock()

	return rc.scheduler.EligibleNodes(manifest, nodeSelector)
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
	return CurrentPods(rc.ID(), rc.podApplicator)
}

// computePodLabels() computes the set of pod labels that should be applied to
// every pod scheduled by this RC. The labels include a combination of user
// requested ones and automatic ones
func (rc *replicationController) computePodLabels() map[string]string {
	rc.mu.Lock()
	manifest := rc.Manifest
	podLabels := rc.PodLabels
	rc.mu.Unlock()
	rcID := rc.ID()
	ret := make(map[string]string)
	// user-requested labels.
	for k, v := range podLabels {
		ret[k] = v
	}

	// our reserved labels (pod id and replication controller id)
	ret[rcstore.PodIDLabel] = manifest.ID().String()
	ret[RCIDLabel] = rcID.String()

	return ret
}

func (rc *replicationController) schedule(txn *auditingTransaction, node types.NodeName) error {
	err := rc.scheduleNoAudit(txn.Context(), node)
	if err != nil {
		return err
	}

	txn.AddNode(node)
	return nil
}

func (rc *replicationController) scheduleNoAudit(ctx context.Context, node types.NodeName) error {
	rc.logger.NoFields().Infof("Scheduling on %s", node)
	rc.mu.Lock()
	manifest := rc.Manifest
	rc.mu.Unlock()
	labelKey := labels.MakePodLabelKey(node, manifest.ID())

	err := rc.podApplicator.SetLabelsTxn(ctx, labels.POD, labelKey, rc.computePodLabels())
	if err != nil {
		return err
	}

	return rc.consulStore.SetPodTxn(ctx, consul.INTENT_TREE, node, manifest)
}

func (rc *replicationController) unschedule(txn *auditingTransaction, node types.NodeName) error {
	rc.logger.NoFields().Infof("Unscheduling from %s", node)
	rc.mu.Lock()
	manifest := rc.Manifest
	rc.mu.Unlock()
	err := rc.consulStore.DeletePodTxn(txn.Context(), consul.INTENT_TREE, node, manifest.ID())
	if err != nil {
		return err
	}

	labelsToSet := rc.computePodLabels()
	var keysToRemove []string
	for k, _ := range labelsToSet {
		keysToRemove = append(keysToRemove, k)
	}

	labelKey := labels.MakePodLabelKey(node, manifest.ID())

	err = rc.podApplicator.RemoveLabelsTxn(txn.Context(), labels.POD, labelKey, keysToRemove)
	if err != nil {
		return err
	}

	txn.RemoveNode(node)
	return nil
}

func (rc *replicationController) transferNodes(ineligible []types.NodeName) error {
	if rc.nodeTransfer.quit != nil {
		// a node transfer to replace the ineligible node is already in progress
		return nil
	}

	if rc.AllocationStrategy != fields.DynamicStrategy {
		errMsg := fmt.Sprintf("static strategy RC has scheduled %d ineligible nodes: %s", len(ineligible), ineligible)
		err := rc.alerter.Alert(rc.alertInfo(errMsg), alerting.LowUrgency)
		if err != nil {
			rc.logger.WithError(err).Errorln("Unable to send alert")
		}
		// We don't return an error here because this is the current behavior for static
		// strategy RCs: only page
		return nil
	}

	status, _, err := rc.rcStatusStore.Get(rc.ID())
	if err != nil && !statusstore.IsNoStatus(err) {
		return err
	}

	var newNode, oldNode types.NodeName
	if status.NodeTransfer == nil {
		newNode, oldNode, err = rc.updateAllocations(ineligible)
		if err != nil {
			return err
		}
	} else {
		newNode = status.NodeTransfer.NewNode
		oldNode = status.NodeTransfer.OldNode
	}

	err = rc.scheduleWithSession(newNode)
	if err != nil {
		// Different RC may have already taken over the new node, abort transfer
		deleteErr := rc.rcStatusStore.Delete(rc.ID())
		if deleteErr != nil {
			rc.logger.WithError(deleteErr).Errorln("could not delete rc status")
		}
		return util.Errorf("Could not schedule with session; new rc may have taken node: %s", err)
	}

	rc.nodeTransfer.newNode = newNode
	rc.nodeTransfer.oldNode = oldNode
	rc.nodeTransfer.quit = make(chan struct{})

	go rc.doBackgroundNodeTransfer()

	return nil
}

func (rc *replicationController) updateAllocations(ineligible []types.NodeName) (types.NodeName, types.NodeName, error) {
	if len(ineligible) < 1 {
		return "", "", util.Errorf("Need at least one ineligible node to transfer from, had 0")
	}

	rc.mu.Lock()
	man := rc.Manifest
	sel := rc.NodeSelector
	rc.mu.Unlock()

	oldNode := ineligible[0]
	err := rc.scheduler.DeallocateNodes(sel, []types.NodeName{oldNode})
	if err != nil {
		return "", "", util.Errorf("Could not deallocate from %s: %s", oldNode, err)
	}

	eligible, err := rc.eligibleNodes()
	if err != nil {
		return "", "", err
	}

	current, err := rc.CurrentPods()
	if err != nil {
		return "", "", err
	}

	// Use an existing allocation if available
	possible := types.NewNodeSet(eligible...).Difference(types.NewNodeSet(current.Nodes()...)).ListNodes()

	var newNode types.NodeName
	if len(possible) > 0 {
		newNode = possible[0]
	} else {
		nodesRequested := 1 // We only support one node transfer at a time right now
		newNodes, err := rc.scheduler.AllocateNodes(man, sel, nodesRequested)
		if err != nil || len(newNodes) < 1 {
			errMsg := fmt.Sprintf("Unable to allocate nodes over grpc: %s", err)
			err := rc.alerter.Alert(rc.alertInfo(errMsg), alerting.LowUrgency)
			if err != nil {
				rc.logger.WithError(err).Errorln("Unable to send alert")
			}

			return "", "", util.Errorf(errMsg)
		}

		newNode = newNodes[0]
	}

	status := rcstatus.Status{
		NodeTransfer: &rcstatus.NodeTransfer{
			OldNode: oldNode,
			NewNode: newNode,
		},
	}

	writeCtx, writeCancel := transaction.New(context.Background())
	defer writeCancel()
	err = rc.rcStatusStore.CASTxn(writeCtx, rc.ID(), 0, status)
	if err != nil {
		return "", "", util.Errorf("Could not write new node to store: %s", err)
	}

	err = transaction.MustCommit(writeCtx, rc.txner)
	if err != nil {
		return "", "", util.Errorf("Could not commit CASTxn: %s", err)
	}

	return newNode, oldNode, nil
}

func (rc *replicationController) scheduleWithSession(newNode types.NodeName) error {
	rc.logger.NoFields().Infof("Scheduling with session on %s", newNode)
	rc.mu.Lock()
	manifest := rc.Manifest
	rc.mu.Unlock()

	manifestBytes, err := manifest.Marshal()
	if err != nil {
		return err
	}

	key, err := consul.PodPath(consul.INTENT_TREE, newNode, manifest.ID())
	if err != nil {
		return err
	}

	name := fmt.Sprintf("wait-on-%s-health-%s", newNode, rc.ID())

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer func() {
		// We close the context on error to prevent a leak but leave it
		// otherwise so it can be used to cancel the session. We write the
		// defer this way so that reassignments to err are noticed and the
		// final value is used.
		if err != nil {
			ctxCancel()
		}
	}()

	// By passing ctx below, ctxCancel (defined above) can be used to destroy
	// the session when the node transfer finishes or is stopped
	_, session, err := consul.SessionContext(ctx, rc.consulClient, name)
	if err != nil {
		return err
	}

	writeCtx, writeCancel := transaction.New(context.Background())
	defer writeCancel()

	txnUnlocker, err := session.LockIfKeyNotExistsTxn(writeCtx, key, manifestBytes)
	if err != nil {
		return err
	}

	ok, resp, err := transaction.Commit(writeCtx, rc.txner)
	switch {
	case err != nil:
		return err
	case !ok:
		// assign to err instead of just returning it so that deferred func sees
		// the assignment
		err = util.Errorf("transaction violation scheduling new node: %s", transaction.TxnErrorsToString(resp.Errors))
		return err
	}

	rc.nodeTransfer.cancelSession = ctxCancel
	rc.nodeTransfer.unlocker = txnUnlocker

	return nil
}

func (rc *replicationController) doBackgroundNodeTransfer() {
	ok := rc.watchHealth()
	if ok {
		err := rc.finishTransfer()
		if err != nil {
			rc.logger.WithError(err).Errorln("could not do final node transfer transaction")
		}
	}

	rc.nodeTransfer.cancelSession()

	rc.logger.Infof("Node transfer from %s to %s complete.", rc.nodeTransfer.oldNode, rc.nodeTransfer.newNode)

	rc.mu.Lock()
	rc.nodeTransfer = nodeTransfer{}
	rc.mu.Unlock()

	err := rc.rcStatusStore.Delete(rc.ID())
	if err != nil {
		rc.logger.WithError(err).Errorln("error deleting transfer status during cleanup")
	}
}

func (rc *replicationController) watchHealth() bool {
	rc.mu.Lock()
	podID := rc.Manifest.ID()
	rc.mu.Unlock()

	healthQuitCh := make(chan struct{})
	defer close(healthQuitCh)

	// these channels are closed by WatchPodOnNode
	resultCh, errCh := rc.healthChecker.WatchPodOnNode(rc.nodeTransfer.newNode, podID, healthQuitCh)

	for {
		select {
		case err, ok := <-errCh:
			// TODO rewrite once we understand why nil errors were sent to this chan
			if err != nil {
				rc.logger.WithError(err).Errorln("Node transfer health checker sent error")
			} else {
				rc.logger.Errorln("Node transfer health checker sent nil error. Ok was: %v", ok)
			}
		case currentHealth := <-resultCh:
			if currentHealth.Status == health.Passing {
				rc.logger.Infof("New transfer node %s health now passing", rc.nodeTransfer.newNode)
				return true
			}
		case <-rc.nodeTransfer.quit:
			return false
		case <-time.After(5 * time.Minute):
			rc.logger.Errorln("watchHealth routine timed out waiting for health result")
			return false
		}
	}
}

func (rc *replicationController) finishTransfer() error {
	current, err := rc.CurrentPods()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	txn, cancelFunc := rc.newAuditingTransaction(ctx, current.Nodes())
	defer cancelFunc()

	err = rc.nodeTransfer.unlocker.UnlockTxn(txn.Context())
	if err != nil {
		return err
	}

	txn.AddNode(rc.nodeTransfer.newNode)

	rc.mu.Lock()
	man := rc.Manifest
	rc.mu.Unlock()

	labelKey := labels.MakePodLabelKey(rc.nodeTransfer.newNode, man.ID())
	err = rc.podApplicator.SetLabelsTxn(txn.Context(), labels.POD, labelKey, rc.computePodLabels())
	if err != nil {
		return err
	}

	err = rc.unschedule(txn, rc.nodeTransfer.oldNode)
	if err != nil {
		return err
	}

	ok, resp, err := txn.CommitWithRetries(rc.txner)
	switch {
	case err != nil:
		errMsg := fmt.Sprintf(
			"transfer unlock, label, and unschedule txn returned err after timeout: %s",
			err,
		)
		alertErr := rc.alerter.Alert(rc.alertInfo(errMsg), alerting.LowUrgency)
		if alertErr != nil {
			rc.logger.WithError(alertErr).Errorln("Unable to send alert")
		}
		return err
	case !ok:
		errMsg := fmt.Sprintf(
			"Transaction violation trying to unlock node transfer session and label new node. New RC may have scheduled node: %s",
			transaction.TxnErrorsToString(resp.Errors),
		)
		err := rc.alerter.Alert(rc.alertInfo(errMsg), alerting.LowUrgency)
		if err != nil {
			rc.logger.WithError(err).Errorln("Unable to send alert")
		}
		return nil
	}

	return nil
}
