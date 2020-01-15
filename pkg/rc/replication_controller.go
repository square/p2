package rc

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/artifact"
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
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/sirupsen/logrus"
)

const (
	// This label is applied to pods owned by an RC.
	RCIDLabel = "replication_controller_id"

	// The maximum number of times to attempt a node allocate or deallocate
	MaxAllocateAttempts = 10
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

type RCMutationLocker interface {
	LockForMutation(fields.ID, consul.Session) (consul.Unlocker, error)
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
	AllocateNodes(manifest manifest.Manifest, nodeSelector klabels.Selector, allocationCount int, force bool) ([]types.NodeName, error)

	// DeallocateNodes() indicates to the scheduler that the RC has unscheduled
	// the pod from these nodes, meaning the scheduler can free the
	// resource reservations
	DeallocateNodes(nodeSelector klabels.Selector, nodes []types.NodeName) error
}

var _ Scheduler = &scheduler.ApplicatorScheduler{}
var _ Scheduler = &grpc_scheduler.Client{}

type ServiceDiscoveryChecker interface {
	// IsSyncedWithCluster can be called by the RC when it needs to know that
	// the service discovery system is up to date with P2's latest cluster
	// state
	IsSyncedWithCluster(rcID fields.ID) (bool, error)
}

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

type replicationController struct {
	rcID fields.ID

	logger logging.Logger

	consulStore      consulStore
	consulClient     consulutil.ConsulClient
	rcStatusStore    rcstatus.ConsulStore
	rcLocker         RCMutationLocker
	auditLogStore    AuditLogStore
	txner            transaction.Txner
	rcWatcher        ReplicationControllerWatcher
	scheduler        Scheduler
	podApplicator    Labeler
	alerter          alerting.Alerter
	healthChecker    checker.HealthChecker
	artifactRegistry artifact.Registry
	sdChecker        ServiceDiscoveryChecker
}

type ReplicationControllerWatcher interface {
	Watch(rcID fields.ID, quit <-chan struct{}) (<-chan fields.RC, <-chan error)
}

func New(
	rcID fields.ID,
	consulStore consulStore,
	consulClient consulutil.ConsulClient,
	rcLocker RCMutationLocker,
	rcStatusStore rcstatus.ConsulStore,
	auditLogStore AuditLogStore,
	txner transaction.Txner,
	rcWatcher ReplicationControllerWatcher,
	scheduler Scheduler,
	podApplicator Labeler,
	logger logging.Logger,
	alerter alerting.Alerter,
	healthChecker checker.HealthChecker,
	artifactRegistry artifact.Registry,
	sdChecker ServiceDiscoveryChecker,
) ReplicationController {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &replicationController{
		rcID: rcID,

		logger:           logger,
		consulStore:      consulStore,
		consulClient:     consulClient,
		rcLocker:         rcLocker,
		rcStatusStore:    rcStatusStore,
		auditLogStore:    auditLogStore,
		txner:            txner,
		rcWatcher:        rcWatcher,
		scheduler:        scheduler,
		podApplicator:    podApplicator,
		alerter:          alerter,
		healthChecker:    healthChecker,
		artifactRegistry: artifactRegistry,
		sdChecker:        sdChecker,
	}
}

func (rc *replicationController) WatchDesires(quit <-chan struct{}) <-chan error {
	rcChanges, errInChannel := rc.rcWatcher.Watch(rc.rcID, quit)

	errOutChannel := make(chan error)
	channelsClosed := make(chan struct{})

	// get random wait time to check for missing artifacts bewteen 1 - 60 minutes
	randomJitter := rand.Intn(60) + 1
	shouldCheckTimer := time.NewTimer(time.Duration(24*time.Hour) + (time.Duration(randomJitter) * time.Minute))
	shouldCheck := shouldCheckTimer.C

	// When seeing any changes, try to meet them.
	// If meeting produces any error, send it on the output error channel.
	go func() {
		for rcFields := range rcChanges {
			select {
			case <-shouldCheck:
				rc.checkMissingArtifacts(rcFields)
				shouldCheckTimer.Reset(time.Hour)
			default:
			}
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
		eligible, err = rc.eligibleNodes(rcFields)
		if err != nil {
			return err
		}
	}

	ineligible := rc.checkForIneligible(current, eligible)
	if len(ineligible) > 0 && rcFields.AllocationStrategy == fields.DynamicStrategy {
		rc.logger.Infof("ineligible nodes: %s found, %v rcID", ineligible, rc.rcID)
		ok, err := rc.attemptNodeTransfer(rcFields, current, ineligible, MaxAllocateAttempts)
		if err != nil {
			return err
		} else if !ok {
			rc.logger.Infoln("node transfer conditions not met; skipping")
		}
	}

	return rc.ensureConsistency(rcFields)
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

		// since significant time may have passed since these values were instantiated,
		// get updated values each iteration, and leverage those
		eligible, err := rc.eligibleNodes(rcFields)
		if err != nil {
			return err
		}
		possible = types.NewNodeSet(eligible...).Difference(types.NewNodeSet(currentNodes...))
		possibleSorted = possible.ListNodes()
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

		err = rc.schedule(txn, rcFields, scheduleOn)
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

func (rc *replicationController) ensureConsistency(rcFields fields.RC) error {
	if rcFields.Disabled {
		return nil
	}

	currentPods, err := rc.CurrentPods()
	if err != nil {
		return err
	}
	current := currentPods.Nodes()
	eligible, err := rc.eligibleNodes(rcFields)
	if err != nil {
		return err
	}

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

// attemptNodeTransfer will transactionally remove a pod on an ineligible node
// and add a pod on a new node if the following conditions are met:
//   1) The ineligible pod is unhealthy OR all of the RC's pods are healthy
//   2) The service discovery system is synced with the pod cluster
//   3) The RC can acquire a mutation lock on its ID
//   4) OR the RC is disabled, only on ineligible nodes, and has fewer desired
//      replicas than current (described more in canNodeTransfer())
// If the conditions are not met, the function will be called again on the next
// call of meetDesires() should a node still be ineligible. It returns true
// when a node transfer occurs
func (rc *replicationController) attemptNodeTransfer(rcFields fields.RC, current types.PodLocations, ineligibles []types.NodeName, allocAttempts int) (bool, error) {
	ok, unlocker, err := rc.canNodeTransfer(rcFields, current, ineligibles)
	if unlocker != nil {
		defer func() {
			err = unlocker.Unlock()
			if err != nil {
				rc.logger.WithError(err).Errorf("failed to unlock session: %s", rc.rcID)
			}
			err = unlocker.DestroySession()
			if err != nil {
				rc.logger.WithError(err).Errorf("failed to destroy session: %s", rc.rcID)
			}
		}()
	}
	if err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}

	ineligible := ineligibles[0]
	err = rc.swapNodes(rcFields, current, ineligible, allocAttempts)
	if err != nil {
		rc.logger.WithError(err).Errorln("could not swap nodes")
		if _, ok := err.(*incorrectAllocationError); ok {
			alertErr := rc.alerter.Alert(rc.alertInfo(rcFields, err.Error()), alerting.LowUrgency)
			if alertErr != nil {
				rc.logger.WithError(alertErr).Errorln("unable to send alert")
			}
		}
		return false, err
	}

	rc.logger.Infof("transferred off %s", ineligible)
	return true, nil
}

func (rc *replicationController) canNodeTransfer(rcFields fields.RC, current types.PodLocations, ineligibles []types.NodeName) (bool, consul.Unlocker, error) {
	ok, err := rc.sdChecker.IsSyncedWithCluster(rcFields.ID)
	if err != nil {
		rc.logger.WithError(err).Errorf("skipping node transfer; error checking service discovery system session %v", rc.rcID)
		return false, nil, err
	} else if !ok {
		rc.logger.Infoln("skipping node transfer; service discovery system not synced")
		return false, nil, nil
	}

	currentSet := types.NewNodeSet(current.Nodes()...)
	ineligibleSet := types.NewNodeSet(ineligibles...)
	if rcFields.Disabled &&
		currentSet.Equal(ineligibleSet) &&
		rcFields.ReplicasDesired < len(current) {
		// If the RC is disabled, all of its nodes are inelgible, and its
		// desired replica count is less than its current replica count,
		// then it is likely the old RC in an RU. In that case, the new RC
		// will not be able to schedule on the ineligible nodes, so we should
		// perform a node transfer. The RU would not have decremented this RC's
		// replicas desired unless its min health was met, so we can safely
		// perform the swap.
		return true, nil, nil
	}
	ineligible := ineligibles[0]
	ok, err = rc.isTransferMinHealthMet(rcFields, current, ineligible)
	if err != nil {
		rc.logger.WithError(err).Errorln("skipping node transfer; error checking health")
		return false, nil, err
	} else if !ok {
		rc.logger.Infoln("skipping node transfer; node transfer health requirement not met")
		return false, nil, nil
	}

	_, session, err := consul.SessionContext(context.Background(), rc.consulClient, fmt.Sprintf("rc-node-transfer-%s", rc.rcID))
	if err != nil {
		rc.logger.WithError(err).Errorln("could not create a session to acquire rc lock")
		return false, nil, err
	}
	// Node transfers do not mutate the RC's state, but we attempt to acquire
	// the lock to ensure that we do not perform a transfer during a rolling
	// update. This avoids races between the two's health checks and scheduling
	unlocker, err := rc.rcLocker.LockForMutation(rc.rcID, session)
	if err != nil {
		defer session.Destroy()
	}
	switch {
	case consul.IsAlreadyLocked(err):
		rc.logger.Infoln("skipping node transfer; rc mutation lock is already held")
		return false, nil, nil
	case err != nil:
		rc.logger.WithError(err).Errorln("could not acquire rc mutatation lock for node transfer")
		return false, nil, err
	}

	return true, unlocker, nil
}

// isTransferMinHealthMet returns true if either the ineligible node is unhealthy
// (in which case a node transfer would not reduce the cluster's health) or if
// all of the RC's current pods are healthy (in which case the cluster can
// tolerate one pod down)
func (rc *replicationController) isTransferMinHealthMet(rcFields fields.RC, current types.PodLocations, ineligible types.NodeName) (bool, error) {
	service := rcFields.Manifest.ID().String()
	healths, err := rc.healthChecker.Service(service)
	if err != nil {
		return false, util.Errorf("could not get %s health: %s", service, err)
	}
	hlth, ok := healths[ineligible]
	if !ok || hlth.Status != health.Passing {
		// If no health result was returned, the preparer may be down on the
		// node. We should perform a transfer. A transfer off an unhealthy node
		// will not reduce the health of a cluster
		return true, nil
	}
	for _, pod := range current {
		hlth, ok := healths[pod.Node]
		if !ok {
			return false, util.Errorf("no health result returned for %s", pod.Node)
		} else if hlth.Status != health.Passing {
			return false, nil
		}
	}
	return true, nil
}

type incorrectAllocationError struct {
	err             error
	needsDeallocate types.NodeName
	needsAllocate   types.NodeName
}

func (e *incorrectAllocationError) Error() string {
	// If the deallocation or scheduling transaction fail, the new node will be
	// allocated but unused. We'll include that in the message so an operator
	// can deallocate
	msg := fmt.Sprintf("%s: allocations incorrect: deallocate %s", e.err, e.needsDeallocate)
	if e.needsAllocate != "" {
		// If the scheduling transaction fails, the ineligible node will be
		// deallocated but still scheduled on. We'll include that in the message
		// so an operator can reallocate
		msg = fmt.Sprintf("%s, allocate %s", msg, e.needsAllocate)
	}
	return msg
}

// swapNodes allocates a node, deallocates the inelgible node, and
// transactionally schedules on the new node and unschedules from the old
func (rc *replicationController) swapNodes(rcFields fields.RC, current types.PodLocations, ineligible types.NodeName, allocAttempts int) error {
	nodes, err := rc.scheduler.AllocateNodes(rcFields.Manifest, rcFields.NodeSelector, 1, true)
	if err != nil {
		return util.Errorf("node transfer Allocate failed: %s", err)
	} else if len(nodes) < 1 {
		return util.Errorf("node transfer Allocate allocated no nodes: %s", err)
	}
	newNode := nodes[0]

	err = rc.retryDeallocate(rcFields, ineligible, allocAttempts)
	if err != nil {
		return &incorrectAllocationError{
			err:             err,
			needsDeallocate: newNode,
		}
	}
	allocErr := incorrectAllocationError{
		needsDeallocate: newNode,
		needsAllocate:   ineligible,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	txn, cancelFunc := rc.newAuditingTransaction(ctx, rcFields, current.Nodes())
	defer cancelFunc()

	err = rc.unschedule(txn, rcFields, ineligible)
	if err != nil {
		allocErr.err = err
		return &allocErr
	}
	err = rc.schedule(txn, rcFields, newNode)
	if err != nil {
		allocErr.err = err
		return &allocErr
	}

	ok, resp, err := txn.CommitWithRetries(rc.txner)
	if err != nil {
		allocErr.err = util.Errorf("schedule and unschedule transaction could not complete within timeout: %s", err)
		return &allocErr
	} else if !ok {
		// This doesn't make sense because there are no "check" conditions in the transaction
		allocErr.err = util.Errorf(
			"transaction violation trying to swap nodes: %s",
			transaction.TxnErrorsToString(resp.Errors),
		)
		return &allocErr
	}

	return nil
}

func (rc *replicationController) retryDeallocate(rcFields fields.RC, ineligible types.NodeName, attempts int) error {
	for i := 0; i < attempts; i++ {
		backoff := time.Duration(math.Pow(float64(i), 2)) * time.Second
		if backoff > 1*time.Minute {
			backoff = 1 * time.Minute
		}
		time.Sleep(backoff)
		err := rc.scheduler.DeallocateNodes(rcFields.NodeSelector, []types.NodeName{ineligible})
		if err != nil {
			rc.logger.WithError(err).Errorf("node transfer deallocate attempt %d failed", i+1)
			continue
		}
		return nil
	}
	return util.Errorf("deallocate failed all %d attempts", attempts)
}

func (rc *replicationController) checkEligibleForUnused(podID types.PodID, eligible []types.NodeName, current []types.NodeName) (types.NodeName, error) {
	toCheck := types.NewNodeSet(eligible...).Difference(types.NewNodeSet(current...)).ListNodes()
	for _, node := range toCheck {
		_, _, err := rc.consulStore.Pod(consul.INTENT_TREE, node, podID)
		switch {
		case err == pods.NoCurrentManifest:
			return node, nil
		case err != nil:
			return "", util.Errorf("error checking for unused allocation for RC: %s", err)
		}
	}

	// no nodes are unused
	return "", nil
}

func (rc *replicationController) checkMissingArtifacts(rcFields fields.RC) {
	if rcFields.ReplicasDesired == 0 {
		return
	}
	podID := rcFields.Manifest.ID()
	launchableStanzas := rcFields.Manifest.GetLaunchableStanzas()
	for launchableID, launchableStanza := range launchableStanzas {
		if launchableStanza.LaunchableType == "docker" {
			// don't check for missing docker images for now
			continue
		}
		artifactUrl, _, err := rc.artifactRegistry.LocationDataForLaunchable(podID, launchableID, launchableStanza)
		if err != nil {
			rc.logger.WithError(err).Errorln("Unable to retrieve location for launchable")
			continue
		}

		exists, err := rc.artifactRegistry.CheckArtifactExists(artifactUrl)
		if err != nil {
			rc.logger.WithError(err).Errorln("Unexpected error when checking if artifact exists")
			continue
		}
		if !exists {
			hostname, _ := os.Hostname()
			rc.logger.WithFields(logrus.Fields{
				"Description":  fmt.Sprintf("This RC is missing an artifact at url %s for launchable id %s", artifactUrl, launchableID),
				"IncidentKey":  fmt.Sprintf("%s-missing_artifact", rcFields.ID.String()),
				"RCID":         rcFields.ID.String(),
				"Hostname":     hostname,
				"PodId":        rcFields.Manifest.ID().String(),
				"NodeSelector": rcFields.NodeSelector.String(),
			}).Errorln("RC references artifact not available in the registry")
		}
	}
}
