package rc

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/rcstore"
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
	DeallocateNodes(nodes []types.NodeName) error
}

var _ Scheduler = &scheduler.ApplicatorScheduler{}

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

// replicationController wraps a fields.RC with information required to manage the RC.
// Note: the fields.RC might be mutated during this struct's lifetime, so a mutex is
// used to synchronize access to it
type replicationController struct {
	fields.RC
	mu sync.Mutex

	logger logging.Logger

	consulStore   consulStore
	auditLogStore AuditLogStore
	txner         transaction.Txner
	rcWatcher     ReplicationControllerWatcher
	scheduler     Scheduler
	podApplicator Labeler
	alerter       alerting.Alerter
}

type ReplicationControllerWatcher interface {
	Watch(rc *fields.RC, mu *sync.Mutex, quit <-chan struct{}) (<-chan struct{}, <-chan error)
}

func New(
	fields fields.RC,
	consulStore consulStore,
	auditLogStore AuditLogStore,
	txner transaction.Txner,
	rcWatcher ReplicationControllerWatcher,
	scheduler Scheduler,
	podApplicator Labeler,
	logger logging.Logger,
	alerter alerting.Alerter,
) ReplicationController {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &replicationController{
		RC: fields,

		logger:        logger,
		consulStore:   consulStore,
		auditLogStore: auditLogStore,
		txner:         txner,
		rcWatcher:     rcWatcher,
		scheduler:     scheduler,
		podApplicator: podApplicator,
		alerter:       alerter,
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

	// If we're disabled, we do nothing, nor is it an error
	// (it's a normal possibility to be disabled)
	if rc.Disabled {
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
		err := rc.addPods(current, eligible)
		if err != nil {
			return err
		}
		nodesChanged = true
	case len(current) > rc.ReplicasDesired:
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

	rc.checkForIneligible(current, eligible)

	return rc.ensureConsistency(current)
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
			err := rc.alerter.Alert(rc.alertInfo(errMsg))
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

func (rc *replicationController) ensureConsistency(current types.PodLocations) error {
	rc.mu.Lock()
	manifest := rc.Manifest
	rc.mu.Unlock()
	manifestSHA, err := manifest.SHA()
	if err != nil {
		return err
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer func() {
		cancelFunc()
	}()
	for i, pod := range current {
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
		intent, _, err := rc.consulStore.Pod(consul.INTENT_TREE, pod.Node, types.PodID(pod.PodID))
		if err != nil && err != pods.NoCurrentManifest {
			return err
		}
		var intentSHA string
		if intent != nil {
			intentSHA, err = intent.SHA()
			if err != nil {
				rc.logger.WithError(err).WithField("node", pod.Node).Warn("Could not hash manifest to determine consistency of intent")
			}
			if intentSHA == manifestSHA {
				continue
			}
		}

		rc.logger.WithField("node", pod.Node).WithField("intentManifestSHA", intentSHA).Info("Found inconsistency in scheduled manifest")

		if err := rc.scheduleNoAudit(ctx, pod.Node); err != nil {
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

func (rc *replicationController) checkForIneligible(current types.PodLocations, eligible []types.NodeName) {
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

	if len(ineligibleCurrent) > 0 {
		errMsg := fmt.Sprintf("RC has scheduled %d ineligible nodes: %s", len(ineligibleCurrent), ineligibleCurrent)
		err := rc.alerter.Alert(rc.alertInfo(errMsg))
		if err != nil {
			rc.logger.WithError(err).Errorln("Unable to send alert")
		}
	}
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
