package rc

import (
	"fmt"
	"os"
	"time"

	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/util"
)

const (
	// This label is applied to pods owned by an RC.
	RCIDLabel = "replication_controller_id"
)

type ReplicationController interface {
	ID() store.ReplicationControllerID

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
	CurrentPods() (store.PodLocations, error)
}

// These methods are the same as the methods of the same name in kp.Store.
// Replication controllers have no need of any methods other than these.
type kpStore interface {
	SetPod(
		podPrefix kp.PodPrefix,
		nodeName store.NodeName,
		manifest store.Manifest,
	) (time.Duration, error)

	Pod(
		podPrefix kp.PodPrefix,
		nodeName store.NodeName,
		podId store.PodID,
	) (store.Manifest, time.Duration, error)

	DeletePod(podPrefix kp.PodPrefix,
		nodeName store.NodeName,
		manifestID store.PodID,
	) (time.Duration, error)
}

type replicationController struct {
	store.ReplicationController

	logger logging.Logger

	kpStore       kpStore
	rcStore       rcstore.Store
	scheduler     scheduler.Scheduler
	podApplicator Labeler
	alerter       alerting.Alerter
}

func New(
	fields store.ReplicationController,
	kpStore kpStore,
	rcStore rcstore.Store,
	scheduler scheduler.Scheduler,
	podApplicator Labeler,
	logger logging.Logger,
	alerter alerting.Alerter,
) ReplicationController {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &replicationController{
		ReplicationController: fields,

		logger:        logger,
		kpStore:       kpStore,
		rcStore:       rcStore,
		scheduler:     scheduler,
		podApplicator: podApplicator,
		alerter:       alerter,
	}
}

func (rc *replicationController) ID() store.ReplicationControllerID {
	return rc.ReplicationController.ID
}

func (rc *replicationController) WatchDesires(quit <-chan struct{}) <-chan error {
	desiresChanged, errInChannel := rc.rcStore.Watch(&rc.ReplicationController, quit)

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

	rc.logger.NoFields().Infof("Currently on nodes %s", current)

	nodesChanged := false
	if rc.ReplicasDesired > len(current) {
		err := rc.addPods(current)
		if err != nil {
			return err
		}
		nodesChanged = true
	} else if len(current) > rc.ReplicasDesired {
		err := rc.removePods(current)
		if err != nil {
			return err
		}
		nodesChanged = true
	} else {
		rc.logger.NoFields().Debugln("Taking no action")
	}

	if nodesChanged {
		current, err = rc.CurrentPods()
		if err != nil {
			return err
		}
	}

	return rc.ensureConsistency(current)
}

func (rc *replicationController) addPods(current store.PodLocations) error {
	currentNodes := current.Nodes()
	eligible, err := rc.eligibleNodes()
	if err != nil {
		return err
	}

	// TODO: With Docker or runc we would not be constrained to running only once per node.
	// So it may be the case that we need to make the Scheduler interface smarter and use it here.
	possible := store.NewNodeSet(eligible...).Difference(store.NewNodeSet(currentNodes...))

	// Users want deterministic ordering of nodes being populated to a new
	// RC. Move nodes in sorted order by hostname to achieve this
	possibleSorted := possible.ListNodes()
	toSchedule := rc.ReplicasDesired - len(currentNodes)

	rc.logger.NoFields().Infof("Need to schedule %d nodes out of %s", toSchedule, possible)

	for i := 0; i < toSchedule; i++ {
		if len(possibleSorted) < i+1 {
			errMsg := fmt.Sprintf(
				"Not enough nodes to meet desire: %d replicas desired, %d currentNodes, %d eligible. Scheduled on %d nodes instead.",
				rc.ReplicasDesired, len(currentNodes), len(eligible), i,
			)
			err := rc.alerter.Alert(rc.alertInfo(errMsg))
			if err != nil {
				rc.logger.WithError(err).Errorln("Unable to send alert")
			}
			return util.Errorf(errMsg)
		}
		scheduleOn := possibleSorted[i]

		err := rc.schedule(scheduleOn)
		if err != nil {
			return err
		}
	}
	return nil
}

// Generates an alerting.AlertInfo struct. Includes information relevant to
// debugging an RC. Attempts to include the hostname the RC is running on as
// well
func (rc *replicationController) alertInfo(msg string) alerting.AlertInfo {
	hostname, _ := os.Hostname()

	return alerting.AlertInfo{
		Description: msg,
		IncidentKey: rc.ID().String(),
		Details: struct {
			RCID         string `json:"rc_id"`
			Hostname     string `json:"hostname"`
			PodId        string `json:"pod_id"`
			NodeSelector string `json:"node_selector"`
		}{
			RCID:         rc.ID().String(),
			Hostname:     hostname,
			PodId:        rc.Manifest.ID().String(),
			NodeSelector: rc.NodeSelector.String(),
		},
	}
}

func (rc *replicationController) removePods(current store.PodLocations) error {
	currentNodes := current.Nodes()
	eligible, err := rc.eligibleNodes()
	if err != nil {
		return err
	}

	// If we need to downsize the number of nodes, prefer any in current that are not eligible anymore.
	// TODO: evaluate changes to 'eligible' more frequently
	preferred := store.NewNodeSet(currentNodes...).Difference(store.NewNodeSet(eligible...))
	rest := store.NewNodeSet(currentNodes...).Difference(preferred)
	toUnschedule := len(current) - rc.ReplicasDesired
	rc.logger.NoFields().Infof("Need to unschedule %d nodes out of %s", toUnschedule, current)

	for i := 0; i < toUnschedule; i++ {
		unscheduleFrom, ok := preferred.PopAny()
		if !ok {
			var ok bool
			unscheduleFrom, ok = rest.PopAny()
			if !ok {
				// This should be mathematically impossible unless replicasDesired was negative
				return util.Errorf(
					"Unable to unschedule enough nodes to meet replicas desired: %d replicas desired, %d current.",
					rc.ReplicasDesired, len(current),
				)
			}
		}
		err := rc.unschedule(unscheduleFrom)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rc *replicationController) ensureConsistency(current store.PodLocations) error {
	manifestSHA, err := rc.Manifest.SHA()
	if err != nil {
		return err
	}
	for _, pod := range current {
		intent, _, err := rc.kpStore.Pod(kp.INTENT_TREE, pod.Node, store.PodID(pod.PodID))
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
		if err := rc.schedule(pod.Node); err != nil {
			return err
		}
	}

	return nil
}

func (rc *replicationController) eligibleNodes() ([]store.NodeName, error) {
	return rc.scheduler.EligibleNodes(rc.Manifest, rc.NodeSelector)
}

func (rc *replicationController) CurrentPods() (store.PodLocations, error) {
	selector := klabels.Everything().Add(RCIDLabel, klabels.EqualsOperator, []string{rc.ID().String()})

	// replication controllers can only pass cachedMatch = false because their operations for matching
	// replica counts are not necessarily idempotent.
	podMatches, err := rc.podApplicator.GetMatches(selector, labels.POD, false)
	if err != nil {
		return nil, err
	}

	result := make(store.PodLocations, len(podMatches))
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

// forEachLabel Attempts to apply the supplied function to all user-supplied labels
// and the reserved labels.
// If forEachLabel encounters any error applying the function, it returns that error immediately.
// The function is not further applied to subsequent labels on an error.
func (rc *replicationController) forEachLabel(node store.NodeName, f func(id, k, v string) error) error {
	id := labels.MakePodLabelKey(node, rc.Manifest.ID())

	// user-requested labels.
	for k, v := range rc.PodLabels {
		if err := f(id, k, v); err != nil {
			return err
		}
	}

	// our reserved labels (pod id and replication controller id)
	err := f(id, rcstore.PodIDLabel, rc.Manifest.ID().String())
	if err != nil {
		return err
	}
	return f(id, RCIDLabel, rc.ID().String())
}

func (rc *replicationController) schedule(node store.NodeName) error {
	rc.logger.NoFields().Infof("Scheduling on %s", node)
	err := rc.forEachLabel(node, func(podID, k, v string) error {
		return rc.podApplicator.SetLabel(labels.POD, podID, k, v)
	})
	if err != nil {
		return err
	}

	_, err = rc.kpStore.SetPod(kp.INTENT_TREE, node, rc.Manifest)
	return err
}

func (rc *replicationController) unschedule(node store.NodeName) error {
	rc.logger.NoFields().Infof("Unscheduling from %s", node)
	_, err := rc.kpStore.DeletePod(kp.INTENT_TREE, node, rc.Manifest.ID())
	if err != nil {
		return err
	}

	return rc.forEachLabel(node, func(podID, k, _ string) error {
		return rc.podApplicator.RemoveLabel(labels.POD, podID, k)
	})
}
