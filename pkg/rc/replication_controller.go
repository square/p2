package rc

import (
	"strings"
	"time"

	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/util/sets"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
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

	// CurrentNodes() returns all nodes that this replication controller is currently scheduled on,
	// by selecting pods labeled with the replication controller's ID.
	CurrentNodes() ([]string, error)

	// Internal: meetDesires synchronously schedules or unschedules pods to meet desired state.
	meetDesires() error
}

// These methods are the same as the methods of the same name in kp.Store.
// Replication controllers have no need of any methods other than these.
type kpStore interface {
	SetPod(key string, manifest pods.Manifest) (time.Duration, error)
	DeletePod(key string) (time.Duration, error)
}

type replicationController struct {
	fields.RC

	logger logging.Logger

	kpStore       kpStore
	rcStore       rcstore.Store
	scheduler     Scheduler
	podApplicator labels.Applicator
}

func New(
	fields fields.RC,
	kpStore kpStore,
	rcStore rcstore.Store,
	scheduler Scheduler,
	podApplicator labels.Applicator,
	logger logging.Logger,
) ReplicationController {
	return &replicationController{
		RC: fields,

		logger:        logger,
		kpStore:       kpStore,
		rcStore:       rcStore,
		scheduler:     scheduler,
		podApplicator: podApplicator,
	}
}

func (rc *replicationController) ID() fields.ID {
	return rc.RC.ID
}

func (rc *replicationController) WatchDesires(quit <-chan struct{}) <-chan error {
	desiresChanged, errInChannel := rc.rcStore.Watch(&rc.RC, quit)

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
	rc.logger.NoFields().Infof("Meeting with desired replicas %d, disabled %v", rc.ReplicasDesired, rc.Disabled)

	// If we're disabled, we do nothing, nor is it an error
	// (it's a normal possibility to be disabled)
	if rc.Disabled {
		return nil
	}

	current, err := rc.CurrentNodes()
	if err != nil {
		return err
	}
	rc.logger.NoFields().Infof("Currently on nodes %s", current)

	if rc.ReplicasDesired > len(current) {
		eligible, err := rc.eligibleNodes()
		if err != nil {
			return err
		}

		// TODO: With Docker or runc we would not be constrained to running only once per node.
		// So it may be the case that we need to make the Scheduler interface smarter and use it here.
		possible := sets.NewString(eligible...).Difference(sets.NewString(current...))
		toSchedule := rc.ReplicasDesired - len(current)

		rc.logger.NoFields().Infof("Need to schedule %d nodes out of %s", toSchedule, possible)

		for i := 0; i < toSchedule; i++ {
			scheduleOn, ok := possible.PopAny()
			if !ok {
				return util.Errorf(
					"Not enough nodes to meet desire: %d replicas desired, %d current, %d eligible. Scheduled on %d nodes instead.",
					rc.ReplicasDesired, len(current), len(eligible), i,
				)
			}

			err := rc.schedule(scheduleOn)
			if err != nil {
				return err
			}
		}
	} else if len(current) > rc.ReplicasDesired {
		eligible, err := rc.eligibleNodes()
		if err != nil {
			return err
		}

		// If we need to downsize the number of nodes, prefer any in current that are not eligible anymore.
		// TODO: evaluate changes to 'eligible' more frequently
		preferred := sets.NewString(current...).Difference(sets.NewString(eligible...))
		rest := sets.NewString(current...).Difference(preferred)
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
	} else {
		rc.logger.NoFields().Debugln("Taking no action")
	}

	return nil
}

func (rc *replicationController) eligibleNodes() ([]string, error) {
	return rc.scheduler.EligibleNodes(rc.Manifest, rc.NodeSelector)
}

func (rc *replicationController) CurrentNodes() ([]string, error) {
	selector := klabels.Everything().Add(RCIDLabel, klabels.EqualsOperator, []string{rc.ID().String()})

	pods, err := rc.podApplicator.GetMatches(selector, labels.POD)
	if err != nil {
		return []string{}, nil
	}

	// ID will be something like <nodename>/<podid>.
	// We just want the nodename.
	result := make([]string, len(pods))
	for i, node := range pods {
		parts := strings.SplitN(node.ID, "/", 2)
		// If it so happens that node.ID contains no "/",
		// then parts[0] is the entire string, so we're OK
		result[i] = parts[0]
	}
	return result, nil
}

// forEachLabel Attempts to apply the supplied function to all user-supplied labels
// and the reserved labels.
// If forEachLabel encounters any error applying the function, it returns that error immediately.
// The function is not further applied to subsequent labels on an error.
func (rc *replicationController) forEachLabel(node string, f func(id, k, v string) error) error {
	id := node + "/" + string(rc.Manifest.ID())

	// user-requested labels.
	for k, v := range rc.PodLabels {
		if err := f(id, k, v); err != nil {
			return err
		}
	}
	// our reserved labels.
	return f(id, RCIDLabel, rc.ID().String())
}

func (rc *replicationController) schedule(node string) error {
	// First, schedule the new pod.
	intentPath := kp.IntentPath(node, string(rc.Manifest.ID()))
	rc.logger.NoFields().Infof("Scheduling on %s", intentPath)
	_, err := rc.kpStore.SetPod(intentPath, rc.Manifest)
	if err != nil {
		return err
	}

	// Then we set the labels.
	// TODO: It could be the case that these SetLabel fail.
	// In this case, we've scheduled a pod but currentNodes() will never be aware of it.
	// We could consider cleaning the now-orphaned pod up?
	return rc.forEachLabel(node, func(id, k, v string) error {
		return rc.podApplicator.SetLabel(labels.POD, id, k, v)
	})
}

func (rc *replicationController) unschedule(node string) error {
	intentPath := kp.IntentPath(node, string(rc.Manifest.ID()))
	rc.logger.NoFields().Infof("Uncheduling from %s", intentPath)

	// TODO: As above in schedule, it could be the case that RemoveLabel fails.
	// This again means that currentNodes() is no longer aware of a pod.
	// But the pod is still running (we haven't called DeletePod yet)
	// So, the pod gets orphaned. Any way to clean it up?
	err := rc.forEachLabel(node, func(id, k, _ string) error {
		return rc.podApplicator.RemoveLabel(labels.POD, id, k)
	})
	if err != nil {
		return err
	}

	_, err = rc.kpStore.DeletePod(intentPath)
	return err
}
