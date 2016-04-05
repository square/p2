package rc

import (
	"sync"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/util"
)

// The Farm is responsible for spawning and reaping replication controllers
// as they are added to and deleted from Consul. Multiple farms can exist
// simultaneously, but each one must hold a different Consul session. This
// ensures that the farms do not instantiate the same replication controller
// multiple times.
//
// RC farms take an RC selector that is used to decide whether this farm should
// pick up a particular RC. This can be used to assist in RC partitioning of
// work or to create test environments. Note that this is _not_ required for RC
// farms to cooperatively schedule work.
type Farm struct {
	// constructor arguments for rcs created by this farm
	kpStore   kp.Store
	rcStore   rcstore.Store
	scheduler Scheduler
	labeler   labels.Applicator

	// session stream for the rcs locked by this farm
	sessions <-chan string

	children map[fields.ID]childRC
	childMu  sync.Mutex
	session  kp.Session

	logger     logging.Logger
	alerter    alerting.Alerter
	rcSelector klabels.Selector
}

type childRC struct {
	rc       ReplicationController
	unlocker kp.Unlocker
	quit     chan<- struct{}
}

func NewFarm(
	kpStore kp.Store,
	rcs rcstore.Store,
	scheduler Scheduler,
	labeler labels.Applicator,
	sessions <-chan string,
	logger logging.Logger,
	rcSelector klabels.Selector,
	alerter alerting.Alerter,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &Farm{
		kpStore:    kpStore,
		rcStore:    rcs,
		scheduler:  scheduler,
		labeler:    labeler,
		sessions:   sessions,
		logger:     logger,
		children:   make(map[fields.ID]childRC),
		alerter:    alerter,
		rcSelector: rcSelector,
	}
}

// Start is a blocking function that monitors Consul for replication controllers.
// The Farm will attempt to claim replication controllers as they appear and,
// if successful, will start goroutines for those replication controllers to do
// their job. Closing the quit channel will cause this function to return,
// releasing all locks it holds.
//
// Start is not safe for concurrent execution. Do not execute multiple
// concurrent instances of Start.
func (rcf *Farm) Start(quit <-chan struct{}) {
	consulutil.WithSession(quit, rcf.sessions, func(sessionQuit <-chan struct{}, sessionID string) {
		rcf.logger.WithField("session", sessionID).Infoln("Acquired new session")
		rcf.session = rcf.kpStore.NewUnmanagedSession(sessionID, "")
		rcf.mainLoop(sessionQuit)
	})
}

func (rcf *Farm) mainLoop(quit <-chan struct{}) {
	subQuit := make(chan struct{})
	defer close(subQuit)
	rcWatch, rcErr := rcf.rcStore.WatchNew(subQuit)

START_LOOP:
	for {
		select {
		case <-quit:
			rcf.logger.NoFields().Infoln("Session expired, releasing replication controllers")
			rcf.session = nil
			rcf.releaseChildren()
			return
		case err := <-rcErr:
			rcf.logger.WithError(err).Errorln("Could not read consul replication controllers")
		case rcFields := <-rcWatch:
			startTime := time.Now()
			rcf.logger.WithField("n", len(rcFields)).Debugln("Received replication controller update")

			rcf.failsafe(rcFields)

			// track which children were found in the returned set
			foundChildren := make(map[fields.ID]struct{})
			for _, rcField := range rcFields {
				rcLogger := rcf.logger.SubLogger(logrus.Fields{
					"rc":  rcField.ID,
					"pod": rcField.Manifest.ID(),
				})
				if _, ok := rcf.children[rcField.ID]; ok {
					// this one is already ours, skip
					rcLogger.NoFields().Debugln("Got replication controller already owned by self")
					foundChildren[rcField.ID] = struct{}{}
					continue
				}

				shouldWorkOnRC, err := rcf.shouldWorkOn(rcField.ID)
				if err != nil {
					rcLogger.WithError(err).Errorf("Could not determine if should work on RC %s, skipping", rcField.ID)
					continue
				}

				if !shouldWorkOnRC {
					rcLogger.WithField("rc", rcField.ID).Infof("Ignoring RC %s, not meant for this farm", rcField.ID)
					continue
				}

				rcUnlocker, err := rcf.rcStore.LockForOwnership(rcField.ID, rcf.session)
				if _, ok := err.(kp.AlreadyLockedError); ok {
					// someone else must have gotten it first - log and move to
					// the next one
					rcLogger.NoFields().Debugln("Lock on replication controller was denied")
					continue
				} else if err != nil {
					rcLogger.WithError(err).Errorln("Got error while locking replication controller - session may be expired")
					// stop processing this update and go back to the select
					// chances are this error is a network problem or session
					// expiry, and all the others in this update would also fail
					continue START_LOOP
				}

				// at this point the rc is ours, time to spin it up
				rcLogger.NoFields().Infoln("Acquired lock on new replication controller, spawning")

				newChild := New(
					rcField,
					rcf.kpStore,
					rcf.rcStore,
					rcf.scheduler,
					rcf.labeler,
					rcLogger,
					rcf.alerter,
				)
				childQuit := make(chan struct{})
				rcf.children[rcField.ID] = childRC{
					rc:       newChild,
					quit:     childQuit,
					unlocker: rcUnlocker,
				}
				foundChildren[rcField.ID] = struct{}{}

				go func(id fields.ID) {
					defer func() {
						if r := recover(); r != nil {
							err := util.Errorf("Caught panic in rc farm: %s", r)
							rcLogger.WithError(err).
								WithField("rc_id", rcField.ID).
								Errorln("Caught panic in rc farm")
						}
					}()
					// disabled-ness is handled in watchdesires
					for err := range newChild.WatchDesires(childQuit) {
						rcLogger.WithError(err).Errorln("Got error in replication controller loop")
					}

					// Release the child so that another farm can reattempt
					rcf.childMu.Lock()
					defer rcf.childMu.Unlock()
					if _, ok := rcf.children[id]; ok {
						rcf.releaseChild(id)
					}
				}(rcField.ID)
			}

			// now remove any children that were not found in the result set
			rcf.releaseDeletedChildren(foundChildren)
			endTime := time.Now()
			rcf.logger.WithField("rc_processing_time", endTime.Sub(startTime).String()).Infoln("Finished processing RC update")
		}
	}
}

func (rcf *Farm) failsafe(rcFields []fields.RC) {
	// FAILSAFES. If no RCs are scheduled, or there are zero replicas of anything scheduled, panic
	if len(rcFields) == 0 {
		rcf.alerter.Alert(alerting.AlertInfo{
			Description: "No RCs have been scheduled",
			IncidentKey: "no_rcs_found",
		})
		panic("No RCs are scheduled at all. Create one RC to enable the farm. Panicking to escape a potentially bad situation.")
	}
	globalReplicaCount := 0
	for _, rc := range rcFields {
		if rc.ReplicasDesired > 0 {
			return
		}
		globalReplicaCount += rc.ReplicasDesired
	}
	if globalReplicaCount == 0 {
		rcf.alerter.Alert(alerting.AlertInfo{
			Description: "All RCs have zero replicas requested",
			IncidentKey: "zero_replicas_found",
		})
		panic("The sum of all replicas is 0. Panicking to escape a potentially bad situation")
	}
}

func (rcf *Farm) releaseDeletedChildren(foundChildren map[fields.ID]struct{}) {
	rcf.childMu.Lock()
	defer rcf.childMu.Unlock()
	rcf.logger.NoFields().Debugln("Pruning replication controllers that have disappeared")
	for id := range rcf.children {
		if _, ok := foundChildren[id]; !ok {
			rcf.releaseChild(id)
		}
	}
}

// test if the farm should work on the given replication controller ID
func (rcf *Farm) shouldWorkOn(rcID fields.ID) (bool, error) {
	if rcf.rcSelector.Empty() {
		return true, nil
	}
	labels, err := rcf.labeler.GetLabels(labels.RC, rcID.String())
	if err != nil {
		return false, err
	}
	return rcf.rcSelector.Matches(labels.Labels), nil
}

// close one child
// should only be called with rcf.childMu locked
func (rcf *Farm) releaseChild(id fields.ID) {
	rcf.logger.WithField("rc", id).Infoln("Releasing replication controller")
	close(rcf.children[id].quit)

	// if our lock is active, attempt to gracefully release it on this rc
	if rcf.session != nil {
		unlocker := rcf.children[id].unlocker
		err := unlocker.Unlock()
		if err != nil {
			rcf.logger.WithField("rc", id).Warnln("Could not release replication controller lock")
		}
	}
	delete(rcf.children, id)
}

// close all children
func (rcf *Farm) releaseChildren() {
	rcf.childMu.Lock()
	defer rcf.childMu.Unlock()
	for id := range rcf.children {
		// it's safe to delete this element during iteration,
		// because we have already iterated over it
		rcf.releaseChild(id)
	}
}
