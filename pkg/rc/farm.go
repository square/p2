package rc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	p2metrics "github.com/square/p2/pkg/metrics"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"

	"github.com/rcrowley/go-metrics"
)

// subset of labels.Applicator
type Labeler interface {
	SetLabelsTxn(ctx context.Context, labelType labels.Type, id string, labels map[string]string) error
	RemoveLabelsTxn(ctx context.Context, labelType labels.Type, id string, keysToRemove []string) error
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
	GetMatches(selector klabels.Selector, labelType labels.Type) ([]labels.Labeled, error)
}

// LabelMatcher is a subset of Labeler, but its small size makes it easier to
// call CurrentPods() in code where transactions are not available
type LabelMatcher interface {
	GetMatches(selector klabels.Selector, labelType labels.Type) ([]labels.Labeled, error)
}

type ReplicationControllerStore interface {
	WatchRCKeysWithLockInfo(quit <-chan struct{}, pauseTime time.Duration) (<-chan []rcstore.RCLockResult, <-chan error)
	Get(id fields.ID) (fields.RC, error)
	List() ([]fields.RC, error)
}

type ReplicationControllerLocker interface {
	RCNodeTransferLocker
	LockForOwnership(rcID fields.ID, session consul.Session) (consul.Unlocker, error)
}

type AuditLogStore interface {
	Create(
		ctx context.Context,
		eventType audit.EventType,
		eventDetails json.RawMessage,
	) error
}

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
	store         consulStore
	client        consulutil.ConsulClient
	rcStatusStore rcstatus.ConsulStore
	auditLogStore AuditLogStore
	rcStore       ReplicationControllerStore
	rcLocker      ReplicationControllerLocker
	rcWatcher     ReplicationControllerWatcher
	scheduler     Scheduler
	labeler       Labeler
	txner         transaction.Txner
	healthChecker checker.ConsulHealthChecker

	// session stream for the rcs locked by this farm
	sessions <-chan string

	children map[fields.ID]childRC
	childMu  sync.Mutex
	session  consul.Session

	logger     logging.Logger
	alerter    alerting.Alerter
	rcSelector klabels.Selector

	// The length of time to wait between a watch returning and initiating
	// the next. This is useful for tuning QPS and bandwidth to the
	// datastore. Higher values will result in delays in processing newly
	// created RCs but lower bandwidth usage and QPS.
	rcWatchPauseTime time.Duration

	artifactRegistry artifact.Registry
}

type childRC struct {
	rc       ReplicationController
	unlocker consul.Unlocker
	quit     chan<- struct{}
}

func NewFarm(
	store consulStore,
	client consulutil.ConsulClient,
	rcStatusStore rcstatus.ConsulStore,
	auditLogStore AuditLogStore,
	rcs ReplicationControllerStore,
	rcLocker ReplicationControllerLocker,
	rcWatcher ReplicationControllerWatcher,
	txner transaction.Txner,
	healthChecker checker.ConsulHealthChecker,
	scheduler Scheduler,
	labeler Labeler,
	sessions <-chan string,
	logger logging.Logger,
	rcSelector klabels.Selector,
	alerter alerting.Alerter,
	rcWatchPauseTime time.Duration,
	artifactRegistry artifact.Registry,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &Farm{
		store:            store,
		client:           client,
		rcStatusStore:    rcStatusStore,
		auditLogStore:    auditLogStore,
		rcStore:          rcs,
		rcLocker:         rcLocker,
		rcWatcher:        rcWatcher,
		txner:            txner,
		healthChecker:    healthChecker,
		scheduler:        scheduler,
		labeler:          labeler,
		sessions:         sessions,
		logger:           logger,
		children:         make(map[fields.ID]childRC),
		alerter:          alerter,
		rcSelector:       rcSelector,
		rcWatchPauseTime: rcWatchPauseTime,
		artifactRegistry: artifactRegistry,
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
		rcf.session = rcf.store.NewUnmanagedSession(sessionID, "")
		rcf.mainLoop(sessionQuit)
	})
}

func (rcf *Farm) mainLoop(quit <-chan struct{}) {
	rcf.initialFailsafe()

	subQuit := make(chan struct{})
	defer close(subQuit)

	rcKeyWatch, rcErr := rcf.rcStore.WatchRCKeysWithLockInfo(subQuit, rcf.rcWatchPauseTime)

	go func(errCh <-chan error) {
		for {
			select {
			case <-quit:
				return
			case err := <-errCh:
				rcf.logger.WithError(err).Errorln("Could not read consul replication controllers")
			}
		}
	}(rcErr)

START_LOOP:
	for {
		// Check the quit channel independently of the others before entering a multi-channel select.
		// This gives the quit channel priority over the others and ensures we quit in a timely manner
		// to avoid a situation where multiple farms handle the same RC.
		select {
		case <-quit:
			rcf.logger.NoFields().Infoln("Session expired, releasing replication controllers")
			rcf.session = nil
			rcf.releaseChildren()
			return
		default:
		}

		select {
		case <-quit:
			rcf.logger.NoFields().Infoln("Session expired, releasing replication controllers")
			rcf.session = nil
			rcf.releaseChildren()
			return
		case rcKeys := <-rcKeyWatch:
			startTime := time.Now()
			rcf.logger.WithField("n", len(rcKeys)).Debugln("Received replication controller update")
			countHistogram := metrics.GetOrRegisterHistogram("rc_count", p2metrics.Registry, metrics.NewExpDecaySample(1028, 0.015))
			countHistogram.Update(int64(len(rcKeys)))

			rcf.failsafe(rcKeys)

			// track which children were found in the returned set
			foundChildren := make(map[fields.ID]struct{})
			for _, rcKey := range rcKeys {
				rcLogger := rcf.logger.SubLogger(logrus.Fields{
					"rc": rcKey.ID,
				})
				if _, ok := rcf.children[rcKey.ID]; ok {
					// this one is already ours, skip
					rcLogger.NoFields().Debugln("Got replication controller already owned by self")
					foundChildren[rcKey.ID] = struct{}{}
					continue
				}

				// Don't try to work on an RC that is already owned. While the LockedForOwnership flag may be stale,
				// the nature of this function is that we (or another farm) will come back to it and the lock will be
				// grabbed. Shortening the length of time it takes to process a list of RCs is paramount.
				if rcKey.LockedForOwnership {
					continue
				}

				shouldWorkOnRC, err := rcf.shouldWorkOn(rcKey.ID)
				if err != nil {
					rcLogger.WithError(err).Errorf("Could not determine if should work on RC %s, skipping", rcKey.ID)
					continue
				}

				if !shouldWorkOnRC {
					rcLogger.Infof("Ignoring RC %s, not meant for this farm", rcKey.ID)
					continue
				}

				rcUnlocker, err := rcf.rcLocker.LockForOwnership(rcKey.ID, rcf.session)
				if _, ok := err.(consul.AlreadyLockedError); ok {
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
					rcKey.ID,
					rcf.store,
					rcf.client,
					rcf.rcLocker,
					rcf.rcStatusStore,
					rcf.auditLogStore,
					rcf.txner,
					rcf.rcWatcher,
					rcf.scheduler,
					rcf.labeler,
					rcLogger,
					rcf.alerter,
					rcf.healthChecker,
					rcf.artifactRegistry,
				)
				childQuit := make(chan struct{})
				rcf.children[rcKey.ID] = childRC{
					rc:       newChild,
					quit:     childQuit,
					unlocker: rcUnlocker,
				}
				foundChildren[rcKey.ID] = struct{}{}

				go func(id fields.ID) {
					defer func() {
						if r := recover(); r != nil {
							err := util.Errorf("Caught panic in rc farm: %s", r)

							stackErr, ok := err.(util.StackError)
							msg := "Caught panic in rc farm"
							if ok {
								msg = fmt.Sprintf("%s:\n%s", msg, stackErr.Stack())
							}
							rcLogger.WithError(err).
								WithField("rc_id", id).
								Errorln(msg)
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
				}(rcKey.ID)
			}

			// now remove any children that were not found in the result set
			rcf.releaseDeletedChildren(foundChildren)
			endTime := time.Now()
			processingTime := endTime.Sub(startTime)
			rcf.logger.WithField("rc_processing_time", processingTime.String()).Infoln("Finished processing RC update")
			histogram := metrics.GetOrRegisterHistogram("rc_processing_time", p2metrics.Registry, metrics.NewExpDecaySample(1028, 0.015))
			histogram.Update(int64(processingTime))
		}
	}
}

// This failsafe is only run at startup of the farm for performance reasons. It checks two conditions:
// 1) there is at least one RC
// 2) the sum of the replicas_desired fields for all RCs is greater than zero.
//
// If either of those conditions are true, the function panics to prevent the
// RC farm from uninstalling pods. There is an additional failsafe mechanism in
// that an RC cannot be unmarshaled from JSON if there is no replicas_desired
// field which would be the most likely way that all RC counts would be zeroed
// out.
func (rcf *Farm) initialFailsafe() {
	rcFields, err := rcf.rcStore.List()
	if err != nil {
		panic(fmt.Sprintf("unable to do initial list of RCs to evaluate failsafe check: %s", err))
	}

	if len(rcFields) == 0 {
		if err := rcf.alerter.Alert(alerting.AlertInfo{
			Description: "No RCs have been scheduled",
			IncidentKey: "no_rcs_found",
		}, alerting.HighUrgency); err != nil {
			rcf.logger.WithError(err).Errorln("Unable to deliver alert!")
		}
		panic("No RCs are scheduled at all. Create one RC to enable the farm. Panicking to escape a potentially bad situation.")
	}

	for _, rc := range rcFields {
		if rc.ReplicasDesired > 0 {
			// If at least one RC has a > 0 count, we're good
			return
		}
	}

	if err := rcf.alerter.Alert(alerting.AlertInfo{
		Description: "All RCs have zero replicas requested",
		IncidentKey: "zero_replicas_found",
	}, alerting.HighUrgency); err != nil {
		rcf.logger.WithError(err).Errorln("Unable to deliver alert!")
	}
	panic("The sum of all replicas is 0. Panicking to escape a potentially bad situation")
}

// Runs every time the RC key watch returns data. Panics if there are zero keys
// as that is not expected under normal operation of an RC farm.
func (rcf *Farm) failsafe(rcs []rcstore.RCLockResult) {
	// FAILSAFES. If no RCs are scheduled, or there are zero replicas of anything scheduled, panic
	if len(rcs) == 0 {
		if err := rcf.alerter.Alert(alerting.AlertInfo{
			Description: "No RCs have been scheduled",
			IncidentKey: "no_rcs_found",
		}, alerting.HighUrgency); err != nil {
			rcf.logger.WithError(err).Errorln("Unable to deliver alert!")
		}
		panic("No RCs are scheduled at all. Create one RC to enable the farm. Panicking to escape a potentially bad situation.")
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
