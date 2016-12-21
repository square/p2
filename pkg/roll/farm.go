package roll

import (
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rcrowley/go-metrics"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/kp/rollstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	p2metrics "github.com/square/p2/pkg/metrics"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"
)

type Factory interface {
	New(store.RollingUpdate, logging.Logger, kp.Session, alerting.Alerter) Update
}

type UpdateFactory struct {
	Store         Store
	RCStore       rcstore.Store
	HealthChecker checker.ConsulHealthChecker
	Labeler       rc.Labeler
	Scheduler     scheduler.Scheduler
}

func (f UpdateFactory) New(u store.RollingUpdate, l logging.Logger, session kp.Session, alerter alerting.Alerter) Update {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return NewUpdate(u, f.Store, f.RCStore, f.HealthChecker, f.Labeler, f.Scheduler, l, session, alerter)
}

type RCGetter interface {
	Get(id store.ReplicationControllerID) (store.ReplicationController, error)
}

// The Farm is responsible for spawning and reaping rolling updates as they are
// added to and deleted from Consul. Multiple farms can exist simultaneously,
// but each one must hold a different Consul session. This ensures that the
// farms do not instantiate the same rolling update multiple times.
//
// Roll farms take an RC selector that is used to decide whether this farm should
// pick up a particular RU request. This can be used to assist in RU partitioning
// of work or to create test environments. Note that this is _not_ required for RU farms
// to cooperatively schedule work.
type Farm struct {
	factory  Factory
	store    Store
	rls      rollstore.Store
	rcs      RCGetter
	sessions <-chan string

	children map[store.RollingUpdateID]childRU
	childMu  sync.Mutex
	session  kp.Session

	logger  logging.Logger
	alerter alerting.Alerter

	labeler    rc.Labeler
	rcSelector klabels.Selector
}

type childRU struct {
	ru       Update
	unlocker consulutil.Unlocker
	quit     chan<- struct{}
}

func NewFarm(
	factory Factory,
	storage Store,
	rls rollstore.Store,
	rcs RCGetter,
	sessions <-chan string,
	logger logging.Logger,
	labeler rc.Labeler,
	rcSelector klabels.Selector,
	alerter alerting.Alerter,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}
	return &Farm{
		factory:    factory,
		store:      storage,
		rls:        rls,
		rcs:        rcs,
		sessions:   sessions,
		logger:     logger,
		children:   make(map[store.RollingUpdateID]childRU),
		labeler:    labeler,
		rcSelector: rcSelector,
		alerter:    alerter,
	}
}

// Start is a blocking function that monitors Consul for updates. The Farm will
// attempt to claim updates as they appear and, if successful, will start
// goroutines for those updatesto do their job. Closing the quit channel will
// cause this function to return, releasing all locks it holds.
//
// Start is not safe for concurrent execution. Do not execute multiple
// concurrent instances of Start.
func (rlf *Farm) Start(quit <-chan struct{}) {
	consulutil.WithSession(quit, rlf.sessions, func(sessionQuit <-chan struct{}, session string) {
		rlf.logger.WithField("session", session).Infoln("Acquired new session")
		rlf.session = rlf.store.NewUnmanagedSession(session, "")
		rlf.mainLoop(sessionQuit)
	})
}

func (rlf *Farm) mainLoop(quit <-chan struct{}) {
	subQuit := make(chan struct{})
	defer close(subQuit)
	rlWatch, rlErr := rlf.rls.Watch(subQuit)

START_LOOP:
	for {
		select {
		case <-quit:
			rlf.logger.NoFields().Infoln("Session expired, releasing updates")
			rlf.session = nil
			rlf.releaseChildren()
			return
		case err := <-rlErr:
			rlf.logger.WithError(err).Errorln("Could not read consul updates")
		case rlFields := <-rlWatch:
			rlf.logger.WithField("n", len(rlFields)).Debugln("Received update update")
			countHistogram := metrics.GetOrRegisterHistogram("ru_count", p2metrics.Registry, metrics.NewExpDecaySample(1028, 0.015))
			countHistogram.Update(int64(len(rlFields)))

			// track which children were found in the returned set
			foundChildren := make(map[store.RollingUpdateID]struct{})
			for _, rlField := range rlFields {

				rlLogger := rlf.logger.SubLogger(logrus.Fields{
					"ru": rlField.ID(),
				})
				rcField, err := rlf.rcs.Get(rlField.NewRC)
				if rcstore.IsNotExist(err) {
					err := util.Errorf("Expected RC %s to exist", rlField.NewRC)
					rlLogger.WithError(err).Errorln()
					continue
				} else if err != nil {
					rlLogger.WithError(err).Errorln("Could not read new RC")
					continue
				}

				rlLogger = rlLogger.SubLogger(logrus.Fields{
					"pod": rcField.Manifest.ID(),
				})
				if _, ok := rlf.children[rlField.ID()]; ok {
					// this one is already ours, skip
					rlLogger.NoFields().Debugln("Got update already owned by self")
					foundChildren[rlField.ID()] = struct{}{}
					continue
				}

				shouldWorkOnOld, err := rlf.shouldWorkOn(rlField.OldRC)
				if err != nil {
					rlLogger.WithError(err).Errorf("Could not determine if should work on RC %s, skipping", rlField.OldRC)
					continue
				}
				if !shouldWorkOnOld {
					rlLogger.WithField("old_rc", rlField.OldRC).Infof("Ignoring roll for old RC %s, not meant for this farm", rlField.OldRC)
					continue
				}

				shouldWorkOnNew, err := rlf.shouldWorkOn(rlField.NewRC)
				if err != nil {
					rlLogger.WithError(err).Errorf("Could not determine if should work on RC %s, skipping", rlField.ID())
					continue
				}
				if !shouldWorkOnNew {
					rlLogger.WithField("new_rc", rlField.ID()).Infof("Ignoring roll for new RC %s, not meant for this farm", rlField.ID())
					continue
				}

				lockPath, err := rollstore.RollLockPath(rlField.ID())
				if err != nil {
					rlLogger.WithError(err).Errorln("Unable to compute roll lock path")
				}

				unlocker, err := rlf.session.Lock(lockPath)
				if _, ok := err.(consulutil.AlreadyLockedError); ok {
					// someone else must have gotten it first - log and move to
					// the next one
					rlLogger.NoFields().Debugln("Lock on update was denied")
					continue
				} else if err != nil {
					rlLogger.WithError(err).Errorln("Got error while locking update - session may be expired")
					// stop processing this update and go back to the select
					// chances are this error is a network problem or session
					// expiry, and all the others in this update would also fail
					continue START_LOOP
				}

				// at this point the ru is ours, time to spin it up
				rlLogger.WithField("new_rc", rlField.ID()).Infof("Acquired lock on update %s -> %s, spawning", rlField.OldRC, rlField.ID())

				newChild := rlf.factory.New(rlField, rlLogger, rlf.session, rlf.alerter)
				childQuit := make(chan struct{})
				rlf.children[rlField.ID()] = childRU{
					ru:       newChild,
					quit:     childQuit,
					unlocker: unlocker,
				}
				foundChildren[rlField.ID()] = struct{}{}

				err = rlf.validateRoll(rlField, rlLogger)
				if err != nil {
					rlLogger.WithError(err).Errorln("RU was invalid, deleting")

					// Just delete the RU, the farm will clean up the lock when releaseDeletedChildren() is called
					rlf.mustDeleteRU(rlField.ID(), rlLogger)
					continue
				}

				newRC := rlField.NewRC
				go func(id store.RollingUpdateID) {
					defer func() {
						if r := recover(); r != nil {
							err := util.Errorf("Caught panic in roll farm: %s", r)
							rlLogger.WithError(err).
								WithField("new_rc", newRC).
								Errorln("Caught panic in roll farm")

							// Release the child so that another farm can reattempt
							rlf.childMu.Lock()
							defer rlf.childMu.Unlock()
							if _, ok := rlf.children[id]; ok {
								rlf.releaseChild(id)
							}
						}
					}()
					if !newChild.Run(childQuit) {
						// returned false, farm must have asked us to quit
						return
					}

					// Block until the RU is deleted because the farm does not release locks until it detects an RU deletion
					// our lock on this RU won't be released until it's deleted
					rlf.mustDeleteRU(id, rlLogger)
				}(rlField.ID()) // do not close over rlField, it's a loop variable
			}

			// now remove any children that were not found in the result set
			rlf.releaseDeletedChildren(foundChildren)
		}
	}
}

func (rlf *Farm) releaseDeletedChildren(foundChildren map[store.RollingUpdateID]struct{}) {
	rlf.childMu.Lock()
	defer rlf.childMu.Unlock()
	rlf.logger.NoFields().Debugln("Pruning updates that have disappeared")
	for id := range rlf.children {
		if _, ok := foundChildren[id]; !ok {
			rlf.releaseChild(id)
		}
	}
}

// test if the farm should work on the given replication controller ID
func (rlf *Farm) shouldWorkOn(rcID store.ReplicationControllerID) (bool, error) {
	if rlf.rcSelector.Empty() {
		return true, nil
	}
	labels, err := rlf.labeler.GetLabels(labels.RC, rcID.String())
	if err != nil {
		return false, err
	}
	return rlf.rcSelector.Matches(labels.Labels), nil
}

// close one child
// should only be called with rlf.childMu locked
func (rlf *Farm) releaseChild(id store.RollingUpdateID) {
	rlf.logger.WithField("ru", id).Infoln("Releasing update")
	close(rlf.children[id].quit)

	// if our lock is active, attempt to gracefully release it
	if rlf.session != nil {
		unlocker := rlf.children[id].unlocker
		err := unlocker.Unlock()
		if err != nil {
			rlf.logger.WithField("ru", id).Warnln("Could not release update lock")
		}
	}
	delete(rlf.children, id)
}

// close all children
func (rlf *Farm) releaseChildren() {
	rlf.childMu.Lock()
	defer rlf.childMu.Unlock()
	for id := range rlf.children {
		// it's safe to delete this element during iteration,
		// because we have already iterated over it
		rlf.releaseChild(id)
	}
}

// Validates that the rolling update is capable of being processed. If not, an
// error is returned.
// The following conditions make an RU invalid:
// 1) New RC does not exist
// 2) Old RC does not exist
func (rlf *Farm) validateRoll(update store.RollingUpdate, logger logging.Logger) error {
	_, err := rlf.rcs.Get(update.NewRC)
	if err == rcstore.NoReplicationController {
		return fmt.Errorf("RU '%s' is invalid, new RC '%s' did not exist", update.ID(), update.NewRC)
	} else if err != nil {
		// There was a potentially transient consul error, we don't necessarily want to delete the RU
		logger.WithError(err).Errorln("Could not fetch new RC to validate RU, assuming it's valid")
	}

	_, err = rlf.rcs.Get(update.OldRC)
	if err == rcstore.NoReplicationController {
		return fmt.Errorf("RU '%s' is invalid, old RC '%s' did not exist", update.ID(), update.OldRC)
	} else if err != nil {
		// There was a potentially transient consul error, we don't necessarily want to delete the RU
		logger.WithError(err).Errorln("Could not fetch old RC in order to validate RU, assuming it's valid")
	}

	return nil
}

// Tries to delete the given RU every second until it succeeds
func (rlf *Farm) mustDeleteRU(id store.RollingUpdateID, logger logging.Logger) {
	for err := rlf.rls.Delete(id); err != nil; err = rlf.rls.Delete(id) {
		logger.WithError(err).Errorln("Could not delete update")
		time.Sleep(1 * time.Second)
	}
}
