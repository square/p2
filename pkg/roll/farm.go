package roll

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/error_reporter"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/kp/rollstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc/fields"
	roll_fields "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"
)

type Factory interface {
	New(roll_fields.Update, logging.Logger, kp.Session, alerting.Alerter, error_reporter.Reporter) Update
}

type UpdateFactory struct {
	KPStore       kp.Store
	RCStore       rcstore.Store
	HealthChecker checker.ConsulHealthChecker
	Labeler       labels.Applicator
	Scheduler     scheduler.Scheduler
}

func (f UpdateFactory) New(u roll_fields.Update, l logging.Logger, session kp.Session, alerter alerting.Alerter, errorReporter error_reporter.Reporter) Update {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	if errorReporter == nil {
		errorReporter = error_reporter.NewNop()
	}

	return NewUpdate(u, f.KPStore, f.RCStore, f.HealthChecker, f.Labeler, f.Scheduler, l, session, alerter, errorReporter)
}

type RCGetter interface {
	Get(id fields.ID) (fields.RC, error)
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
	kps      kp.Store
	rls      rollstore.Store
	rcs      RCGetter
	sessions <-chan string

	children map[roll_fields.ID]childRU
	childMu  sync.Mutex
	session  kp.Session

	logger        logging.Logger
	alerter       alerting.Alerter
	errorReporter error_reporter.Reporter

	labeler    labels.Applicator
	rcSelector klabels.Selector
}

type childRU struct {
	ru       Update
	unlocker consulutil.Unlocker
	quit     chan<- struct{}
}

func NewFarm(
	factory Factory,
	kps kp.Store,
	rls rollstore.Store,
	rcs RCGetter,
	sessions <-chan string,
	logger logging.Logger,
	labeler labels.Applicator,
	rcSelector klabels.Selector,
	alerter alerting.Alerter,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}
	return &Farm{
		factory:    factory,
		kps:        kps,
		rls:        rls,
		rcs:        rcs,
		sessions:   sessions,
		logger:     logger,
		children:   make(map[roll_fields.ID]childRU),
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
		rlf.session = rlf.kps.NewUnmanagedSession(session, "")
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

			// track which children were found in the returned set
			foundChildren := make(map[roll_fields.ID]struct{})
			for _, rlField := range rlFields {

				rlLogger := rlf.logger.SubLogger(logrus.Fields{
					"ru": rlField.ID(),
				})
				rcField, err := rlf.rcs.Get(rlField.NewRC)
				if rcstore.IsNotExist(err) {
					err := util.Errorf("Expected RC %s to exist", rlField.NewRC)
					rlLogger.WithError(err).Errorln()
					rlf.errorReporter.Report(err, nil, 1)
					continue
				} else if err != nil {
					rlLogger.WithError(err).Errorln("Could not read new RC")
					rlf.errorReporter.Report(err, nil, 1)
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
					rlf.errorReporter.Report(err, nil, 1)
					continue
				}
				if !shouldWorkOnOld {
					rlLogger.WithField("old_rc", rlField.OldRC).Infof("Ignoring roll for old RC %s, not meant for this farm", rlField.OldRC)
					continue
				}

				shouldWorkOnNew, err := rlf.shouldWorkOn(rlField.NewRC)
				if err != nil {
					rlLogger.WithError(err).Errorf("Could not determine if should work on RC %s, skipping", rlField.ID())
					rlf.errorReporter.Report(err, nil, 1)
					continue
				}
				if !shouldWorkOnNew {
					rlLogger.WithField("new_rc", rlField.ID()).Infof("Ignoring roll for new RC %s, not meant for this farm", rlField.ID())
					continue
				}

				lockPath, err := rollstore.RollLockPath(rlField.ID())
				if err != nil {
					rlLogger.WithError(err).Errorln("Unable to compute roll lock path")
					rlf.errorReporter.Report(err, nil, 1)
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

				newChild := rlf.factory.New(rlField, rlLogger, rlf.session, rlf.alerter, rlf.errorReporter)
				childQuit := make(chan struct{})
				rlf.children[rlField.ID()] = childRU{
					ru:       newChild,
					quit:     childQuit,
					unlocker: unlocker,
				}
				foundChildren[rlField.ID()] = struct{}{}

				go func(id roll_fields.ID) {
					defer func() {
						if r := recover(); r != nil {
							err := util.Errorf("Caught panic in roll farm: %s", r)
							rlLogger.WithError(err).
								WithField("new_rc", rlField.NewRC).
								Errorln("Caught panic in roll farm")
							rlf.errorReporter.Report(err, nil, 1)

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
					// our lock on this RU won't be released until it's deleted,
					// so if we fail to delete it, we have to retry
					for err := rlf.rls.Delete(id); err != nil; err = rlf.rls.Delete(id) {
						rlLogger.WithError(err).Errorln("Could not delete update")
						rlf.errorReporter.Report(err, nil, 1)
						time.Sleep(1 * time.Second)
					}
				}(rlField.ID()) // do not close over rlField, it's a loop variable
			}

			// now remove any children that were not found in the result set
			rlf.releaseDeletedChildren(foundChildren)
		}
	}
}

func (rlf *Farm) releaseDeletedChildren(foundChildren map[roll_fields.ID]struct{}) {
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
func (rlf *Farm) shouldWorkOn(rcID fields.ID) (bool, error) {
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
func (rlf *Farm) releaseChild(id roll_fields.ID) {
	rlf.logger.WithField("ru", id).Infoln("Releasing update")
	close(rlf.children[id].quit)

	// if our lock is active, attempt to gracefully release it
	if rlf.session != nil {
		unlocker := rlf.children[id].unlocker
		err := unlocker.Unlock()
		if err != nil {
			rlf.logger.WithField("ru", id).Warnln("Could not release update lock")
			rlf.errorReporter.Report(err, nil, 1)
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
