package roll

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	p2metrics "github.com/square/p2/pkg/metrics"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	roll_fields "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/rollstore"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"github.com/rcrowley/go-metrics"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type Factory interface {
	New(roll_fields.Update, logging.Logger, consul.Session) Update
}

type UpdateFactory struct {
	Store         Store
	RCLocker      ReplicationControllerLocker
	RCStore       ReplicationControllerStore
	HealthChecker checker.ConsulHealthChecker
	Labeler       labeler
	WatchDelay    time.Duration
	Alerter       alerting.Alerter
}

type labeler interface {
	rc.LabelMatcher
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
}

func NewUpdateFactory(
	store Store,
	rcLocker ReplicationControllerLocker,
	rcStore ReplicationControllerStore,
	healthChecker checker.ConsulHealthChecker,
	labeler labeler,
	watchDelay time.Duration,
	alerter alerting.Alerter,
) UpdateFactory {
	return UpdateFactory{
		Store:         store,
		RCLocker:      rcLocker,
		RCStore:       rcStore,
		HealthChecker: healthChecker,
		Labeler:       labeler,
		WatchDelay:    watchDelay,
		Alerter:       alerter,
	}
}

func (f UpdateFactory) New(u roll_fields.Update, l logging.Logger, session consul.Session) Update {
	return NewUpdate(
		u,
		f.Store,
		f.RCLocker,
		f.RCStore,
		f.HealthChecker,
		f.Labeler,
		l,
		session,
		f.WatchDelay,
		f.Alerter,
	)
}

type RCGetter interface {
	Get(id fields.ID) (fields.RC, error)
}

type RollingUpdateStore interface {
	Watch(quit <-chan struct{}) (<-chan []roll_fields.Update, <-chan error)
	Delete(ctx context.Context, id roll_fields.ID) error
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
	rls      RollingUpdateStore
	rcs      RCGetter
	sessions <-chan string

	children map[roll_fields.ID]childRU
	childMu  sync.Mutex
	session  consul.Session

	logger logging.Logger

	labeler       labeler
	rcSelector    klabels.Selector
	txner         transaction.Txner
	auditLogStore auditlogstore.ConsulStore
	config        FarmConfig
	alerter       alerting.Alerter
}

type childRU struct {
	ru       Update
	unlocker consul.Unlocker
	quit     chan<- struct{}
}

// FarmConfig contains configuration options for the farm. All fields have safe
// defaults
type FarmConfig struct {
	// ShouldCreateAuditLogRecords determines whether the farm will create audit
	// log records when deleting a rolling update, which occurs after completing
	// the RU
	ShouldCreateAuditLogRecords bool
}

func NewFarm(
	factory Factory,
	store Store,
	rls RollingUpdateStore,
	rcs RCGetter,
	sessions <-chan string,
	logger logging.Logger,
	labeler rc.Labeler,
	rcSelector klabels.Selector,
	txner transaction.Txner,
	config FarmConfig,
	alerter alerting.Alerter,
) *Farm {
	return &Farm{
		factory:    factory,
		store:      store,
		rls:        rls,
		rcs:        rcs,
		sessions:   sessions,
		logger:     logger,
		children:   make(map[roll_fields.ID]childRU),
		labeler:    labeler,
		rcSelector: rcSelector,
		txner:      txner,
		config:     config,
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
			foundChildren := make(map[roll_fields.ID]struct{})
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
				if _, ok := err.(consul.AlreadyLockedError); ok {
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

				newChild := rlf.factory.New(rlField, rlLogger, rlf.session)
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
				go func(id roll_fields.ID) {
					defer func() {
						if r := recover(); r != nil {
							err := util.Errorf("Caught panic in roll farm: %s", r)

							stackErr, ok := err.(util.StackError)
							msg := "Caught panic in roll farm"
							if ok {
								msg = fmt.Sprintf("%s:\n%s", msg, stackErr.Stack())
							}

							rlLogger.WithError(err).
								WithField("new_rc", newRC).
								Errorln(msg)

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
func (rlf *Farm) validateRoll(update roll_fields.Update, logger logging.Logger) error {
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
func (rlf *Farm) mustDeleteRU(id roll_fields.ID, logger logging.Logger) {
	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err := rlf.rls.Delete(ctx, id)
	if err != nil {
		// this error is really bad because we can't recover from it
		logger.WithError(err).Errorln("could not construct transaction to delete RU")
		return
	}

	if rlf.config.ShouldCreateAuditLogRecords {
		details, err := audit.NewRUCompletionEventDetails(id, true, false, rlf.labeler)
		if err != nil {
			logger.WithError(err).Errorln("could not create RU completion audit log record")
			// this error won't be recoverable so continue with deleting
			// the RU without making an audit record
		} else {
			err = rlf.auditLogStore.Create(ctx, audit.RUCompletionEvent, details)
			if err != nil {
				logger.WithError(err).Errorln("could not add audit log record creation operation to transaction")
				// this error won't be recoverable so continue with deleting
				// the RU without making an audit record
			}
		}
	}

	var ok bool
	var resp *api.KVTxnResponse
	f := func() error {
		ok, resp, err = transaction.Commit(ctx, rlf.txner)
		return err
	}

	for err = f(); err != nil; err = f() {
		logger.WithError(err).Errorln("Could not delete update")
		time.Sleep(1 * time.Second)
	}

	if ok {
		return
	}

	// the transaction was rolled back which shouldn't happen because it
	// doesn't contain any conditional operations, just deleting the RU and
	// creating an audit log record. something really weird is going on if
	// we get here so send an alert for manual intervention
	f2 := func() error {
		return rlf.alerter.Alert(alerting.AlertInfo{
			Description: "could not commit RU deletion transaction",
			IncidentKey: "roll-deletion-" + id.String(),
			Details: struct {
				Errors api.TxnErrors `json:"error"`
			}{
				Errors: resp.Errors,
			},
		})
	}

	for err = f2(); err != nil; err = f2() {
		logger.WithError(err).Errorln("Could not send alert about RU deletion failure")
		time.Sleep(1 * time.Second)
	}
}
