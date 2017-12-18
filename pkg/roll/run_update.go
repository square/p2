package roll

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/health"
	hclient "github.com/square/p2/pkg/health/client"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc"
	rcf "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
)

type Store interface {
	SetPod(podPrefix consul.PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
	Pod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (manifest.Manifest, time.Duration, error)
	DeletePod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (time.Duration, error)
	NewUnmanagedSession(session, name string) consul.Session
}

type ReplicationControllerLocker interface {
	LockForMutationTxn(
		lockCtx context.Context,
		rcID rcf.ID,
		session consul.Session,
	) (consul.TxnUnlocker, error)
}

type ReplicationControllerStore interface {
	Get(id rcf.ID) (rcf.RC, error)
	SetDesiredReplicas(id rcf.ID, n int) error
	Delete(id rcf.ID, force bool) error
	DeleteTxn(ctx context.Context, id rcf.ID, force bool) error
	TransferReplicaCounts(ctx context.Context, req rcstore.TransferReplicaCountsRequest) error
	DisableTxn(ctx context.Context, id rcf.ID) error
	EnableTxn(ctx context.Context, id rcf.ID) error
}

type Labeler interface {
	rc.LabelMatcher
	audit.Labeler
}

type ServiceWatcher interface {
	WatchService(
		serviceID string,
		resultCh chan<- map[types.NodeName]health.Result,
		errCh chan<- error,
		quitCh <-chan struct{},
		watchDelay time.Duration,
	)
	Service(serviceID string) (map[types.NodeName]health.Result, error)
}

type update struct {
	fields.Update

	consuls       Store
	consulClient  consulutil.ConsulClient
	rcStore       ReplicationControllerStore
	rcStatusStore RCStatusStore
	rollStore     RollingUpdateStore
	rcLocker      ReplicationControllerLocker
	hcheck        ServiceWatcher
	hclient       hclient.HealthServiceClient
	labeler       Labeler
	txner         transaction.Txner

	logger logging.Logger

	// watchDelay can be used to tune the QPS (and therefore bandwidth)
	// footprint of the health watches performed by the update. A higher
	// value will slow the responsiveness of the update to health changes
	// but will reduce the QPS on the datastore
	watchDelay time.Duration

	// alerter allows the roll farm to page human operators if an
	// unrecoverable problem occurs
	alerter alerting.Alerter

	// scheduler is used to determine which nodes are eligible for a given RC
	// which factors into some of the computations that are performed during
	// a rolling update
	scheduler RCScheduler

	// If set, will create an audit log record when a rolling upate is completed (and thus deleted)
	// to signify that the rolling update was successful
	shouldCreateAuditLogRecords bool
	auditLogStore               auditlogstore.ConsulStore
}

type RCStatusStore interface {
	Get(rcID rcf.ID) (rcstatus.Status, *api.QueryMeta, error)
}

// Create a new Update. The consul.Store, rcstore.Store, labels.Applicator and
// scheduler.Scheduler arguments should be the same as those of the RCs themselves. The
// session must be valid for the lifetime of the Update; maintaining this is the
// responsibility of the caller.
func NewUpdate(
	f fields.Update,
	consuls Store,
	consulClient consulutil.ConsulClient,
	rcLocker ReplicationControllerLocker,
	rcStore ReplicationControllerStore,
	rcStatusStore RCStatusStore,
	rollStore RollingUpdateStore,
	txner transaction.Txner,
	hcheck ServiceWatcher,
	hclient hclient.HealthServiceClient,
	labeler Labeler,
	logger logging.Logger,
	session consul.Session,
	watchDelay time.Duration,
	alerter alerting.Alerter,
	scheduler RCScheduler,
	shouldCreateAuditLogRecords bool,
	auditLogStore auditlogstore.ConsulStore,
) Update {
	logger = logger.SubLogger(logrus.Fields{
		"desired_replicas": f.DesiredReplicas,
		"minimum_replicas": f.MinimumReplicas,
	})
	return &update{
		Update:        f,
		consuls:       consuls,
		rcLocker:      rcLocker,
		rcStore:       rcStore,
		rcStatusStore: rcStatusStore,
		rollStore:     rollStore,
		txner:         txner,
		hcheck:        hcheck,
		hclient:       hclient,
		labeler:       labeler,
		logger:        logger,
		watchDelay:    watchDelay,
		alerter:       alerter,
		consulClient:  consulClient,
		scheduler:     scheduler,
		auditLogStore: auditLogStore,
	}
}

type Update interface {
	// Run will execute the Update and modify the passed context such that
	// its transaction removes the old RC upon completion. Run should claim
	// exclusive ownership of both affected RCs, and release that
	// exclusivity upon completion. Run is long-lived and blocking;
	// canceling the context causes it to terminate it early. If an Update
	// is interrupted or encounters an unrecoverable error such as a
	// transaction violation, Run should leave the RCs in a state such that
	// it can later be called again to resume. The return value indicates
	// if the update completed (true) or if it was terminated early
	// (false).
	Run(ctx context.Context) bool
}

// returned by shouldStop
type ruStep int

const (
	ruShouldTerminate ruStep = iota
	ruShouldBlock
	ruShouldContinue
)

// retries a given function until it returns a nil error or the quit channel is
// closed. returns true if it exited in the former case, false in the latter.
// errors are sent to the given logger with the given string as the message.
func RetryOrQuit(ctx context.Context, f func() error, logger logging.Logger, errtext string) bool {
	for err := f(); err != nil; err = f() {
		logger.WithError(err).Errorln(errtext)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Second):
			// unblock the select and loop again
		}
	}
	return true
}

// Run causes the update to be processed either until it is complete or it is
// cancelled via the passed quit channel. The passed context is expected to
// have a consul transaction value stored in it and cleanup operations such as
// deleting the old RC will be added to it when applicable.
func (u *update) Run(ctx context.Context) (ret bool) {
	u.logger.Infoln("creating a session for this RU")
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "(unknown)"
	}
	sessionRenewalCtx, sessionRenewalCancel := context.WithCancel(ctx)
	defer sessionRenewalCancel()

	sessionCtx, session, err := consul.SessionContext(sessionRenewalCtx, u.consulClient, fmt.Sprintf("ru-farm:%s:%s", hostname, u.ID()))
	if err != nil {
		u.logger.WithError(err).Errorln("could not create session")
		return false
	}

	// create a transaction to lock the RCs with. This way we can't lock
	// one and not the other
	lockRCsCtx, cancelLockRCs := transaction.New(sessionCtx)

	// create a transaction to check that the RCs are locked. We're going
	// to branch a lot of transactions off of this one to guarantee they
	// only succeed if the locks are held
	checkRCLocksCtx, cancelCheckRCLocksCtx := transaction.New(sessionCtx)
	defer cancelCheckRCLocksCtx()

	// create a transaction for cleanup operations such as releasing locks
	// and (if the update succeeds) deleting the RU and the old RC (when
	// applicable).
	cleanupCtx, cancelCleanup := transaction.New(sessionCtx)
	performCleanup := func() {
		defer cancelCleanup()
		ok, resp, err := transaction.CommitWithRetries(cleanupCtx, u.txner)
		if err != nil {
			// If we get here, it means the context was cancelled which
			// probably means our consul session (and thus RC locks)
			// disappeared.

			// log the error, but this is otherwise okay because
			// another farm will pick up the RU, notice that it's
			// finished, and try to perform cleanup (e.g. deleting
			// the old RC)
			u.logger.WithError(err).Errorln("could not perform RU cleanup because transaction did not succeed before cancellation")

			// set ret to false so that the farm does not delete the RU, giving another farm a chance to handle this cleanup
			ret = false
			return
		}
		if !ok {
			// This probably means that the transaction was submitted to
			// the server just before context cancellation, but we didn't
			// have the RC locks (due to session dying). Log the error
			// but otherwise another farm should pick up this RU and
			// perform the necessary cleanup
			err := util.Errorf("transaction errors: %s", transaction.TxnErrorsToString(resp.Errors))
			u.logger.WithError(err).Errorln("could not perform RU cleanup due to transaction conflict")
			ret = false
			return
		}

		// leave ret set however it was before this function was called
		return
	}
	defer performCleanup()

	// Pass ctx as the "unlock RCs" transaction. That way when the caller of Run() commits
	// the transaction it will release the locks.
	// TODO: we probably don't want to add unlocking operations to
	// cleanupCtx until the Commit() succeeds, explore making a
	// transaction.Merge() function and only performing the merge after the
	// commit is successful
	err = u.lockRCs(lockRCsCtx, cleanupCtx, checkRCLocksCtx, session)
	if err != nil {
		cancelLockRCs()
		// this is a pageable error because it's only adding operations to
		// transactions in memory. If it fails once it's never going to
		// succeed and probably requires a code rollback
		u.mustAlert(
			ctx,
			"could not build RC locking transaction",
			"build-lock-"+u.ID().String(),
			err,
		)
		return false
	}

	ok, resp, err := transaction.CommitWithRetries(lockRCsCtx, u.txner)
	if err != nil {
		// this will only happen if the context was canceled, so just stop processing the roll
		return false
	}
	if !ok {
		// another farm must have the locks, stop processng the roll
		u.logger.Infof("could not lock RCs (transaction response %s). Exiting roll loop", transaction.TxnErrorsToString(resp.Errors))
		return false
	}
	cancelLockRCs()

	u.logger.NoFields().Debugln("Enabling")
	err = u.enable(checkRCLocksCtx)
	if err != nil {
		u.logger.WithError(err).Errorln("could not enable RCs")
		return false
	}

	u.logger.NoFields().Debugln("Launching health watch")
	var newFields rcf.RC
	if !RetryOrQuit(
		sessionCtx,
		func() error {
			newFields, err = u.rcStore.Get(u.NewRC)
			if rcstore.IsNotExist(err) {
				return util.Errorf("Replication controller %s is unexpectedly empty", u.NewRC)
			} else if err != nil {
				return err
			}

			return nil
		}, u.logger, "Could not read new RC") {
		return
	}

	hChecks := make(chan map[types.NodeName]health.Result)
	hErrs := make(chan error)
	hQuit := make(chan struct{})
	defer close(hQuit)
	watchDelay := 1 * time.Second
	go u.hcheck.WatchService(string(newFields.Manifest.ID()), hChecks, hErrs, hQuit, watchDelay)

	if updateSucceeded := u.rollLoop(checkRCLocksCtx, newFields.Manifest.ID(), hChecks, hErrs); !updateSucceeded {
		// We were asked to quit. Do so without cleaning old RC.
		return false
	}

	// rollout complete, clean up old RC if told to do so
	if !u.LeaveOld {
		u.cleanupOldRC(cleanupCtx)
	}

	err = u.rollStore.Delete(cleanupCtx, u.ID())
	if err != nil {
		// this error is really bad because we can't recover from it
		u.logger.WithError(err).Errorln("could not construct transaction to delete RU")
		u.mustAlert(
			context.Background(),
			"could not build RU deletion transaction",
			"ru-deletion-txn"+u.ID().String(),
			err,
		)
		return false
	}

	if u.shouldCreateAuditLogRecords {
		succeeded := true
		canceled := false
		details, err := audit.NewRUCompletionEventDetails(u.ID(), succeeded, canceled, u.labeler)
		if err != nil {
			u.logger.WithError(err).Errorln("could not create RU completion audit log record")
			u.mustAlert(
				context.Background(),
				"could not build RU deletion transaction due to audit log operation",
				"ru-deletion-txn"+u.ID().String(),
				err,
			)
		}

		err = u.auditLogStore.Create(cleanupCtx, audit.RUCompletionEvent, details)
		if err != nil {
			u.logger.WithError(err).Errorln("could not add audit log operation to transaction")
			u.mustAlert(
				context.Background(),
				"could not build RU deletion transaction due to audit log operation",
				"ru-deletion-txn"+u.ID().String(),
				err,
			)
			return false
		}
	}

	// return true here, but note that it might become false because of the
	// deferred performCleanup() function
	return true
}

func (u *update) cleanupOldRC(ctx context.Context) {
	oldRCZeroed := false

	cleanupFunc := func() error {
		if !oldRCZeroed {
			oldRC, err := u.rcStore.Get(u.OldRC)
			if err != nil {
				return err
			}

			if oldRC.ReplicasDesired != 0 {
				// This likely means there was a mathematical error
				// of some kind that was made when the RU was
				// scheduled. If LeaveOld is false, we expect that
				// all nodes will have been rolled from the old RC to
				// the new one at this point, so we can delete the
				// old RC after it isn't managing any nodes anymore.
				// This error likely needs manual fixing by shifting
				// the remaining nodes off of this RC and then
				// deleting it
				u.logger.Errorln("could not delete old RC because its replica count is nonzero")
				err = u.alerter.Alert(alerting.AlertInfo{
					Description: "old RC did not have 0 replicas and could not be deleted.",
					IncidentKey: "roll-" + u.ID().String(),
					Details: struct {
						OldRCID     string `json:"old_rc_id"`
						RUID        string `json:"ru_id"`
						NumReplicas int    `json:"num_replicas"`
					}{
						OldRCID:     u.OldRC.String(),
						RUID:        u.ID().String(),
						NumReplicas: oldRC.ReplicasDesired,
					},
				}, alerting.LowUrgency)
				if err != nil {
					return err
				}

				// return nil to avoid looping, the alert
				// should cause the issue to be fixed by a
				// human operator
				return nil
			}
			oldRCZeroed = true
		}

		currentPods, err := rc.CurrentPods(u.OldRC, u.labeler)
		if err != nil {
			return err
		}

		if len(currentPods) != 0 {
			u.logger.Warnf("old RC still has %d current pods, waiting to delete", len(currentPods))
			return util.Errorf("waiting for old RC to have 0 current pods before deleting")
		}

		return nil
	}

	u.logger.NoFields().Infoln("Cleaning up old RC")
	if !RetryOrQuit(ctx, cleanupFunc, u.logger, "Could not delete old RC") {
		return
	}

	if !oldRCZeroed {
		u.logger.Infoln("Not cleaning up old RC because it still has some replicas desired")
		return
	}

	err := u.rcStore.DeleteTxn(ctx, u.OldRC, false)
	if err != nil {
		alertContext, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			select {
			case <-alertContext.Done():
				// this means the alert was sent successfully, so exit the goroutine
				return
			case <-ctx.Done():
				// this means we've been asked to shut down, so cancel the alert send
				cancel()
				return
			}
		}()
		// this error is really bad because we can't recover from it
		u.logger.WithError(err).Errorln("could not construct transaction to delete RU")
		u.mustAlert(
			alertContext,
			"could not build RC deletion transaction",
			"rc-deletion-txn"+u.ID().String(),
			err,
		)
	}
}

// returns true if roll succeeded, false if asked to quit.
func (u *update) rollLoop(ctx context.Context, podID types.PodID, hChecks <-chan map[types.NodeName]health.Result, hErrs <-chan error) bool {
	for {
		// Select on just the quit channel before entering the select with both quit and hChecks. This protects against a situation where
		// hChecks and quit are both ready, and hChecks might be chosen due to the random choice semantics of select {}. If multiple
		// iterations continue after quit is closed, a dangerous situation is created because multiple farm instances might end up
		// handling the same RU.
		select {
		case <-ctx.Done():
			return false
		default:
		}

		select {
		case <-ctx.Done():
			return false
		case err := <-hErrs:
			u.logger.WithError(err).Errorln("Could not read health checks")
		case checks := <-hChecks:
			newNodes, err := u.countHealthy(u.NewRC, checks)
			if err != nil {
				u.logger.WithErrorAndFields(err, logrus.Fields{
					"new": newNodes.ToString(),
				}).Errorln("Could not count nodes on new RC")
				break
			}
			oldNodes, err := u.countHealthy(u.OldRC, checks)
			if err != nil {
				u.logger.WithErrorAndFields(err, logrus.Fields{
					"old": oldNodes,
				}).Errorln("Could not count nodes on old RC")
				break
			}

			if nextAction := u.shouldStop(oldNodes, newNodes); nextAction == ruShouldTerminate {
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes.ToString(),
					"new": newNodes.ToString(),
				}).Debugln("Upgrade complete")
				return true
			} else if nextAction == ruShouldBlock {
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes.ToString(),
					"new": newNodes.ToString(),
				}).Debugln("Upgrade almost complete, blocking for more healthy new nodes")
				break
			}

			nextRemove, nextAdd := rollAlgorithm(u.rollAlgorithmParams(oldNodes, newNodes))
			if nextRemove > 0 || nextAdd > 0 {
				// apply the delay only if we've already added to the new RC, since there's
				// no value in sitting around doing nothing before anything has happened.
				if newNodes.Desired > 0 && u.RollDelay > time.Duration(0) {
					u.logger.WithField("delay", u.RollDelay).Infof("Waiting %v before continuing deploy", u.RollDelay)

					select {
					case <-time.After(u.RollDelay):
					case <-ctx.Done():
						return false
					}

					// determine the new value of `next`, which may have changed
					// following the delay.
					nextRemove, nextAdd, err = u.shouldRollAfterDelay(podID)

					if err != nil {
						u.logger.NoFields().Errorln(err)
						break
					}
				}

				u.logger.WithFields(logrus.Fields{
					"old":        oldNodes.ToString(),
					"new":        newNodes.ToString(),
					"nextRemove": nextRemove,
					"nextAdd":    nextAdd,
				}).Infof("Adding %d new nodes and removing %d old nodes", nextAdd, nextRemove)
				transferReq := rcstore.TransferReplicaCountsRequest{
					ToRCID:               u.NewRC,
					FromRCID:             u.OldRC,
					ReplicasToAdd:        &nextAdd,
					ReplicasToRemove:     &nextRemove,
					StartingToReplicas:   &newNodes.Desired,
					StartingFromReplicas: &oldNodes.Desired,
				}

				// branch off of the passed ctx which implicitly ensures that RC locks are held
				transferCtx, cancel := transaction.New(ctx)
				err = u.rcStore.TransferReplicaCounts(transferCtx, transferReq)
				if err != nil {
					// this error is really bad because it means
					// the transaction has exceeded 64
					// operations. only a code change can fix
					// this
					cancel()
					u.logger.WithError(err).Errorln("could not update RC replica counts")

					// the panic will be caught by our recover()
					panic(fmt.Sprintf("could not update RC replica counts: %s", err))
				}

				err := transaction.MustCommit(transferCtx, u.txner)
				if err != nil {
					// This can happen for a few reasons:
					// 1) a CAS violation in the operations added
					// to the context by TransferReplicaCounts().
					// This can be fixed by starting this for
					// loop over
					// 2) a CAS violation due to not holding the RC locks anymore. That should only occur if our session died
					// which should cause our context to be canceled which means we'll exit soon
					// 3) a temporary consul unavailability issue. breaking and starting the loop again should be a natural
					// retry
					u.logger.WithError(err).Errorln("could not update RC replica counts")
					break
				}
				cancel()
			} else {
				u.logger.WithFields(logrus.Fields{
					"old": oldNodes.ToString(),
					"new": newNodes.ToString(),
				}).Debugln("Blocking for more healthy nodes")
			}
		}
	}
}

func (u *update) shouldStop(oldNodes, newNodes rcNodeCounts) ruStep {
	if newNodes.Desired < u.DesiredReplicas {
		// Not enough nodes scheduled on the new side, so deploy should continue.
		return ruShouldContinue
	}

	// Enough nodes are scheduled on the new side.

	if newNodes.Current >= u.DesiredReplicas {
		// We only ask for the new RC to have labeled the desired number of nodes
		// (number of nodes labeled is reflected in newNodes.Current)
		// before declaring an upgrade to be complete.
		// No need to check health for two reasons:
		// 1) So that if a deployer intentionally deploys a known-bad SHA
		// for which no nodes become healthy (specifying minimum == 0),
		// we terminate and don't leave an RU and old RC lying around.
		// 2) rollAlgorithm is responsible for maintaining the min health guarantee,
		// not this code. If we reach this point, rollAlgorithm is finished anyway.
		// The only decision to make is whether to terminate or block.
		return ruShouldTerminate
	}

	// Fewer nodes are owned by the new RC than are desired.
	// We don't need to schedule new nodes, but we can't terminate the deploy yet.
	return ruShouldBlock
}

func (u *update) lockRCs(
	lockCtx context.Context,
	unlockCtx context.Context,
	checkLockedCtx context.Context,
	session consul.Session,
) error {
	newUnlocker, err := u.rcLocker.LockForMutationTxn(lockCtx, u.NewRC, session)
	if err != nil {
		return err
	}

	oldUnlocker, err := u.rcLocker.LockForMutationTxn(lockCtx, u.OldRC, session)
	if err != nil {
		return err
	}

	// Add the operations to unlock both to unlockCtx transaction
	err = newUnlocker.UnlockTxn(unlockCtx)
	if err != nil {
		return err
	}

	err = oldUnlocker.UnlockTxn(unlockCtx)
	if err != nil {
		return err
	}

	// Add the operations to check locks both to checkLockedCtx transaction
	err = newUnlocker.CheckLockedTxn(checkLockedCtx)
	if err != nil {
		return err
	}

	err = oldUnlocker.CheckLockedTxn(checkLockedCtx)
	if err != nil {
		return err
	}

	return nil
}

// enable sets the old & new RCs to a known-good state to start a rolling update:
// the old RC should be disabled and the new RC should be enabled.
func (u *update) enable(checkLocksCtx context.Context) error {
	newRC, err := u.rcStore.Get(u.NewRC)
	if err != nil {
		u.logger.WithError(err).Errorln("could not fetch new RC for enabling")
		return err
	}

	if newRC.Disabled {
		err = u.validateNewRCCounts(newRC)
		if err != nil {
			return err
		}
	}

	ctx, cancel := transaction.New(checkLocksCtx)
	defer cancel()
	// We do this AFTER the convergence check, because we don't want to reach this state:
	// disabled, 1 desired, 2 labeled | disabled, 1 desired, 0 labeled.
	// In this case, neither RC will act, so manual intervention is required.
	err = u.rcStore.DisableTxn(ctx, u.OldRC)
	if err != nil {
		return err
	}

	err = u.rcStore.EnableTxn(ctx, u.NewRC)
	if err != nil {
		return err
	}

	ok, resp, err := transaction.CommitWithRetries(ctx, u.txner)
	if err != nil {
		return err
	}

	if !ok {
		return util.Errorf("could not enable RCs due to transaction failure, likely due to losing RC locks: %s", transaction.TxnErrorsToString(resp.Errors))
	}

	return nil
}

func (u *update) validateNewRCCounts(newRC rcf.RC) error {
	// We must exercise caution before enabling a disabled RC.
	//
	// Consider a deploy:
	// Old RC:                        | New RC:
	// disabled, 2 desired, 2 labeled | enabled, 0 desired, 0 labeled.
	// disabled, 1 desired, 2 labeled | enabled, 1 desired, 0 labeled.
	//
	// Now let's say we interrupt the deploy here,
	// and because of Consul latency the new RC doesn't get to act on its desire.
	// Then, we rollback.
	//
	//  enabled, 1 desired, 2 labeled | disabled, 1 desired, 0 labeled.
	//
	// At this point, the formerly-old RC momentarily unschedules one node, because it has too many.
	// This is undesirable.
	//
	// Solution: Wait until formerly-old RC has only 1 node labeled.
	// TODO: We can explore whether it's safe to just set the RCs to 2 desired if it has 2 RCs labeled,
	// but we would be more comfortable with this if we could ascertain there is no chance of race.
	currentPods, err := rc.CurrentPods(u.NewRC, u.labeler)
	if err != nil {
		return err
	}

	if len(currentPods) == newRC.ReplicasDesired {
		return nil
	}

	// Special case: if current pods == replicaCount + 1, we should check if this is only the case because
	// a node transfer is happening
	if len(currentPods) == newRC.ReplicasDesired+1 {
		rcStatus, _, err := u.rcStatusStore.Get(u.NewRC)
		switch {
		case statusstore.IsNoStatus(err):
			return util.Errorf("RC %s currently has %d replicas but wants %d (and has no node transfer) - waiting until it matches to enable.", u.NewRC, len(currentPods), newRC.ReplicasDesired)
		case err != nil:
			return util.Errorf("could not check RC status: %s", err)
		}

		// If a node transfer is in progress, and the
		// current pods we counted include both the old
		// node and the new node of the node transfer,
		// then the RC effectively has one node less
		// than CurrentPods() shows
		if rcStatus.NodeTransfer == nil {
			// release the RU and let another farm try later
			return util.Errorf("RC %s currently has %d replicas but wants %d (and has no node transfer) - waiting until it matches to enable.", u.NewRC, len(currentPods), newRC.ReplicasDesired)
		}

		oldNodeIncluded := false
		newNodeIncluded := false
		for _, node := range currentPods.Nodes() {
			if node == rcStatus.NodeTransfer.NewNode {
				newNodeIncluded = true
			}
			if node == rcStatus.NodeTransfer.OldNode {
				oldNodeIncluded = true
			}
		}

		if oldNodeIncluded || newNodeIncluded {
			return nil
		}
	}

	return util.Errorf("RC %s currently has %d replicas but wants %d - waiting until it matches to enable.", u.NewRC, len(currentPods), newRC.ReplicasDesired)
}

// rcNodeCounts represents a snapshot of an RC with details about how many pods
// it has under various conditions. Most of the fields are for debug logging
// purposes, but the "Healthy" count is used to determine how many nodes the
// rolling update can proceed with on each iteration
type rcNodeCounts struct {
	Desired    int // the number of nodes the RC wants to be on
	Current    int // the number of nodes the RC has scheduled itself on
	Ineligible int // the number of nodes the RC has scheduled itself on that are no longer eligible. These nodes will not be considered for the "Real" or "Healthy" counts
	Real       int // the number of current and non-ineligible nodes that have finished scheduling
	Healthy    int // the number of real nodes that are healthy
	Unhealthy  int // the number of real nodes that are unhealthy
	Unknown    int // the number of real nodes that are of unknown health
}

func (r rcNodeCounts) ToString() string {
	return fmt.Sprintf("%+v", r)
}

func (u *update) countHealthy(id rcf.ID, checks map[types.NodeName]health.Result) (rcNodeCounts, error) {
	ret := rcNodeCounts{}
	rcFields, err := u.rcStore.Get(id)
	if rcstore.IsNotExist(err) {
		err := util.Errorf("RC %s did not exist", id)
		return ret, err
	} else if err != nil {
		return ret, err
	}

	ret.Desired = rcFields.ReplicasDesired

	currentPods, err := rc.CurrentPods(id, u.labeler)
	if err != nil {
		return ret, err
	}
	ret.Current = len(currentPods)

	if ret.Desired > ret.Current {
		// This implies that the RC hasn't yet scheduled pods that it desires to have.
		// We consider their health to be unknown in this case.
		// Note that the below loop over `range currentPods` may also increase `ret.Unknown`.
		ret.Unknown = ret.Desired - ret.Current
	}

	var eligibleNodes []types.NodeName
	if rcFields.AllocationStrategy == rcf.DynamicStrategy {
		eligibleNodes, err = u.scheduler.EligibleNodes(rcFields.Manifest, rcFields.NodeSelector)
		if err != nil {
			return ret, util.Errorf("could not enumerate eligible nodes for %s: %s", rcFields.ID, err)
		}
	}

	for _, pod := range currentPods {
		if rcFields.AllocationStrategy == rcf.DynamicStrategy {
			// don't count this pod if it's ineligible on a dynamic RC, it will be undergoing a node transfer soon and therefore might be unscheduled
			// beneath the rolling update
			nodeEligible := false
			for _, eligibleNode := range eligibleNodes {
				if eligibleNode == pod.Node {
					nodeEligible = true
					break
				}
			}
			if !nodeEligible {
				ret.Ineligible++
				// Don't count this node toward the Real or Healthy counts, continue on to the next one
				continue
			}
		}

		node := pod.Node
		// TODO: is reality checking an rc-layer concern?
		realManifest, _, err := u.consuls.Pod(consul.REALITY_TREE, node, rcFields.Manifest.ID())
		if err != nil && err != pods.NoCurrentManifest {
			return ret, err
		}

		// if realManifest is nil, we use an empty string for comparison purposes against rc
		// manifest SHA, which works for that purpose.
		var realSHA string
		if realManifest != nil {
			realSHA, _ = realManifest.SHA()
		}
		targetSHA, _ := rcFields.Manifest.SHA()
		if targetSHA == realSHA {
			ret.Real++
		} else {
			// don't check health if the update isn't even done there yet
			continue
		}
		if hres, ok := checks[node]; ok {
			if hres.Status == health.Passing {
				ret.Healthy++
			} else if hres.Status == health.Unknown {
				ret.Unknown++
			} else {
				ret.Unhealthy++
			}
		} else {
			ret.Unknown++
		}
	}
	return ret, err
}

func (u *update) shouldRollAfterDelay(podID types.PodID) (int, int, error) {
	// Check health again following the roll delay. If things have gotten
	// worse since we last looked, or there is an error, we break this iteration.
	checks, err := u.hcheck.Service(podID.String())
	if err != nil {
		return 0, 0, util.Errorf("Could not retrieve health following delay: %v", err)
	}

	afterDelayNew, err := u.countHealthy(u.NewRC, checks)
	if err != nil {
		return 0, 0, util.Errorf("Could not determine new service health: %v", err)
	}

	afterDelayOld, err := u.countHealthy(u.OldRC, checks)
	if err != nil {
		return 0, 0, util.Errorf("Could not determine old service health: %v", err)
	}

	afterDelayRemove, afterDelayAdd := rollAlgorithm(u.rollAlgorithmParams(afterDelayOld, afterDelayNew))

	if afterDelayRemove <= 0 && afterDelayAdd <= 0 {
		return 0, 0, util.Errorf("No nodes can be safely updated after %v roll delay, will wait again", u.RollDelay)
	}

	return afterDelayRemove, afterDelayAdd, nil
}

func (u *update) rollAlgorithmParams(oldHealth, newHealth rcNodeCounts) (oldHealthy, newHealthy, oldDesired, newDesired, targetDesired, minHealthy int) {
	oldHealthy = oldHealth.Healthy
	if oldHealth.Desired < oldHealthy {
		// Because of the non-atomicity of our KV stores,
		// we may run into this situation while decrementing old RC's count:
		// old RC has decremented desire, but not yet removed RC label from the pod.
		// We will see this as {Desired: 1, Healthy: 2}, or similar.
		// We assume that the old RC will eventually cause Healthy to go to 1 here.
		// So, we should assume that oldHealthy is is the lesser of the two.
		// Otherwise, we may over-eagerly remove more nodes from old RC.
		// That would cause a violation of minimum health.
		oldHealthy = oldHealth.Desired
	}
	newHealthy = newHealth.Healthy
	oldDesired = oldHealth.Desired
	newDesired = newHealth.Desired
	targetDesired = u.DesiredReplicas
	minHealthy = u.MinimumReplicas
	return
}

func (u *update) mustAlert(ctx context.Context, description string, incidentKey string, err error) {
	f := func() error {
		return u.alerter.Alert(alerting.AlertInfo{
			Description: description,
			IncidentKey: incidentKey,
			Details: struct {
				Error string `json:"error"`
				RUID  string `json:"ru_id"`
			}{
				Error: err.Error(),
				RUID:  u.ID().String(),
			}}, alerting.LowUrgency)
	}

	if !RetryOrQuit(
		ctx,
		f,
		u.logger,
		"could not send alert",
	) {
		return
	}
}

// the roll algorithm defines how to mutate RCs over time. it takes six args:
// - old: the number of healthy nodes on the old RC
// - new: the number of healthy nodes on the new RC
// - oldDesired: the number of nodes currently desired by the old RC
// - newDesired: the number of nodes currently desired by the new RC
// - targetDesired: the number of nodes desired on the new RC (ie the target) in the final state
// - minHealthy: the number of nodes that must always be up (ie the minimum)
// given these five arguments, rollAlgorithm returns the number of nodes to add
// to the new RC and to delete from the old
//
// Returns two values, both of which are a positive number or zero:
// The first value indicates how many nodes to remove from the old RC.
// The second value indicates how many nodes to add to the old RC.
//
// Under the following circumstances, both return values will be zero:
// - new >= desired (the update is done)
// - old+new <= minHealthy (at or below the minimum, update has to block)
func rollAlgorithm(old, new, oldDesired, newDesired, targetDesired, minHealthy int) (nodesToRemove, nodesToAdd int) {
	// how much "headroom" do we have between the number of nodes that are
	// currently healthy, and the number that must be healthy?
	// if we schedule more than this, we'll go below the minimum
	// Note that this can go negative (if old + new don't satisfy minHealthy).
	headroom := old + new - minHealthy

	capacityIncrease := targetDesired - (oldDesired + newDesired)
	if capacityIncrease > 0 {
		// If we intend to schedule new nodes (nodes not currently managed by either RC),
		// then we increase headroom by the capacity difference.
		// Note that headroom could previously have been negative.
		// This means we will schedule some number between 0 and capacityIncrease.
		// For example, with old = new = 0, minHealthy = 2, capacityIncrease = 3,
		// we'll schedule 0 - 2 + 3 = 1 node.
		// This conservatively respects minimum health,
		// on the assumption that there may be healthy nodes we don't know about.
		// This is particularly useful when migrating from other deployment systems.
		headroom += capacityIncrease
	} else {
		// We're not yet sure what the effects of a capacity decrease in a rolling update will be.
		capacityIncrease = 0
	}

	// how many nodes do we have left to go? we can't schedule more than this
	// or we'll go over the target
	// note that remaining < headroom is possible depending on how many
	// old nodes are still alive
	remaining := targetDesired - newDesired

	// heuristic time:
	if remaining <= 0 {
		// no nodes remaining, noop out
		return 0, 0
	}
	if new >= minHealthy {
		// minimum is satisfied by new nodes, doesn't matter how many old ones
		// we kill. this includes the edge case where minHealthy==0
		return clampToZero(remaining - capacityIncrease), remaining
	}
	if headroom <= 0 {
		// the case of minHealthy==0 was caught above. in this case, minHealthy>0 and
		// headroom is non-positive. this means that we are at, or below, the
		// minimum, and we cannot schedule new nodes because we might take
		// down an old one (which would go below the minimum)
		return 0, 0
	}
	// remaining>0 and headroom>0. each one imposes an upper bound on how
	// many nodes we can add at once, so nodesToAdd is their minimum.
	// nodesToRemove is the same number minus any capacity increase
	// (so the net number of nodes added equals the capacity increase)
	if remaining < headroom {
		nodesToAdd = remaining
	} else {
		nodesToAdd = headroom
	}
	nodesToRemove = clampToZero(nodesToAdd - capacityIncrease)
	return
}

func clampToZero(n int) int {
	if n < 0 {
		return 0
	}
	return n
}
