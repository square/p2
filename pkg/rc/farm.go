package rc

import (
	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc/fields"
)

// The Farm is responsible for spawning and reaping replication controllers
// as they are added to and deleted from Consul. Multiple farms can exist
// simultaneously, but each one must hold a different Consul session. This
// ensures that the farms do not instantiate the same replication controller
// multiple times.
type Farm struct {
	// constructor arguments for rcs created by this farm
	kpStore   kp.Store
	rcStore   rcstore.Store
	scheduler Scheduler
	labeler   labels.Applicator

	// session stream for the rcs locked by this farm
	sessions <-chan string

	children map[fields.ID]childRC
	lock     *kp.Lock

	logger logging.Logger
}

type childRC struct {
	rc   ReplicationController
	quit chan<- struct{}
}

func NewFarm(
	kpStore kp.Store,
	rcs rcstore.Store,
	scheduler Scheduler,
	labeler labels.Applicator,
	sessions <-chan string,
	logger logging.Logger,
) *Farm {
	return &Farm{
		kpStore:   kpStore,
		rcStore:   rcs,
		scheduler: scheduler,
		labeler:   labeler,
		sessions:  sessions,
		logger:    logger,
		children:  make(map[fields.ID]childRC),
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
func (rcm *Farm) Start(quit <-chan struct{}) {
	subQuit := make(chan struct{})
	defer close(subQuit)
	rcWatch, rcErr := rcm.rcStore.WatchNew(subQuit)

START_LOOP:
	for {
		select {
		case <-quit:
			rcm.logger.NoFields().Infoln("Halt requested, releasing replication controllers")
			rcm.releaseChildren()
			return
		case session := <-rcm.sessions:
			if session == "" {
				// our session has expired, we must assume our locked children
				// have all been released and that someone else may have
				// claimed them by now
				rcm.logger.NoFields().Errorln("Session expired, releasing replication controllers")
				rcm.lock = nil
				rcm.releaseChildren()
			} else {
				// a new session has been acquired - only happens after an
				// expiration message, so len(children)==0
				rcm.logger.WithField("session", session).Infoln("Acquired new session")
				lock := rcm.kpStore.NewUnmanagedLock(session, "")
				rcm.lock = &lock
				// TODO: restart the watch so that you get updates right away?
			}
		case err := <-rcErr:
			rcm.logger.WithError(err).Errorln("Could not read consul replication controllers")
		case rcFields := <-rcWatch:
			rcm.logger.WithField("n", len(rcFields)).Debugln("Received replication controller update")
			if rcm.lock == nil {
				// we can't claim new nodes because our session is invalidated.
				// raise an error and ignore this update
				rcm.logger.NoFields().Warnln("Received replication controller update, but do not have session to acquire locks")
				continue
			}

			// track which children were found in the returned set
			foundChildren := make(map[fields.ID]struct{})
			for _, rcField := range rcFields {
				rcLogger := rcm.logger.SubLogger(logrus.Fields{
					"rc_id": rcField.ID,
				})
				if _, ok := rcm.children[rcField.ID]; ok {
					// this one is already ours, skip
					rcLogger.NoFields().Debugln("Got replication controller already owned by self")
					foundChildren[rcField.ID] = struct{}{}
					continue
				}

				err := rcm.lock.Lock(kp.LockPath(kp.RCPath(rcField.ID.String())))
				if _, ok := err.(kp.AlreadyLockedError); ok {
					// someone else must have gotten it first - log and move to
					// the next one
					rcLogger.NoFields().Debugln("Lock on replication controller was denied")
					continue
				} else if err != nil {
					rcLogger.NoFields().Errorln("Got error while locking replication controller - session may be expired")
					// stop processing this update and go back to the select
					// chances are this error is a network problem or session
					// expiry, and all the others in this update would also fail
					continue START_LOOP
				}

				// at this point the rc is ours, time to spin it up
				rcLogger.NoFields().Infoln("Acquired lock on new replication controller, spawning")

				newChild := New(
					rcField,
					rcm.kpStore,
					rcm.rcStore,
					rcm.scheduler,
					rcm.labeler,
					rcLogger,
				)
				childQuit := make(chan struct{})
				rcm.children[rcField.ID] = childRC{rc: newChild, quit: childQuit}
				foundChildren[rcField.ID] = struct{}{}

				go func() {
					// disabled-ness is handled in watchdesires
					for err := range newChild.WatchDesires(childQuit) {
						rcLogger.WithError(err).Errorln("Got error in replication controller loop")
					}
				}()
			}

			// now remove any children that were not found in the result set
			rcm.logger.NoFields().Debugln("Pruning replication controllers that have disappeared")
			for id := range rcm.children {
				if _, ok := foundChildren[id]; !ok {
					rcm.releaseChild(id)
				}
			}
		}
	}
}

// close one child
func (rcm *Farm) releaseChild(id fields.ID) {
	rcm.logger.WithField("rc_id", id).Infoln("Releasing replication controller")
	close(rcm.children[id].quit)
	delete(rcm.children, id)

	// if our lock is active, attempt to gracefully release it on this rc
	if rcm.lock != nil {
		err := rcm.lock.Unlock(kp.LockPath(kp.RCPath(id.String())))
		if err != nil {
			rcm.logger.WithField("rc_id", id).Warnln("Could not release replication controller lock")
		}
	}
}

// close all children
func (rcm *Farm) releaseChildren() {
	for id := range rcm.children {
		// it's safe to delete this element during iteration,
		// because we have already iterated over it
		rcm.releaseChild(id)
	}
}
