package rc

import (
	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"

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
	kpStore   kpStore
	rcStore   rcstore.Store
	scheduler Scheduler
	labeler   labels.Applicator

	// session stream for the rcs locked by this farm
	sessions <-chan string

	// rcs owned by this farm
	children map[fields.ID]childRC

	logger logging.Logger
}

type childRC struct {
	rc   ReplicationController
	quit chan<- struct{}
}

func NewFarm(
	kpStore kpStore,
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
// their job. Closing the quit channel will cause this function to return, but
// does not release locks.
//
// Start is not safe for concurrent execution. Do not execute multiple
// concurrent instances of Start.
func (rcm *Farm) Start(quit <-chan struct{}) {
	subQuit := make(chan struct{})
	defer close(subQuit)
	rcWatch, rcErr := rcm.rcStore.WatchNew(subQuit)
	var session string

	for {
		select {
		case <-quit:
			// TODO: we have to release these locks as well to make sure others
			// can capture them, otherwise the lock will persist until it expires
			// on its own. in practice, we will also kill the consul session
			// manager, so is this really a problem?
			rcm.logger.WithField("session", session).Infoln("Halt requested, releasing replication controllers")
			rcm.releaseChildren()
			return
		case session = <-rcm.sessions:
			if session == "" {
				// our session has expired, we must assume our locked children
				// have all been released and that someone else may have
				// claimed them by now
				rcm.logger.NoFields().Errorln("Session expired, releasing replication controllers")
				rcm.releaseChildren()
			} else {
				// a new session has been acquired - only happens after an
				// expiration message, so len(children)==0. right now this is a
				// noop, since the session has been set to its new value, but in
				// the future we should use a refactored lock API and instantiate
				// it here
				rcm.logger.WithField("session", session).Infoln("Acquired new session")
				// TODO: restart the watch so that you get updates right away?
			}
		case err := <-rcErr:
			rcm.logger.WithError(err).Errorln("Could not read consul replication controllers")
		case rcFields := <-rcWatch:
			rcm.logger.WithField("n", len(rcFields)).Debugln("Received replication controller update")
			if session == "" {
				// we can't claim new nodes because our session is invalidated.
				// raise an error and ignore this update
				rcm.logger.NoFields().Warnln("Received replication controller update, but do not have session to acquire locks")
				continue
			}

			// track which children were found in the returned set
			foundChildren := make(map[fields.ID]struct{})
			for _, rcField := range rcFields {
				if _, ok := rcm.children[rcField.ID]; ok {
					// this one is already ours, skip
					rcm.logger.WithField("rc_id", rcField.ID).Debugln("Got replication controller already owned by self")
					foundChildren[rcField.ID] = struct{}{}
					continue
				}

				success, err := rcm.rcStore.Lock(rcField.ID, session)
				if err != nil {
					// log and break out of the loop - we can't continue if
					// we had a real failure, our session probably disappeared
					rcm.logger.WithErrorAndFields(err, logrus.Fields{
						"rc_id":   rcField.ID,
						"session": session,
					}).Errorln("Got error while locking replication controller - session may be expired")
					break // TODO: should we be pruning after this?
				} else if !success {
					// someone else must have gotten it first - log and move to
					// the next one
					rcm.logger.WithFields(logrus.Fields{
						"rc_id":   rcField.ID,
						"session": session,
					}).Debugln("Lock on replication controller was denied")
					continue
				}

				// at this point the rc is ours, time to spin it up
				rcm.logger.WithFields(logrus.Fields{
					"rc_id":   rcField.ID,
					"session": session,
				}).Infoln("Acquired lock on new replication controller, spawning")

				newChild := New(
					rcField,
					rcm.kpStore,
					rcm.rcStore,
					rcm.scheduler,
					rcm.labeler,
					rcm.logger.SubLogger(logrus.Fields{"rc_id": rcField.ID}),
				)
				childQuit := make(chan struct{})
				rcm.children[rcField.ID] = childRC{rc: newChild, quit: childQuit}
				foundChildren[rcField.ID] = struct{}{}

				go func() {
					// disabled-ness is handled in watchdesires
					for range newChild.WatchDesires(childQuit) {
						rcm.logger.WithErrorAndFields(err, logrus.Fields{
							"rc_id": newChild.ID(),
						}).Errorln("Got error in replication controller loop")
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
}

// close all children
func (rcm *Farm) releaseChildren() {
	for id := range rcm.children {
		// it's safe to delete this element during iteration,
		// because we have already iterated over it
		rcm.releaseChild(id)
	}
}
