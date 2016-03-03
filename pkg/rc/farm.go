package rc

import (
	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
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
	session  kp.Session

	logger logging.Logger
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
			rcf.logger.WithField("n", len(rcFields)).Debugln("Received replication controller update")

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

				lockPath, err := rcstore.RCLockPath(rcField.ID)
				if err != nil {
					rcf.logger.WithError(err).Errorln("Could not compute path for rc lock")
					continue
				}

				rcUnlocker, err := rcf.session.Lock(lockPath)
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
					rcf.kpStore,
					rcf.rcStore,
					rcf.scheduler,
					rcf.labeler,
					rcLogger,
				)
				childQuit := make(chan struct{})
				rcf.children[rcField.ID] = childRC{
					rc:       newChild,
					quit:     childQuit,
					unlocker: rcUnlocker,
				}
				foundChildren[rcField.ID] = struct{}{}

				go func() {
					// disabled-ness is handled in watchdesires
					for err := range newChild.WatchDesires(childQuit) {
						rcLogger.WithError(err).Errorln("Got error in replication controller loop")
					}

					// NOTE: if WatchDesires experiences an unrecoverable error, we don't release the replication controller.
					// However, it is unlikely that another farm instance would fare any better so that's okay
				}()
			}

			// now remove any children that were not found in the result set
			rcf.logger.NoFields().Debugln("Pruning replication controllers that have disappeared")
			for id := range rcf.children {
				if _, ok := foundChildren[id]; !ok {
					rcf.releaseChild(id)
				}
			}
		}
	}
}

// close one child
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
	for id := range rcf.children {
		// it's safe to delete this element during iteration,
		// because we have already iterated over it
		rcf.releaseChild(id)
	}
}
