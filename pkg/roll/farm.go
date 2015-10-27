package roll

import (
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"

	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/kp/rollstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	roll_fields "github.com/square/p2/pkg/roll/fields"
)

type Factory interface {
	New(roll_fields.Update, logging.Logger, kp.Lock) Update
}

type UpdateFactory struct {
	KPStore       kp.Store
	RCStore       rcstore.Store
	HealthChecker checker.ConsulHealthChecker
	Labeler       labels.Applicator
	Scheduler     rc.Scheduler
}

func (f UpdateFactory) New(u roll_fields.Update, l logging.Logger, lock kp.Lock) Update {
	return NewUpdate(u, f.KPStore, f.RCStore, f.HealthChecker, f.Labeler, f.Scheduler, l, lock)
}

// The Farm is responsible for spawning and reaping rolling updates as they are
// added to and deleted from Consul. Multiple farms can exist simultaneously,
// but each one must hold a different Consul session. This ensures that the
// farms do not instantiate the same rolling update multiple times.
type Farm struct {
	factory  Factory
	kps      kp.Store
	rls      rollstore.Store
	sessions <-chan string

	children map[fields.ID]childRU
	lock     *kp.Lock

	logger logging.Logger
}

type childRU struct {
	ru   Update
	quit chan<- struct{}
}

func NewFarm(
	factory Factory,
	kps kp.Store,
	rls rollstore.Store,
	sessions <-chan string,
	logger logging.Logger,
) *Farm {
	return &Farm{
		factory:  factory,
		kps:      kps,
		rls:      rls,
		sessions: sessions,
		logger:   logger,
		children: make(map[fields.ID]childRU),
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
	subQuit := make(chan struct{})
	defer close(subQuit)
	rlWatch, rlErr := rlf.rls.Watch(subQuit)

START_LOOP:
	for {
		select {
		case <-quit:
			rlf.logger.NoFields().Infoln("Halt requested, releasing updates")
			rlf.releaseChildren()
			return
		case session := <-rlf.sessions:
			if session == "" {
				// our session has expired, we must assume our locked children
				// have all been released and that someone else may have
				// claimed them by now
				rlf.logger.NoFields().Errorln("Session expired, releasing updates")
				rlf.lock = nil
				rlf.releaseChildren()
			} else {
				// a new session has been acquired - only happens after an
				// expiration message, so len(children)==0
				rlf.logger.WithField("session", session).Infoln("Acquired new session")
				lock := rlf.kps.NewUnmanagedLock(session, "")
				rlf.lock = &lock
				// TODO: restart the watch so that you get updates right away?
			}
		case err := <-rlErr:
			rlf.logger.WithError(err).Errorln("Could not read consul updates")
		case rlFields := <-rlWatch:
			rlf.logger.WithField("n", len(rlFields)).Debugln("Received update update")
			if rlf.lock == nil {
				// we can't claim new nodes because our session is invalidated.
				// raise an error and ignore this update
				rlf.logger.NoFields().Warnln("Received update update, but do not have session to acquire locks")
				continue
			}

			// track which children were found in the returned set
			foundChildren := make(map[fields.ID]struct{})
			for _, rlField := range rlFields {
				rlLogger := rlf.logger.SubLogger(logrus.Fields{
					"ru": rlField.NewRC,
				})
				if _, ok := rlf.children[rlField.NewRC]; ok {
					// this one is already ours, skip
					rlLogger.NoFields().Debugln("Got update already owned by self")
					foundChildren[rlField.NewRC] = struct{}{}
					continue
				}

				err := rlf.lock.Lock(kp.LockPath(kp.RollPath(rlField.NewRC.String())))
				if _, ok := err.(kp.AlreadyLockedError); ok {
					// someone else must have gotten it first - log and move to
					// the next one
					rlLogger.NoFields().Debugln("Lock on update was denied")
					continue
				} else if err != nil {
					rlLogger.NoFields().Errorln("Got error while locking update - session may be expired")
					// stop processing this update and go back to the select
					// chances are this error is a network problem or session
					// expiry, and all the others in this update would also fail
					continue START_LOOP
				}

				// at this point the ru is ours, time to spin it up
				rlLogger.NoFields().Infoln("Acquired lock on new update, spawning")

				newChild := rlf.factory.New(rlField, rlLogger, *rlf.lock)
				childQuit := make(chan struct{})
				rlf.children[rlField.NewRC] = childRU{ru: newChild, quit: childQuit}
				foundChildren[rlField.NewRC] = struct{}{}

				go func(id fields.ID) {
					if !newChild.Run(childQuit) {
						// returned false, farm must have asked us to quit
						return
					}
					// our lock on this RU won't be released until it's deleted,
					// so if we fail to delete it, we have to retry
					for err := rlf.rls.Delete(id); err != nil; err = rlf.rls.Delete(id) {
						rlLogger.WithError(err).Errorln("Could not delete update")
						time.Sleep(1 * time.Second)
					}
				}(rlField.NewRC) // do not close over rlField, it's a loop variable
			}

			// now remove any children that were not found in the result set
			rlf.logger.NoFields().Debugln("Pruning updates that have disappeared")
			for id := range rlf.children {
				if _, ok := foundChildren[id]; !ok {
					rlf.releaseChild(id)
				}
			}
		}
	}
}

// close one child
func (rlf *Farm) releaseChild(id fields.ID) {
	rlf.logger.WithField("ru", id).Infoln("Releasing update")
	close(rlf.children[id].quit)
	delete(rlf.children, id)

	// if our lock is active, attempt to gracefully release it
	if rlf.lock != nil {
		err := rlf.lock.Unlock(kp.LockPath(kp.RollPath(id.String())))
		if err != nil {
			rlf.logger.WithField("ru", id).Warnln("Could not release update lock")
		}
	}
}

// close all children
func (rlf *Farm) releaseChildren() {
	for id := range rlf.children {
		// it's safe to delete this element during iteration,
		// because we have already iterated over it
		rlf.releaseChild(id)
	}
}
