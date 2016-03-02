package roll

import (
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"

	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/kp/rollstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	roll_fields "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/util"
)

type Factory interface {
	New(roll_fields.Update, logging.Logger, kp.Session) Update
}

type UpdateFactory struct {
	KPStore       kp.Store
	RCStore       rcstore.Store
	HealthChecker checker.ConsulHealthChecker
	Labeler       labels.Applicator
	Scheduler     rc.Scheduler
}

func (f UpdateFactory) New(u roll_fields.Update, l logging.Logger, session kp.Session) Update {
	return NewUpdate(u, f.KPStore, f.RCStore, f.HealthChecker, f.Labeler, f.Scheduler, l, session)
}

type RCGetter interface {
	Get(id fields.ID) (fields.RC, error)
}

// The Farm is responsible for spawning and reaping rolling updates as they are
// added to and deleted from Consul. Multiple farms can exist simultaneously,
// but each one must hold a different Consul session. This ensures that the
// farms do not instantiate the same rolling update multiple times.
type Farm struct {
	factory  Factory
	kps      kp.Store
	rls      rollstore.Store
	rcs      RCGetter
	sessions <-chan string

	children map[fields.ID]childRU
	session  kp.Session

	logger logging.Logger
}

type childRU struct {
	ru       Update
	unlocker kp.Unlocker
	quit     chan<- struct{}
}

func NewFarm(
	factory Factory,
	kps kp.Store,
	rls rollstore.Store,
	rcs RCGetter,
	sessions <-chan string,
	logger logging.Logger,
) *Farm {
	return &Farm{
		factory:  factory,
		kps:      kps,
		rls:      rls,
		rcs:      rcs,
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
			foundChildren := make(map[fields.ID]struct{})
			for _, rlField := range rlFields {
				rlLogger := rlf.logger.SubLogger(logrus.Fields{
					"ru": rlField.NewRC,
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
				if _, ok := rlf.children[rlField.NewRC]; ok {
					// this one is already ours, skip
					rlLogger.NoFields().Debugln("Got update already owned by self")
					foundChildren[rlField.NewRC] = struct{}{}
					continue
				}

				lockPath, err := rollstore.RollLockPath(rlField.NewRC)
				if err != nil {
					rlLogger.WithError(err).Errorln("Unable to compute roll lock path")
				}

				unlocker, err := rlf.session.Lock(lockPath)
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

				newChild := rlf.factory.New(rlField, rlLogger, rlf.session)
				childQuit := make(chan struct{})
				rlf.children[rlField.NewRC] = childRU{
					ru:       newChild,
					quit:     childQuit,
					unlocker: unlocker,
				}
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
	for id := range rlf.children {
		// it's safe to delete this element during iteration,
		// because we have already iterated over it
		rlf.releaseChild(id)
	}
}
