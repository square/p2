package ds

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/ds/fields"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/scheduler"
)

// Farm instatiates and deletes daemon sets as needed
type Farm struct {
	// constructor arguments
	kpStore    kp.Store
	dsStore    dsstore.Store
	scheduler  scheduler.Scheduler
	applicator labels.Applicator

	// TODO: Make use of these locks
	children map[fields.ID]*childDS
	childMu  sync.Mutex

	logger  logging.Logger
	alerter alerting.Alerter
}

type childDS struct {
	ds        DaemonSet
	quitCh    chan<- struct{}
	updatedCh chan<- *ds_fields.DaemonSet
	deletedCh chan<- *ds_fields.DaemonSet
	errCh     <-chan error
}

func NewFarm(
	kpStore kp.Store,
	dsStore dsstore.Store,
	applicator labels.Applicator,
	logger logging.Logger,
	alerter alerting.Alerter,
) *Farm {
	if alerter == nil {
		alerter = alerting.NewNop()
	}

	return &Farm{
		kpStore:    kpStore,
		dsStore:    dsStore,
		scheduler:  scheduler.NewApplicatorScheduler(applicator),
		applicator: applicator,
		children:   make(map[fields.ID]*childDS),
		logger:     logger,
		alerter:    alerter,
	}
}

func (dsf *Farm) Start(quitCh <-chan struct{}) {
	dsf.mainLoop(quitCh)
}

func (dsf *Farm) mainLoop(quitCh <-chan struct{}) {
	subQuit := make(chan struct{})
	defer close(subQuit)
	dsWatch := dsf.dsStore.Watch(subQuit)

	defer dsf.closeAllChildren()

	var changes dsstore.WatchedDaemonSets
	var err error
	var ok bool

	for {
		select {
		case <-quitCh:
			return
		case changes, ok = <-dsWatch:
			if !ok {
				return
			}
		}

		// This loop will check all the error channels of the children spawn by this
		// farm, if any child outputs an error, close the child.
		// The quitCh here will also return if the caller to mainLoop closes it
		for dsID, child := range dsf.children {
			select {
			case <-quitCh:
				return
			case err, ok = <-child.errCh:
				if !ok {
					// child error channel closed
					dsf.closeChild(dsID)
					continue
				}
				dsf.logger.Errorf("An error has occurred in spawned ds '%v':, %v", child.ds, err)
				continue
			default:
			}
		}
		dsf.handleDSChanges(changes, quitCh)
	}
}

func (dsf *Farm) closeAllChildren() {
	for dsID := range dsf.children {
		dsf.closeChild(dsID)
	}
}

func (dsf *Farm) closeChild(dsID fields.ID) {
	if child, ok := dsf.children[dsID]; ok {
		close(child.quitCh)
		close(child.updatedCh)
		close(child.deletedCh)
		delete(dsf.children, dsID)
	}
}

func (dsf *Farm) handleDSChanges(changes dsstore.WatchedDaemonSets, quitCh <-chan struct{}) {
	if changes.Err != nil {
		dsf.logger.Infof("An error has occurred while watching daemon sets: %v", changes.Err)
		return
	}

	if len(changes.Created) > 0 {
		dsf.logger.Infof("The following daemon sets have been created:")
		for _, dsFields := range changes.Created {
			dsf.logger.Infof("%v", *dsFields)

			// If the daemon set contends with another daemon set, disable it
			dsIDContended, isContended, err := dsf.dsContends(dsFields)
			if err != nil {
				dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
				continue
			}

			if isContended {
				dsf.logger.Errorf("Created daemon set '%s' contends with %s", dsFields.ID, dsIDContended)
				newDS, err := dsf.dsStore.Disable(dsFields.ID)
				if err != nil {
					dsf.logger.Errorf("Error occurred when trying to disable daemon set: %v", err)
					continue
				}
				dsf.children[newDS.ID] = dsf.spawnDaemonSet(&newDS)
			} else {
				dsf.children[dsFields.ID] = dsf.spawnDaemonSet(dsFields)
			}
		}
	}

	if len(changes.Updated) > 0 {
		dsf.logger.Infof("The following daemon sets have been updated:")
		for _, dsFields := range changes.Updated {
			dsf.logger.Infof("%v", *dsFields)

			// If the daemon set contends with another daemon set, disable it
			if _, ok := dsf.children[dsFields.ID]; ok {
				dsIDContended, isContended, err := dsf.dsContends(dsFields)
				if err != nil {
					dsf.logger.Errorf("Error occurred when trying to check for daemon set contention: %v", err)
					continue
				}

				if isContended {
					dsf.logger.Errorf("Updated daemon set '%s' contends with %s", dsFields.ID, dsIDContended)
					newDS, err := dsf.dsStore.Disable(dsFields.ID)
					if err != nil {
						dsf.logger.Errorf("Error occurred when trying to delete daemon set: %v", err)
						continue
					}
					dsf.children[newDS.ID].updatedCh <- &newDS
				} else {
					dsf.children[dsFields.ID].updatedCh <- dsFields
				}
			}
		}
	}

	if len(changes.Deleted) > 0 {
		dsf.logger.Infof("The following daemon sets have been deleted:")
		for _, dsFields := range changes.Deleted {
			dsf.logger.Infof("%v", *dsFields)
			if child, ok := dsf.children[dsFields.ID]; ok {
				select {
				case <-quitCh:
					return
				case err := <-child.errCh:
					if err != nil {
						dsf.logger.Errorf("Error occurred when deleting spawned daemon set '%v': %v", dsFields, err)
					}
					dsf.closeChild(dsFields.ID)
				case child.deletedCh <- dsFields:
					dsf.closeChild(dsFields.ID)
				}
			}
		}
	}
}

func (dsf *Farm) dsContends(dsFields *ds_fields.DaemonSet) (ds_fields.ID, bool, error) {
	// NOT IMPLEMENTED
	return "", false, nil
}

// Creates a functioning daemon set that will watch and write to the pod tree
func (dsf *Farm) spawnDaemonSet(dsFields *ds_fields.DaemonSet) *childDS {
	dsLogger := dsf.logger.SubLogger(logrus.Fields{
		"ds":  dsFields.ID,
		"pod": dsFields.Manifest.ID(),
	})

	ds := New(
		*dsFields,
		dsf.dsStore,
		dsf.kpStore,
		dsf.applicator,
		dsLogger,
	)

	quitSpawnCh := make(chan struct{})
	updatedCh := make(chan *ds_fields.DaemonSet)
	deletedCh := make(chan *ds_fields.DaemonSet)

	desiresCh := ds.WatchDesires(quitSpawnCh, updatedCh, deletedCh)

	return &childDS{
		ds:        ds,
		quitCh:    quitSpawnCh,
		updatedCh: updatedCh,
		deletedCh: deletedCh,
		errCh:     desiresCh,
	}
}
