package dsstoretest

import (
	"sync"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	created = "created"
	updated = "updated"
	deleted = "deleted"
)

type FakeWatchedDaemonSet struct {
	DaemonSet *fields.DaemonSet
	Operation string
	Err       error
}

// Used for unit testing
type FakeDSStore struct {
	daemonSets   map[fields.ID]fields.DaemonSet
	watchers     map[fields.ID]chan FakeWatchedDaemonSet
	watchersLock sync.Locker
	logger       logging.Logger
}

var _ dsstore.Store = &FakeDSStore{}

func NewFake() *FakeDSStore {
	return &FakeDSStore{
		daemonSets:   make(map[fields.ID]fields.DaemonSet),
		watchers:     make(map[fields.ID]chan FakeWatchedDaemonSet),
		watchersLock: &sync.Mutex{},
		logger:       logging.DefaultLogger,
	}
}

func (s *FakeDSStore) Create(
	manifest manifest.Manifest,
	minHealth int,
	name fields.ClusterName,
	nodeSelector klabels.Selector,
	podID types.PodID,
) (fields.DaemonSet, error) {
	id := fields.ID(uuid.New())
	ds := fields.DaemonSet{
		ID:           id,
		Disabled:     false,
		Manifest:     manifest,
		MinHealth:    minHealth,
		Name:         name,
		NodeSelector: nodeSelector,
		PodID:        podID,
	}
	s.daemonSets[id] = ds

	s.watchersLock.Lock()
	defer s.watchersLock.Unlock()

	if _, ok := s.watchers[id]; ok {
		return ds, util.Errorf("Daemon set uuid collision on id: %v", id)
	}

	watched := FakeWatchedDaemonSet{
		DaemonSet: &ds,
		Operation: created,
		Err:       nil,
	}
	s.watchers[id] = make(chan FakeWatchedDaemonSet, 1)
	s.watchers[id] <- watched

	return ds, nil
}

func (s *FakeDSStore) Delete(id fields.ID) error {
	if ds, ok := s.daemonSets[id]; ok {
		s.watchersLock.Lock()
		defer s.watchersLock.Unlock()
		if watcher, ok := s.watchers[id]; ok {
			delete(s.daemonSets, id)
			watched := FakeWatchedDaemonSet{
				DaemonSet: &ds,
				Operation: deleted,
				Err:       nil,
			}
			watcher <- watched
			return nil
		}
	}
	return dsstore.NoDaemonSet
}

func (s *FakeDSStore) Get(id fields.ID) (fields.DaemonSet, *api.QueryMeta, error) {
	//TODO: Check if there is a use for this in the fake dsstore
	queryMeta := &api.QueryMeta{
		KnownLeader: false,
		LastContact: 0,
		LastIndex:   0,
		RequestTime: 0,
	}
	if ds, ok := s.daemonSets[id]; ok {
		return ds, queryMeta, nil
	}
	return fields.DaemonSet{}, queryMeta, dsstore.NoDaemonSet
}

func (s *FakeDSStore) List() ([]fields.DaemonSet, error) {
	var ret []fields.DaemonSet
	for _, ds := range s.daemonSets {
		ret = append(ret, ds)
	}
	return ret, nil
}

func (s *FakeDSStore) MutateDS(
	id fields.ID,
	mutator func(fields.DaemonSet) (fields.DaemonSet, error),
) (fields.DaemonSet, error) {
	ds, _, err := s.Get(id)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	ds, err = mutator(ds)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	s.daemonSets[id] = ds

	s.watchersLock.Lock()
	defer s.watchersLock.Unlock()
	if watcher, ok := s.watchers[id]; ok {
		watched := FakeWatchedDaemonSet{
			DaemonSet: &ds,
			Operation: updated,
			Err:       nil,
		}
		// In case you mutate more than once and no one reads from the channel
		// this will prevent a deadlock, if no one reads from the update, replacing
		// the data in the channel will still keep the functionality of the fake watch
		select {
		case <-watcher:
			watcher <- watched
		case watcher <- watched:
		}
	}

	return ds, nil
}

func (s *FakeDSStore) Disable(id fields.ID) (fields.DaemonSet, error) {
	s.logger.Infof("Attempting to disable '%s' in store now", id)

	mutator := func(dsToUpdate fields.DaemonSet) (fields.DaemonSet, error) {
		dsToUpdate.Disabled = true
		return dsToUpdate, nil
	}
	newDS, err := s.MutateDS(id, mutator)

	// Delete the daemon set because there was an error during mutation
	if err != nil {
		s.logger.Errorf("Error occured when trying to disable daemon set in store: %v, attempting to delete now", err)
		err = s.Delete(id)
		// If you tried to delete it and there was an error, this is fatal
		if err != nil {
			return fields.DaemonSet{}, err
		}
		s.logger.Infof("Deletion was successful for the daemon set: '%s' in store", id)
		return fields.DaemonSet{}, nil
	}

	s.logger.Infof("Daemon set '%s' was successfully disabled in store", id)
	return newDS, nil
}

func (s *FakeDSStore) Watch(quitCh <-chan struct{}) <-chan dsstore.WatchedDaemonSets {
	outCh := make(chan dsstore.WatchedDaemonSets)

	go func() {
		for {
			select {
			case <-quitCh:
				return
			default:
			}

			s.watchersLock.Lock()
			outgoingChanges := dsstore.WatchedDaemonSets{}
			var watchersToDelete []fields.ID
			// Reads in new changes that are sent to FakeDSStore.watchers
			for _, ch := range s.watchers {
				select {
				case watched := <-ch:
					switch watched.Operation {
					case created:
						outgoingChanges.Created = append(outgoingChanges.Created, watched.DaemonSet)
					case updated:
						outgoingChanges.Updated = append(outgoingChanges.Updated, watched.DaemonSet)
					case deleted:
						outgoingChanges.Deleted = append(outgoingChanges.Deleted, watched.DaemonSet)
						watchersToDelete = append(watchersToDelete, watched.DaemonSet.ID)
					default:
					}
				default:
				}
			}
			// Remove deleted watchers
			for _, id := range watchersToDelete {
				delete(s.watchers, id)
			}
			s.watchersLock.Unlock()

			// Blocks until the receiver quits or reads outCh's previous output
			select {
			case outCh <- outgoingChanges:
			case <-quitCh:
				return
			}
		}
	}()

	return outCh
}
