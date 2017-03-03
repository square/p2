package dsstoretest

import (
	"path"
	"sync"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/types"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const dsTree string = "daemon_sets"

type FakeWatchedDaemonSet struct {
	DaemonSet *fields.DaemonSet
	Operation string
	Err       error
}

// Used for unit testing
type FakeDSStore struct {
	daemonSets map[fields.ID]fields.DaemonSet
	writeLock  sync.Locker
	logger     logging.Logger
}

func NewFake() *FakeDSStore {
	return &FakeDSStore{
		daemonSets: make(map[fields.ID]fields.DaemonSet),
		writeLock:  &sync.Mutex{},
		logger:     logging.DefaultLogger,
	}
}

func (s *FakeDSStore) Create(
	manifest manifest.Manifest,
	minHealth int,
	name fields.ClusterName,
	nodeSelector klabels.Selector,
	podID types.PodID,
	timeout time.Duration,
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
		Timeout:      timeout,
	}

	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if _, ok := s.daemonSets[id]; ok {
		return ds, util.Errorf("Daemon set uuid collision on id: %v", id)
	}
	s.daemonSets[id] = ds

	return ds, nil
}

func (s *FakeDSStore) Delete(id fields.ID) error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if _, ok := s.daemonSets[id]; ok {
		delete(s.daemonSets, id)
		return nil
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
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	ds, _, err := s.Get(id)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	ds, err = mutator(ds)
	if err != nil {
		return fields.DaemonSet{}, err
	}

	s.daemonSets[id] = ds

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
		return newDS, util.Errorf("Error occured when trying to disable daemon set in store: %v", err)
	}

	s.logger.Infof("Daemon set '%s' was successfully disabled in store", id)
	return newDS, nil
}

func (s *FakeDSStore) WatchList(quitCh <-chan struct{}) <-chan []fields.DaemonSet {
	outCh := make(chan []fields.DaemonSet)

	go func() {
		for {
			select {
			case <-quitCh:
				return
			default:
			}

			dsList, err := s.List()
			if err != nil {
				s.logger.Errorf("Encountered an error in WatchList: '%v'", err)
			}

			// Blocks until the receiver quits or reads outCh's previous output
			select {
			case outCh <- dsList:
			case <-quitCh:
				return
			}
		}
	}()

	return outCh
}

func (s *FakeDSStore) Watch(quitCh <-chan struct{}) <-chan dsstore.WatchedDaemonSets {
	inCh := s.WatchList(quitCh)
	return s.watchDiffDaemonSets(inCh, quitCh)
}

// pauseTime not implemented
func (s *FakeDSStore) WatchAll(quitCh <-chan struct{}, _ time.Duration) <-chan dsstore.WatchedDaemonSetList {
	inCh := s.WatchList(quitCh)
	outCh := make(chan dsstore.WatchedDaemonSetList)

	go func() {
		defer close(outCh)

		for {
			var results dsstore.WatchedDaemonSetList
			select {
			case <-quitCh:
				return
			case val, ok := <-inCh:
				if !ok {
					// channel closed
					return
				}
				results.DaemonSets = val
				results.Err = nil
			}

			select {
			case <-quitCh:
				return
			case outCh <- results:
			}
		}
	}()

	return outCh
}

func (s *FakeDSStore) watchDiffDaemonSets(inCh <-chan []fields.DaemonSet, quitCh <-chan struct{}) <-chan dsstore.WatchedDaemonSets {
	outCh := make(chan dsstore.WatchedDaemonSets)

	go func() {
		defer close(outCh)
		oldDSs := make(map[fields.ID]fields.DaemonSet)

		for {
			var results []fields.DaemonSet
			select {
			case <-quitCh:
				return
			case val, ok := <-inCh:
				if !ok {
					// channel closed
					return
				}
				results = val
			}

			newDSs := make(map[fields.ID]fields.DaemonSet)
			for _, ds := range results {
				newDSs[ds.ID] = ds
			}

			outgoingChanges := dsstore.WatchedDaemonSets{}
			for id, ds := range newDSs {
				copyDS := ds

				if _, ok := oldDSs[id]; !ok {
					// If it was not observed, then it was created
					outgoingChanges.Created = append(outgoingChanges.Created, &copyDS)
					oldDSs[id] = copyDS

				} else if !dsEquals(oldDSs[id], copyDS) {
					// If they are not equal, update them
					outgoingChanges.Updated = append(outgoingChanges.Updated, &copyDS)
					oldDSs[id] = copyDS
				}
				// Otherwise no changes need to be made
			}

			for id, ds := range oldDSs {
				copyDS := ds
				if _, ok := newDSs[id]; !ok {
					outgoingChanges.Deleted = append(outgoingChanges.Deleted, &copyDS)
					delete(oldDSs, id)
				}
			}

			select {
			case <-quitCh:
				return
			case outCh <- outgoingChanges:
			}
		}
	}()

	return outCh
}

func dsEquals(firstDS fields.DaemonSet, secondDS fields.DaemonSet) bool {
	if (firstDS.ID != secondDS.ID) ||
		(firstDS.Disabled != secondDS.Disabled) ||
		(firstDS.MinHealth != secondDS.MinHealth) ||
		(firstDS.Name != secondDS.Name) ||
		(firstDS.NodeSelector.String() != secondDS.NodeSelector.String()) ||
		(firstDS.PodID != secondDS.PodID) ||
		(firstDS.Timeout != secondDS.Timeout) {
		return false
	}

	firstSHA, err := firstDS.Manifest.SHA()
	if err != nil {
		return false
	}

	secondSHA, err := secondDS.Manifest.SHA()
	if err != nil {
		return false
	}

	return firstSHA == secondSHA
}

func (s *FakeDSStore) dsPath(dsID fields.ID) (string, error) {
	if dsID == "" {
		return "", util.Errorf("Path requested for empty DS id")
	}
	return path.Join(dsTree, dsID.String()), nil
}

func (s *FakeDSStore) dsLockPath(dsID fields.ID) (string, error) {
	dsPath, err := s.dsPath(dsID)
	if err != nil {
		return "", err
	}
	return path.Join(consulutil.LOCK_TREE, dsPath), nil
}

func (s *FakeDSStore) LockForOwnership(dsID fields.ID, session consul.Session) (consulutil.Unlocker, error) {
	lockPath, err := s.dsLockPath(dsID)
	if err != nil {
		return nil, err
	}
	s.logger.Logger.Infof("Locking daemon set on the following path: '%v'", lockPath)
	return session.Lock(lockPath)
}
