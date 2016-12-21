package rcstore

import (
	"fmt"
	"strconv"
	"time"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/util"
)

type fakeStore struct {
	rcs     map[store.ReplicationControllerID]*fakeEntry
	creates int
}

type fakeEntry struct {
	store.ReplicationController
	watchers      map[int]chan struct{}
	lastWatcherId int
	lockedRead    string
	lockedWrite   string
}

var _ Store = &fakeStore{}

func NewFake() *fakeStore {
	return &fakeStore{
		rcs:     make(map[store.ReplicationControllerID]*fakeEntry),
		creates: 0,
	}
}

func (s *fakeStore) Create(manifest manifest.Manifest, nodeSelector labels.Selector, podLabels labels.Set) (store.ReplicationController, error) {
	// A real replication controller will use a UUID.
	// We'll just use a monotonically increasing counter for expedience.
	s.creates += 1
	id := store.ReplicationControllerID(strconv.Itoa(s.creates))

	entry := fakeEntry{
		ReplicationController: store.ReplicationController{
			ID:              id,
			Manifest:        manifest,
			NodeSelector:    nodeSelector,
			PodLabels:       podLabels,
			ReplicasDesired: 0,
			Disabled:        false,
		},
		watchers:      make(map[int]chan struct{}),
		lastWatcherId: 0,
	}

	s.rcs[id] = &entry

	return entry.ReplicationController, nil
}

func (s *fakeStore) Get(id store.ReplicationControllerID) (store.ReplicationController, error) {
	entry, ok := s.rcs[id]
	if !ok {
		return store.ReplicationController{}, NoReplicationController
	}

	return entry.ReplicationController, nil
}

func (s *fakeStore) List() ([]store.ReplicationController, error) {
	results := make([]store.ReplicationController, len(s.rcs))
	i := 0
	for _, v := range s.rcs {
		results[i] = v.ReplicationController
		i += 1
	}
	return results, nil
}

func (s *fakeStore) WatchNew(quit <-chan struct{}) (<-chan []store.ReplicationController, <-chan error) {
	return nil, nil
}

func (s *fakeStore) WatchNewWithRCLockInfo(quit <-chan struct{}, pauseTime time.Duration) (<-chan []RCLockResult, <-chan error) {
	panic("not implemented")
}

func (s *fakeStore) Disable(id store.ReplicationControllerID) error {
	entry, ok := s.rcs[id]
	if !ok {
		return util.Errorf("Nonexistent RC")
	}

	entry.Disabled = true
	for _, channel := range entry.watchers {
		channel <- struct{}{}
	}
	return nil
}

func (s *fakeStore) Enable(id store.ReplicationControllerID) error {
	entry, ok := s.rcs[id]
	if !ok {
		return util.Errorf("Nonexistent RC")
	}

	entry.Disabled = false
	for _, channel := range entry.watchers {
		channel <- struct{}{}
	}
	return nil
}

func (s *fakeStore) SetDesiredReplicas(id store.ReplicationControllerID, n int) error {
	entry, ok := s.rcs[id]
	if !ok {
		return util.Errorf("Nonexistent RC")
	}

	entry.ReplicasDesired = n
	for _, channel := range entry.watchers {
		channel <- struct{}{}
	}
	return nil
}

func (s *fakeStore) AddDesiredReplicas(id store.ReplicationControllerID, n int) error {
	entry, ok := s.rcs[id]
	if !ok {
		return util.Errorf("Nonexistent RC")
	}

	entry.ReplicasDesired += n
	if entry.ReplicasDesired < 0 {
		entry.ReplicasDesired = 0
	}
	for _, channel := range entry.watchers {
		channel <- struct{}{}
	}
	return nil
}

func (s *fakeStore) CASDesiredReplicas(id store.ReplicationControllerID, expected int, n int) error {
	entry, ok := s.rcs[id]
	if !ok {
		return util.Errorf("Nonexistent RC")
	}

	if entry.ReplicasDesired != expected {
		return util.Errorf("pre-empted")
	}

	entry.ReplicasDesired = n
	for _, channel := range entry.watchers {
		channel <- struct{}{}
	}
	return nil
}

func (s *fakeStore) Delete(id store.ReplicationControllerID, force bool) error {
	entry, ok := s.rcs[id]
	if !ok {
		return util.Errorf("Nonexistent RC")
	}

	if !force && entry.ReplicasDesired != 0 {
		return util.Errorf("Replicas desired must be 0 to delete.")
	}

	for k, channel := range entry.watchers {
		delete(entry.watchers, k)
		close(channel)
	}

	delete(s.rcs, id)
	return nil
}

func (s *fakeStore) Watch(rc *store.ReplicationController, quit <-chan struct{}) (<-chan struct{}, <-chan error) {
	updatesOut := make(chan struct{})
	entry, ok := s.rcs[rc.ID]
	if !ok {
		errors := make(chan error, 1)
		errors <- util.Errorf("Nonexistent RC")
		close(updatesOut)
		close(errors)
		return updatesOut, errors
	}

	errors := make(chan error)
	updatesIn := make(chan struct{})
	entry.lastWatcherId += 1
	id := entry.lastWatcherId
	entry.watchers[id] = updatesIn

	go func() {
		<-quit
		// Delete() may have closed updatesIn already!
		// So we only close if we are still alive.
		if _, stillAlive := entry.watchers[id]; stillAlive {
			delete(entry.watchers, id)
			close(updatesIn)
		}
	}()

	go func() {
		for range updatesIn {
			*rc = entry.ReplicationController
			updatesOut <- struct{}{}
		}
		close(updatesOut)
		close(errors)
	}()

	return updatesOut, errors
}

func (s *fakeStore) LockForMutation(rcID store.ReplicationControllerID, session kp.Session) (consulutil.Unlocker, error) {
	key := fmt.Sprintf("%s/%s", rcID, "mutation_lock")
	return session.Lock(key)
}

func (s *fakeStore) LockForOwnership(rcID store.ReplicationControllerID, session kp.Session) (consulutil.Unlocker, error) {
	key := fmt.Sprintf("%s/%s", rcID, "ownership_lock")
	return session.Lock(key)
}

func (s *fakeStore) LockForUpdateCreation(rcID store.ReplicationControllerID, session kp.Session) (consulutil.Unlocker, error) {
	key := fmt.Sprintf("%s/%s", rcID, "update_creation_lock")
	return session.Lock(key)
}
