package rcstore

import (
	"fmt"
	"strconv"
	"time"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/util"
)

type fakeStore struct {
	rcs     map[fields.ID]*fakeEntry
	creates int
}

type fakeEntry struct {
	fields.RC
	watchers      map[int]chan struct{}
	lastWatcherId int
	lockedRead    string
	lockedWrite   string
}

func NewFake() *fakeStore {
	return &fakeStore{
		rcs:     make(map[fields.ID]*fakeEntry),
		creates: 0,
	}
}

func (s *fakeStore) Create(manifest manifest.Manifest, nodeSelector labels.Selector, podLabels labels.Set) (fields.RC, error) {
	// A real replication controller will use a UUID.
	// We'll just use a monotonically increasing counter for expedience.
	s.creates += 1
	id := fields.ID(strconv.Itoa(s.creates))

	entry := fakeEntry{
		RC: fields.RC{
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

	return entry.RC, nil
}

func (s *fakeStore) Get(id fields.ID) (fields.RC, error) {
	entry, ok := s.rcs[id]
	if !ok {
		return fields.RC{}, NoReplicationController
	}

	return entry.RC, nil
}

func (s *fakeStore) List() ([]fields.RC, error) {
	results := make([]fields.RC, len(s.rcs))
	i := 0
	for _, v := range s.rcs {
		results[i] = v.RC
		i += 1
	}
	return results, nil
}

func (s *fakeStore) WatchNew(quit <-chan struct{}) (<-chan []fields.RC, <-chan error) {
	return nil, nil
}

func (s *fakeStore) WatchRCKeysWithLockInfo(quit <-chan struct{}, pauseTime time.Duration) (<-chan []RCLockResult, <-chan error) {
	panic("not implemented")
}

func (s *fakeStore) Disable(id fields.ID) error {
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

func (s *fakeStore) Enable(id fields.ID) error {
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

func (s *fakeStore) SetDesiredReplicas(id fields.ID, n int) error {
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

func (s *fakeStore) AddDesiredReplicas(id fields.ID, n int) error {
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

func (s *fakeStore) CASDesiredReplicas(id fields.ID, expected int, n int) error {
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

func (s *fakeStore) Delete(id fields.ID, force bool) error {
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

func (s *fakeStore) Watch(rc *fields.RC, quit <-chan struct{}) (<-chan struct{}, <-chan error) {
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
			*rc = entry.RC
			updatesOut <- struct{}{}
		}
		close(updatesOut)
		close(errors)
	}()

	return updatesOut, errors
}

func (s *fakeStore) LockForMutation(rcID fields.ID, session consul.Session) (consulutil.Unlocker, error) {
	key := fmt.Sprintf("%s/%s", rcID, "mutation_lock")
	return session.Lock(key)
}

func (s *fakeStore) LockForOwnership(rcID fields.ID, session consul.Session) (consulutil.Unlocker, error) {
	key := fmt.Sprintf("%s/%s", rcID, "ownership_lock")
	return session.Lock(key)
}

func (s *fakeStore) LockForUpdateCreation(rcID fields.ID, session consul.Session) (consulutil.Unlocker, error) {
	key := fmt.Sprintf("%s/%s", rcID, "update_creation_lock")
	return session.Lock(key)
}
