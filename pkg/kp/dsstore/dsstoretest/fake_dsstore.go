package dsstoretest

import (
	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/pods"
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
	daemonSets map[fields.ID]fields.DaemonSet
	watchers   map[fields.ID]chan FakeWatchedDaemonSet
}

var _ dsstore.Store = &FakeDSStore{}

func NewFake() *FakeDSStore {
	return &FakeDSStore{
		daemonSets: make(map[fields.ID]fields.DaemonSet),
	}
}

func (s *FakeDSStore) Create(
	manifest pods.Manifest,
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

	if watcher, ok := s.watchers[id]; ok {
		watched := FakeWatchedDaemonSet{
			DaemonSet: &ds,
			Operation: created,
			Err:       nil,
		}
		watcher <- watched
	}

	return ds, nil
}

func (s *FakeDSStore) Delete(id fields.ID) error {
	if ds, ok := s.daemonSets[id]; ok {
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
		case watcher <- watched:
		}
	}

	return ds, nil
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

			outgoingChanges := dsstore.WatchedDaemonSets{}
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
					default:
					}
				default:
				}
			}
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
