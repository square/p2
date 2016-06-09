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

// Used for unit testing
type FakeDSStore struct {
	daemonSets map[fields.ID]fields.DaemonSet
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
	return ds, nil
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
