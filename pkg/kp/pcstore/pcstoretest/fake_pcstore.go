package pcstoretest

import (
	"github.com/square/p2/pkg/kp/pcstore"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"

	"github.com/square/p2/Godeps/_workspace/src/github.com/pborman/uuid"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

// Implementation of the pcstore.Store interface that can be used for unit
// testing
type FakePCStore struct {
	podClusters map[fields.ID]fields.PodCluster
}

var _ pcstore.Store = &FakePCStore{}

func NewFake() *FakePCStore {
	return &FakePCStore{
		podClusters: make(map[fields.ID]fields.PodCluster),
	}
}

func (p *FakePCStore) Create(
	podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName,
	podSelector klabels.Selector,
	annotations fields.Annotations,
) (fields.PodCluster, error) {
	id := fields.ID(uuid.New())
	pc := fields.PodCluster{
		ID:               id,
		PodID:            podID,
		AvailabilityZone: availabilityZone,
		Name:             clusterName,
		PodSelector:      podSelector,
		Annotations:      annotations,
	}

	p.podClusters[id] = pc
	return pc, nil
}

func (p *FakePCStore) Get(id fields.ID) (fields.PodCluster, error) {
	if pc, ok := p.podClusters[id]; ok {
		return pc, nil
	}

	return fields.PodCluster{}, pcstore.NoPodCluster
}
