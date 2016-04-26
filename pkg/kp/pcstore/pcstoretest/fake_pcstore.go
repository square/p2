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
	watchers    map[fields.ID]chan pcstore.WatchedPodCluster
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
	_ pcstore.Session,
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
	if watcher, ok := p.watchers[id]; ok {
		watcher <- pcstore.WatchedPodCluster{PodCluster: &pc, Err: nil}
	}
	return pc, nil
}

func (p *FakePCStore) Get(id fields.ID) (fields.PodCluster, error) {
	if pc, ok := p.podClusters[id]; ok {
		return pc, nil
	}

	return fields.PodCluster{}, pcstore.NoPodCluster
}

func (p *FakePCStore) Delete(id fields.ID) error {
	delete(p.podClusters, id)
	return nil
}

func (p *FakePCStore) FindWhereLabeled(
	podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName,
) ([]fields.PodCluster, error) {
	ret := []fields.PodCluster{}
	for _, pc := range p.podClusters {
		if availabilityZone == pc.AvailabilityZone &&
			clusterName == pc.Name &&
			podID == pc.PodID {
			ret = append(ret, pc)
		}
	}
	return ret, nil
}

func (p *FakePCStore) Watch(quit <-chan struct{}) <-chan pcstore.WatchedPodClusters {
	ret := make(chan pcstore.WatchedPodClusters)

	go func() {
		for {
			select {
			case <-quit:
				return
			default:
			}

			clusters := pcstore.WatchedPodClusters{}
			for _, ch := range p.watchers {
				select {
				case watched := <-ch:
					clusters.Clusters = append(clusters.Clusters, watched.PodCluster)
				default:
				}
			}

			select {
			case ret <- clusters:
			case <-quit:
				return
			}
		}
	}()

	return ret
}

func (p *FakePCStore) WatchPodCluster(id fields.ID, quit <-chan struct{}) <-chan pcstore.WatchedPodCluster {
	return p.watchers[id]
}
