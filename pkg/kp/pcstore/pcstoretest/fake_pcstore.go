package pcstoretest

import (
	"fmt"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/pcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/types"

	"github.com/pborman/uuid"
	klabels "k8s.io/kubernetes/pkg/labels"
)

// Implementation of the pcstore.Store interface that can be used for unit
// testing
type FakePCStore struct {
	podClusters map[store.PodClusterID]store.PodCluster
	watchers    map[store.PodClusterID]chan pcstore.WatchedPodCluster
}

var _ pcstore.Store = &FakePCStore{}

func NewFake() *FakePCStore {
	return &FakePCStore{
		podClusters: make(map[store.PodClusterID]store.PodCluster),
	}
}

func (p *FakePCStore) Create(
	podID types.PodID,
	availabilityZone store.AvailabilityZone,
	clusterName store.ClusterName,
	podSelector klabels.Selector,
	annotations store.Annotations,
	_ pcstore.Session,
) (store.PodCluster, error) {
	id := store.PodClusterID(uuid.New())
	pc := store.PodCluster{
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

func (p *FakePCStore) Get(id store.PodClusterID) (store.PodCluster, error) {
	if pc, ok := p.podClusters[id]; ok {
		return pc, nil
	}

	return store.PodCluster{}, pcstore.NoPodCluster
}

func (p *FakePCStore) Delete(id store.PodClusterID) error {
	delete(p.podClusters, id)
	return nil
}

func (p *FakePCStore) List() ([]store.PodCluster, error) {
	var ret []store.PodCluster
	for _, pc := range p.podClusters {
		ret = append(ret, pc)
	}
	return ret, nil
}

func (p *FakePCStore) MutatePC(
	id store.PodClusterID,
	mutator func(store.PodCluster) (store.PodCluster, error),
) (store.PodCluster, error) {
	pc, err := p.Get(id)
	if err != nil {
		return store.PodCluster{}, err
	}

	pc, err = mutator(pc)
	if err != nil {
		return store.PodCluster{}, err
	}

	p.podClusters[id] = pc
	if watcher, ok := p.watchers[id]; ok {
		// In case the user mutates more than once, this prevents a deadlock
		// while keeping the functionality of the fake watch
		select {
		case <-watcher:
			watcher <- pcstore.WatchedPodCluster{PodCluster: &pc, Err: nil}
		case watcher <- pcstore.WatchedPodCluster{PodCluster: &pc, Err: nil}:
		}
	}

	return pc, nil
}

func (p *FakePCStore) FindWhereLabeled(
	podID types.PodID,
	availabilityZone store.AvailabilityZone,
	clusterName store.ClusterName,
) ([]store.PodCluster, error) {
	ret := []store.PodCluster{}
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

func (p *FakePCStore) WatchAndSync(concrete pcstore.ConcreteSyncer, quit <-chan struct{}) error {
	pods := p.Watch(quit)

	for {
		select {
		case <-quit:
			return nil
		case watched := <-pods:
			for _, cluster := range watched.Clusters {
				_ = concrete.SyncCluster(cluster, []labels.Labeled{})
			}
		}
	}
}

func (p *FakePCStore) WatchPodCluster(id store.PodClusterID, quit <-chan struct{}) <-chan pcstore.WatchedPodCluster {
	return p.watchers[id]
}

func (p *FakePCStore) LockForSync(id store.PodClusterID, syncerType pcstore.ConcreteSyncerType, session pcstore.Session) (consulutil.Unlocker, error) {
	key := fmt.Sprintf("%s/%s", id, syncerType)
	return session.Lock(key)
}

func (p *FakePCStore) SetMetricsRegistry(_ pcstore.MetricsRegistry) {
}
