package pcstore

import (
	"errors"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"

	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

const podClusterTree string = "pod_clusters"

var NoPodCluster error = errors.New("No pod cluster found")

type Session interface {
	Lock(key string) (consulutil.Unlocker, error)
}

// WatchedPodCluster is an Either type: it will have 1 one of pc xor err
type WatchedPodCluster struct {
	PodCluster *fields.PodCluster
	Err        error
}

type WatchedPodClusters struct {
	Clusters []*fields.PodCluster
	Err      error
}

type Store interface {
	Create(
		podID types.PodID,
		availabilityZone fields.AvailabilityZone,
		clusterName fields.ClusterName,
		podSelector klabels.Selector,
		annotations fields.Annotations,
		session Session,
	) (fields.PodCluster, error)
	Get(id fields.ID) (fields.PodCluster, error)
	// Although pod clusters should always be unique for this 3-ple, this method
	// will return a slice in cases where duplicates are discovered. It is up to
	// clients to decide how to respond to such situations.
	FindWhereLabeled(
		podID types.PodID,
		availabilityZone fields.AvailabilityZone,
		clusterName fields.ClusterName,
	) ([]fields.PodCluster, error)
	Delete(id fields.ID) error
	WatchPodCluster(id fields.ID, quit <-chan struct{}) <-chan WatchedPodCluster
	Watch(quit <-chan struct{}) <-chan WatchedPodClusters

	// A convenience method that handles watching pod clusters
	// as well as the labeled pods in each pod cluster.
	WatchAndSync(syncer ConcreteSyncer, quit <-chan struct{}) error
	LockForSync(id fields.ID, syncerType ConcreteSyncerType, session Session) (consulutil.Unlocker, error)
}

// There may be multiple implementations of ConcreteSyncer that are interested
// in pod cluster updates, and wish to acquire a pod cluster lock to guarantee
// exclusive right to sync an update. ConcreteSyncerType is used to namespace a
// lock by implementation type so that two different concrete syncer
// implementations may sync the same pod cluster at the same time
type ConcreteSyncerType string

func (t ConcreteSyncerType) String() string { return string(t) }

type ConcreteSyncer interface {
	SyncCluster(pc *fields.PodCluster, pods []labels.Labeled) error
	DeleteCluster(pc *fields.PodCluster) error
}

func IsNotExist(err error) bool {
	return err == NoPodCluster
}
