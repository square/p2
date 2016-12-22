package pcstore

import (
	"errors"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/store"

	klabels "k8s.io/kubernetes/pkg/labels"
)

const podClusterTree string = "pod_clusters"

var (
	NoPodCluster            error = errors.New("No pod cluster found")
	PodClusterAlreadyExists error = errors.New("Pod cluster already exists")
)

type Session interface {
	Lock(key string) (consulutil.Unlocker, error)
}

// WatchedPodCluster is an Either type: it will have 1 one of pc xor err
type WatchedPodCluster struct {
	PodCluster *store.PodCluster
	Err        error
}

type WatchedPodClusters struct {
	Clusters []*store.PodCluster
	Err      error
}

// Subset of metrics.Registry interface
type MetricsRegistry interface {
	Get(metricName string) interface{}
	Register(metricName string, metric interface{}) error
}

type Store interface {
	Create(
		podID store.PodID,
		availabilityZone store.AvailabilityZone,
		clusterName store.ClusterName,
		podSelector klabels.Selector,
		annotations store.Annotations,
		session Session,
	) (store.PodCluster, error)
	Get(id store.PodClusterID) (store.PodCluster, error)
	List() ([]store.PodCluster, error)
	// Although pod clusters should always be unique for this 3-ple, this method
	// will return a slice in cases where duplicates are discovered. It is up to
	// clients to decide how to respond to such situations.
	FindWhereLabeled(
		podID store.PodID,
		availabilityZone store.AvailabilityZone,
		clusterName store.ClusterName,
	) ([]store.PodCluster, error)
	Delete(id store.PodClusterID) error
	MutatePC(
		id store.PodClusterID,
		mutator func(store.PodCluster) (store.PodCluster, error),
	) (store.PodCluster, error)
	WatchPodCluster(id store.PodClusterID, quit <-chan struct{}) <-chan WatchedPodCluster
	Watch(quit <-chan struct{}) <-chan WatchedPodClusters

	// A convenience method that handles watching pod clusters as well as the
	// labeled pods in each pod cluster. See the ConcreteSyncer interface for
	// details on how to use this function
	WatchAndSync(syncer ConcreteSyncer, quit <-chan struct{}) error
	LockForSync(id store.PodClusterID, syncerType ConcreteSyncerType, session Session) (consulutil.Unlocker, error)

	SetMetricsRegistry(reg MetricsRegistry)
}

// There may be multiple implementations of ConcreteSyncer that are interested
// in pod cluster updates, and wish to acquire a pod cluster lock to guarantee
// exclusive right to sync an update. ConcreteSyncerType is used to namespace a
// lock by implementation type so that two different concrete syncer
// implementations may sync the same pod cluster at the same time
type ConcreteSyncerType string

func (t ConcreteSyncerType) String() string { return string(t) }

type ConcreteSyncer interface {
	// SyncCluster implements a concrete synchronization of the pod cluster to
	// some real world implementation of a load balancer, credential management
	// system, or service discovery implementation.
	//
	// When a ConcreteSyncer is passed to WatchAndSync, SyncCluster is called
	// every time the set of labeled pods change, the pod cluster's metadata
	// changes, or when the long-lived watch on the pod cluster store returns.
	// This function is expected to be idempotent.
	//
	// SyncCluster will be called for every pod cluster present in the store.
	// If this function returns an error, SyncCluster will be called again later
	// with the same cluster and pods, assuming that no changes occur in the intervening
	// period.
	//
	// ConcreteSyncers will be called concurrently and must operate safely.
	SyncCluster(pc *store.PodCluster, pods []labels.Labeled) error

	// DeleteCluster is called when a pod cluster is observed to have been removed
	// from the store. DeleteCluster can be invoked in two circumstances: first,
	// if the cluster was present in a first watch, then absent in a subsequent watch,
	// then it is assumed it was deleted from the pcstore. Second, if the call
	// to GetInitialClusters() returns a pod cluster ID that is not present in the very
	// first watch result, DeleteCluster will be invoked with that pod cluster.
	//
	// If the passed ID is used to retrieve the pod cluster via store.Get(), it will
	// return ErrNoPodCluster. Clients should track any relevant metadata to the pod
	// cluster ID in the status store or in vendor-specific code.
	DeleteCluster(pc store.PodClusterID) error

	// GetInitialClusters is called at the beginning of the WatchAndSync
	// routine. See DeleteCluster() for an explanation of how its results are
	// used. If the function results in an error, the WatchAndSync function will
	// terminate immediately, forwarding the error.
	GetInitialClusters() ([]store.PodClusterID, error)

	// Used to derive the syncer type from a syncer instance for things
	// like namespacing of metrics
	Type() ConcreteSyncerType
}

func IsNotExist(err error) bool {
	return err == NoPodCluster
}

func IsAlreadyExists(err error) bool {
	return err == PodClusterAlreadyExists
}
