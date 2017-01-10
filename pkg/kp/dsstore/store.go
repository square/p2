package dsstore

import (
	"errors"
	"time"

	"k8s.io/kubernetes/pkg/labels"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/store"
)

const dsTree string = "daemon_sets"

var NoDaemonSet error = errors.New("No daemon set found")

type WatchedDaemonSets struct {
	Created []*store.DaemonSet
	Updated []*store.DaemonSet
	Deleted []*store.DaemonSet
	Err     error
}

type WatchedDaemonSetList struct {
	DaemonSets []store.DaemonSet
	Err        error
}

// Store represents an interface for persisting daemon set to Consul,
// as well as restoring daemon set from Consul.
type Store interface {
	// Create creates a daemon set with the specified manifest and selectors.
	// The node selector is used to determine what nodes the daemon set may schedule on.
	// The pod label set is applied to every pod the daemon set schedules.
	Create(
		manifest store.Manifest,
		minHealth int,
		name store.DaemonSetName,
		nodeSelector labels.Selector,
		podID store.PodID,
		timeout time.Duration,
	) (store.DaemonSet, error)

	// Gets a daemon set by ID, if it does not exist, it will produce an error
	Get(id store.DaemonSetID) (store.DaemonSet, *api.QueryMeta, error)
	List() ([]store.DaemonSet, error)

	// Deletes a daemon set by ID, deleting something that does not exist
	// should not produce an error
	Delete(store.DaemonSetID) error

	// Mutates a daemon set by ID, ID cannot be mutated,
	// PodID of the daemon set and its manifest must also match
	MutateDS(
		id store.DaemonSetID,
		mutator func(store.DaemonSet) (store.DaemonSet, error),
	) (store.DaemonSet, error)

	Watch(quit <-chan struct{}) <-chan WatchedDaemonSets

	// Returns a list of all the daemon sets that are on the tree
	WatchAll(quit <-chan struct{}, pauseTime time.Duration) <-chan WatchedDaemonSetList

	// Disables daemon set and produces logging, if there are problems disabling,
	// disable the daemon set, and if there are still problems, return a fatal error
	Disable(id store.DaemonSetID) (store.DaemonSet, error)

	// Locks an daemon set for ownership,
	LockForOwnership(rcID store.DaemonSetID, session kp.Session) (consulutil.Unlocker, error)
}

func IsNotExist(err error) bool {
	return err == NoDaemonSet
}
