package store

import (
	"github.com/square/p2/pkg/logging"
)

// ManifestLocation stores a manifest and the node it came from.
type ManifestLocation struct {
	Node     string
	Manifest Manifest
}

// PodStore stores pod manifests attached to a node.
type PodStore interface {
	// Get retrieves one pod from a node.
	Get(node string, pod PodID) (Manifest, error)

	// Put saves one manifest to a node.
	Put(node string, manifest Manifest) error

	// Delete removes a pod from a node.
	Delete(node string, pod PodID) error

	// GetNode retrieves all pods from a node.
	GetNode(node string) ([]Manifest, error)

	// All retrieves all pods from all nodes.
	All() ([]ManifestLocation, error)

	// Watch watches one pod for changes. Whenever the pod is changed (added, modified, or
	// deleted), the pod's new value will be sent to the given channel. This method runs
	// until explicitly canceled by closing the done channel.
	Watch(
		done <-chan struct{},
		logger logging.Logger,
		podValues chan<- Manifest,
		node string,
		pod PodID,
	)

	// WatchNode watches a node for changes to any of its pods. Whenever anything changes,
	// all of the node's manifests will be sent to the given channel. This method runs
	// until explicitly canceled by closing the done channel.
	WatchNode(
		done <-chan struct{},
		logger logging.Logger,
		podValues chan<- []Manifest,
		node string,
	)

	// Lock acquires a lock on the given pod.
	Lock(manager LockManager, node string, pod PodID) (Unlocker, error)
}

// HealthUpdater allows a single pod's health to be updated.
type HealthUpdater interface {
	// PutHealth updates the health status of the pod. Checkers are free to call it after
	// every health check or other status change.
	Put(check *HealthCheck) error

	// Close removes a pod's health check and releases all updater resources. Call this
	// when no more health statuses will be published.
	Close()
}

// HealthManager manages a collection of health checks that share configuration and
// resources.
type HealthManager interface {
	// NewUpdater creates a new object for publishing a single pod's health. Each
	// pod should have its own updater.
	NewUpdater(pod PodID) HealthUpdater

	// Close removes all published health statuses and releases all manager resources.
	Close()
}

// HealthStore stores health checks.
type HealthStore interface {
	Get(node string, pod PodID) (*HealthCheck, error)
	Put(check *HealthCheck) error
	GetPod(pod PodID) (map[string]*HealthCheck, error)

	NewManager(node string, logger logging.Logger) HealthManager
}

type ReplicationControllerStore interface {
	Get(id ReplicationControllerID) (*ReplicationController, error)
	Put(controller *ReplicationController) error
	All() ([]*ReplicationController, error)

	// AtomicModify allows an existing replication controller to be atomically modified.
	// The caller should provide a function that accepts a controller and changes it to
	// the desired state. The new controller state will be atomically written back to the
	// store. The caller's function may be called multiple times if there is contention.
	//
	// NOTE: Should this be removed, since modifications should only happen while a lock
	// is held? Do we really want to support unlocked atomic operations on an RC while
	// something else holds its modify lock?
	AtomicModify(id ReplicationControllerID, f func(*ReplicationController) error) error

	// Watch a single replication controller for any changes. This method runs until
	// explicitly canceled by closing the done channel. New values of the controller will
	// be sent to the given channel.
	Watch(
		done <-chan struct{},
		logger logging.Logger,
		controllers chan<- *ReplicationController,
		id ReplicationControllerID,
	)

	// WatchAll watches all replication controllers for any changes. This method runs
	// until explicitly canceled by closing the done channel. When any controller changes,
	// all controllers will be sent to the given channel. WARNING: leaky abstraction.
	WatchAll(
		done <-chan struct{},
		logger logging.Logger,
		controllers chan<- []*ReplicationController,
	)

	// LockOwner acquires the ownership lock on a replication controller. This is used for
	// determining which instance of a farm is responsible for monitoring the controller
	// for changes.
	LockOwner(id ReplicationControllerID) (Unlocker, error)

	// LockModify acquires the modify lock on a replication controller. This lock should
	// be acquired whenever a process needs to change the controller.
	LockModify(id ReplicationControllerID) (Unlocker, error)
}

type RollingUpdateStore interface {
	Get(id ReplicationControllerID) (*RollingUpdate, error)
	Put(update *RollingUpdate) error
	Delete(id ReplicationControllerID) error

	All() ([]*RollingUpdate, error)

	WatchAll(
		done <-chan struct{},
		logger logging.Logger,
		updates chan<- []*RollingUpdate,
	)

	// LockOwner acquires the ownership lock on the rolling update. This is used for
	// determining which instance of a farm is responsible for monitoring the update for
	// deletion.
	LockOwner(id ReplicationControllerID) (Unlocker, error)

	// LockRCs acquires locks on the two replication controllers involved in creating a
	// new update.
	LockRCs(from, to ReplicationControllerID) (Unlocker, error)
}

// Unlocker instances are returned when a lock is successfully acquired. Their Unlock()
// method should be called to release the lock.
type Unlocker interface {
	Unlock() error
}

// Returned by Lock() methods when the lock is already held by another LockManager.
type AlreadyLockedError interface {
	AlreadyLocked() // Has no effect. It exists to create a distinct interface type.
	error
}

// A LockManager controls how locks are acquired and released. Methods to acquire and
// release locks are found in the stores that contain objects that can be locked.
type LockManager interface {
	// When a lock cannot be acquired because the object is already locked, this method
	// returns the name of the LockManager that holds the lock.
	LockHolder(err AlreadyLockedError) (string, error)

	// DestroyLockHolder destroys the LockManager that holds the lock. WARNING: leaky
	// abstraction.
	DestroyLockHolder(err AlreadyLockedError) error

	// Close destroys this LockManager and releases all the locks it holds. WARNING: leaky
	// abstraction.
	Close() error
}

// Store defines the main interface that P2 uses to store data.
type Store interface {
	Reality() PodStore
	Intent() PodStore
	Health() HealthStore
	ReplicationControllers() ReplicationControllerStore
	RollingUpdates() RollingUpdateStore

	// NewLockManager creates a LockManager
	NewLockManager(name string) (LockManager, error)

	// NewSharedLockManager creates a new LockManager that uses a shared Consul session.
	// WARNING: leaky abstraction.
	NewSessionLockManager(name string, session string) (LockManager, error)

	// Ping checks for basic connectivity to the storage server. A nil error should
	// indicate that the service was available at the time.
	Ping() error
}
