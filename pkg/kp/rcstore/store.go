package rcstore

import (
	"errors"

	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
)

const rcTree string = "replication_controllers"

var NoReplicationController error = errors.New("No replication controller found")

// Store represents an interface for persisting replication controllers to Consul,
// as well as restoring replication controllers from Consul.
type Store interface {
	// Create creates a replication controller with the specified manifest and selectors.
	// The node selector is used to determine what nodes the replication controller may schedule on.
	// The pod label set is applied to every pod the replication controller schedules.
	Create(manifest pods.Manifest, nodeSelector labels.Selector, podLabels labels.Set) (fields.RC, error)

	Get(id fields.ID) (fields.RC, error)
	List() ([]fields.RC, error)

	// Watch for changes to the entire store and output the current list of RCs
	// any time it changes. This function blocks; close the quit channel to
	// terminate. Callers must drain the returned channels, including the error
	// channel.
	WatchNew(quit <-chan struct{}) (<-chan []fields.RC, <-chan error)

	// Set the desired replica count for the given RC to the given integer.
	SetDesiredReplicas(fields.ID, int) error
	// Add the given integer to the given RC's replica count (bounding at zero).
	AddDesiredReplicas(fields.ID, int) error

	// Set the RC to be enabled. An enabled RC will attempt to meet its desires.
	Enable(fields.ID) error
	// Set the RC to be disabled. A disabled RC will not attempt to meet its desires.
	Disable(fields.ID) error

	// Deletes the targeted RC, returning an error if it does not exist.
	// Normally an RC can only be deleted if its desired replica count is zero;
	// pass force=true to override this check.
	Delete(fields.ID, bool) error

	// Watch(rc, quitChannel) watches for any changes to the replication controller `rc`.
	// This returns two output channels.
	// A `struct{}` is sent on the first output channel whenever a change has occurred.
	// At that time, the replication controller will have been updated in place.
	// Errors are sent on the second output channel.
	// Send a value on `quitChannel` to stop watching.
	// The two output channels will be closed in response.
	Watch(rc *fields.RC, quit <-chan struct{}) (<-chan struct{}, <-chan error)

	// Locks an RC for ownership, e.g. by a process carrying out the intent
	// of a replication controller by ensuring that ReplicasDesired copies
	// of Manifest are running
	LockForOwnership(rcID fields.ID, session kp.Session) (kp.Unlocker, error)

	// Locks an RC for mutation, e.g. by a running rolling update or any
	// tool making a change
	LockForMutation(rcID fields.ID, session kp.Session) (kp.Unlocker, error)

	// Locks an RC for the purposes of RU creation. We want to guarantee
	// that no two RUs are admitted that operate on the same RCs, which is
	// accomplished by locking them first
	LockForUpdateCreation(rcID fields.ID, session kp.Session) (kp.Unlocker, error)
}

func IsNotExist(err error) bool {
	return err == NoReplicationController
}
