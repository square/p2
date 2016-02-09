package rcstore

import (
	"path"

	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/util"
)

const rcTree string = "replication_controllers"

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

	Enable(fields.ID) error
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
}

func rcPath(rcId fields.ID) (string, error) {
	if rcId == "" {
		return "", util.Errorf("rcId not specified when computing rc path")
	}
	return path.Join(rcTree, string(rcId)), nil
}

func RCLockPath(rcId fields.ID) (string, error) {
	subRCPath, err := rcPath(rcId)
	if err != nil {
		return "", err
	}

	return path.Join(kp.LOCK_TREE, subRCPath), nil
}

// Replication controllers have potential for two locks:
// 1) the key itself, held by a replication controller goroutine that is
// responsible for making sure its desires are met
// 2) an "update" key under the replication controller key, held by the
// goroutine running a rolling update that will mutate a replication controller
func RCUpdateLockPath(rcId fields.ID) (string, error) {
	rcPath, err := rcPath(rcId)
	if err != nil {
		return "", err
	}

	return path.Join(kp.LOCK_TREE, rcPath, "update"), nil
}
