package rollstore

import (
	"errors"
	"time"

	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store"

	klabels "k8s.io/kubernetes/pkg/labels"
)

var AmbiguousRCSelector error = errors.New("The old RC selector was ambigous and produced > 1 matches")

func IsAmbiguousRCSelector(err error) bool {
	return err == AmbiguousRCSelector
}

// Store persists Updates into Consul. Updates are uniquely identified by their
// new RC's ID.
// NOTE: the interface is cluttered with methods for creating rolling updates
// for various situations. A generic solution (e.g. Create(req
// RollingUpdateRequest)) was explored and ultimately deemed too difficult to
// understand all of the involved situations and the ways they would behave due
// to the different locking needs of each scenario
type Store interface {
	// retrieve this Update
	Get(store.RollingUpdateID) (store.RollingUpdate, error)
	// retrieve all updates
	List() ([]store.RollingUpdate, error)
	// DEPRECATED: creates a rollstore with no guarantees about atomic
	// creation of RCs. It's easy to generate inconsistent data when using
	// this
	Put(u store.RollingUpdate) error
	// Creates a rolling update from two existing RCs. Will check that the
	// RCs actually exist before applying the update, and acquire locks on
	// them in a deterministic order to guarantee that no two RUs will
	// operate on the same RC and will avoid deadlock scenarios. Before
	// the RU is created, newRCLabels will be applied to the new RC
	CreateRollingUpdateFromExistingRCs(
		update store.RollingUpdate,
		newRCLabels klabels.Set,
		rollLabels klabels.Set,
	) (store.RollingUpdate, error)
	// Creates a rolling update using an existing RC with a known ID as the
	// old replication controller, creates the new replication controller,
	// and labels the new replication controller according to newRCLabels.
	CreateRollingUpdateFromOneExistingRCWithID(
		oldRCID store.ReplicationControllerID,
		desiredReplicas int,
		minimumReplicas int,
		leaveOld bool,
		rollDelay time.Duration,
		newRCManifest manifest.Manifest,
		newRCNodeSelector klabels.Selector,
		newRCPodLabels klabels.Set,
		newRCLabels klabels.Set,
		rollLabels klabels.Set,
	) (store.RollingUpdate, error)
	// Creates a rolling update using a label selector to identify the old
	// replication controller to be used.  If one does not exist, one will
	// be created to serve as a "dummy" old replication controller. The new
	// replication controller will be created and have newRCLabels applied
	// to it.
	CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
		oldRCSelector klabels.Selector,
		desiredReplicas int,
		minimumReplicas int,
		leaveOld bool,
		rollDelay time.Duration,
		newRCManifest manifest.Manifest,
		newRCNodeSelector klabels.Selector,
		newRCPodLabels klabels.Set,
		newRCLabels klabels.Set,
		rollLabels klabels.Set,
	) (store.RollingUpdate, error)
	// delete this Update from the store
	Delete(store.RollingUpdateID) error
	// take a lock on this ID. Before taking ownership of an Update, its new RC
	// ID, and old RC ID if any, should both be locked. If the error return is
	// nil, then the boolean indicates whether the lock was successfully taken.
	Lock(store.RollingUpdateID, string) (bool, error)
	// Watch for changes to the store and generate a list of Updates for each
	// change. This function does not block.
	Watch(<-chan struct{}) (<-chan []store.RollingUpdate, <-chan error)
}
