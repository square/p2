package rollstore

import (
	"time"

	"github.com/square/p2/pkg/pods"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"

	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

// Store persists Updates into Consul. Updates are uniquely identified by their
// new RC's ID.
// NOTE: the interface is cluttered with methods for creating rolling updates
// for various situations. A generic solution (e.g. Create(req
// RollingUpdateRequest)) was explored and ultimately deemed too difficult to
// understand all of the involved situations and the ways they would behave due
// to the different locking needs of each scenario
type Store interface {
	// retrieve this Update
	Get(fields.ID) (fields.Update, error)
	// retrieve all updates
	List() ([]fields.Update, error)
	// Creates a rolling update from two existing RCs. Will check that the
	// RCs actually exist before applying the update, and acquire locks on
	// them in a deterministic order to guarantee that no two RUs will
	// operate on the same RC and will avoid deadlock scenarios. Before
	// the RU is created, newRCLabels will be applied to the new RC
	CreateRollingUpdateFromExistingRCs(update fields.Update, newRCLabels klabels.Set) (fields.Update, error)
	// Creates a rolling update using an existing RC with a known ID as the
	// old replication controller, creates the new replication controller,
	// and labels the new replication controller according to newRCLabels.
	CreateRollingUpdateFromOneExistingRCWithID(
		oldRCID rc_fields.ID,
		desiredReplicas int,
		minimumReplicas int,
		leaveOld bool,
		rollDelay time.Duration,
		newRCManifest pods.Manifest,
		newRCNodeSelector klabels.Selector,
		newRCPodLabels klabels.Set,
		newRCLabels klabels.Set,
	) (fields.Update, error)
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
		newRCManifest pods.Manifest,
		newRCNodeSelector klabels.Selector,
		newRCPodLabels klabels.Set,
		newRCLabels klabels.Set,
	) (fields.Update, error)
	// delete this Update from the store
	Delete(fields.ID) error
	// take a lock on this ID. Before taking ownership of an Update, its new RC
	// ID, and old RC ID if any, should both be locked. If the error return is
	// nil, then the boolean indicates whether the lock was successfully taken.
	Lock(fields.ID, string) (bool, error)
	// Watch for changes to the store and generate a list of Updates for each
	// change. This function does not block.
	Watch(<-chan struct{}) (<-chan []fields.Update, <-chan error)
}
