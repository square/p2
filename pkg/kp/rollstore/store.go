package rollstore

import (
	rcf "github.com/square/p2/pkg/rc/fields"
	rollf "github.com/square/p2/pkg/roll/fields"
)

// Store persists Updates into Consul. Updates are uniquely identified by their
// new RC's ID.
type Store interface {
	// retrieve this Update
	Get(rcf.ID) (rollf.Update, error)
	// retrieve all updates
	List() ([]rollf.Update, error)
	// put this Update into the store. Updates are immutable - if another Update
	// exists with this newRC ID, an error is returned
	Put(rollf.Update) error
	// delete this Update from the store
	Delete(rcf.ID) error
	// take a lock on this ID. Before taking ownership of an Update, its new RC
	// ID, and old RC ID if any, should both be locked. If the error return is
	// nil, then the boolean indicates whether the lock was successfully taken.
	Lock(rcf.ID, string) (bool, error)
	// Watch for changes to the store and generate a list of Updates for each
	// change. This function does not block.
	Watch(<-chan struct{}) (<-chan []rollf.Update, <-chan error)
}
