package fields

import (
	"time"

	"github.com/square/p2/pkg/rc/fields"
)

type ID string

// An Update (roll.Update, borrowed from kubectl's "rolling-update" command)
// represents a transition from one replication controller to another. The IDs
// represent the two RCs involved in the transition.
type Update struct {
	// The old replication controller that will be releasing control. At the end
	// of the Update, this RC will be deleted.
	OldRC fields.ID
	// Thew new replication controller that will be taking over. Updates can be
	// uniquely identified by their new RC's ID.
	NewRC fields.ID
	// When the new RC's replica count equals this number, the Update is
	// considered to be complete.
	DesiredReplicas int
	// During the course of the update, pods may be restarted or removed. This
	// number specifies the minimum number of replicas that must be alive
	// during the entire Update.
	//
	// The acceptable value of Minimum is subject to several constraints:
	//   - the update will not even start until the minimum is satisfied, so if
	//     the current number of live nodes is less than the minimum, it will
	//     block forever.
	//   - the rollout algorithm assumes that scheduling a new pod may halt an
	//     old one. For example, launchables based on hoist artifacts have this
	//     behavior. Therefore, if the current number of live nodes is the same
	//     as the minimum, the update will block. It cannot risk scheduling a
	//     new pod because it might evict an old one and go below the minimum.
	// Therefore, the minimum replicas must ALWAYS be less than the total number
	// of live nodes when the update starts. Enforcing this constraint is the
	// responsibility of the backend that executes Updates.
	MinimumReplicas int
	// If LeaveOld is set to true, the update will not delete the old RC when
	// complete. Instead, the old RC will be left in whatever state it is in
	// when the update ends. This is useful if, for example, you want to perform
	// a partial rollout using one update, and leave the old RC so that another
	// update can be created to finish the rollout.
	LeaveOld bool

	// RollDelay adds a period of mandatory sleep between rolling updates to the
	// next set of nodes. This can be useful for creating a buffer period between
	// a pod instance becoming healthy and clients to that instance resuming
	// normal operation. In some pathological cases, applications may become
	// unhealthy after being healthy for a short duration. Naive implementations like
	// p2-replicate do not handle such after-the-fact unhealthiness. Default is 0.
	RollDelay time.Duration
}

// Implementation detail: a rolling updates ID matches that of it's NewRC. We may
// change this at some point
func (u Update) ID() ID {
	return ID(u.NewRC)
}

func (id ID) String() string {
	return string(id)
}
