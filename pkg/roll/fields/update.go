package fields

import (
	"github.com/square/p2/pkg/rc/fields"
)

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
	// considered to be complete. This should be at least equal to the minimum.
	DesiredReplicas int
	// During the course of the update, pods may be restarted or removed. This
	// number specifies the minimum number of replicas that must be alive
	// during the entire Update.
	//
	// The acceptable value of Minimum is subject to several constraints:
	//   - the update will not even start until the minimum is satisfied, so if
	//     the current number of live nodes is than the minimum, it will block
	//     forever.
	//   - the rollout algorithm assumes that scheduling a new pod may halt an
	//     old one. For example, launchables based on hoist artifacts have this
	//     behavior. Therefore, if the current number of live nodes is the same
	//     as the minimum, the update will block. It cannot risk scheduling a
	//     new pod because it might evict an old one and go below the minimum.
	// Therefore, the minimum replicas must ALWAYS be less than the total number
	// of live nodes when the update starts. Enforcing this constraint is the
	// responsibility of the backend that executes Updates.
	MinimumReplicas int
	// If DeletePods is set to true, the update will explicitly lower the
	// replica count of the old RC before raising the new one. This causes the
	// old RC to delete some of its pods. If set to false, the old RC will be
	// disabled until the update is finished.
	//
	// DeletePods=false is a good choice when scheduling a new pod on a node will
	// also stop the old one (eg hoist artifacts). In that case, deleting the
	// old pod is not necessary and may cause unnecessary slowdowns. Using
	// DeletePods=true is more suited to pods that are independent of each
	// other (eg linux containers), where it is desired to remove the old pods
	// throughout the rollout.
	//
	// Be certain that the delete command cannot delete a newly scheduled pod.
	// Otherwise, if DeletePods=true, the delete issued by the old RC may race
	// with the scheduling issued by the new RC. In these situations, setting
	// DeletePods=false is the correct choice.
	//
	// Regardless of the value of DeletePods, the old RC and all its pods will
	// be removed when the update is complete.
	DeletePods bool
	// If LeaveOld is set to true, the update will not delete the old RC when
	// complete. Instead, the old RC will be left in whatever state it is in
	// when the update ends. This is useful if, for example, you want to perform
	// a partial rollout using one update, and leave the old RC so that another
	// update can be created to finish the rollout.
	LeaveOld bool
}
