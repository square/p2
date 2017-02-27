package podstoretest

import (
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

// FailingPodStore partially implements the pkg/store/consul/podstore.Store
// interface, but all of its operations return errors. This is useful for
// testing the behavior of code that interacts with the pod store in failure
// scenarios
type FailingPodStore struct {
}

func NewFailingPodStore() FailingPodStore {
	return FailingPodStore{}
}

func (FailingPodStore) Schedule(manifest.Manifest, types.NodeName) (types.PodUniqueKey, error) {
	return "", util.Errorf("failing pod store failed to schedule pod")
}

func (FailingPodStore) Unschedule(types.PodUniqueKey) error {
	return util.Errorf("failing pod store failed to unschedule pod")
}
