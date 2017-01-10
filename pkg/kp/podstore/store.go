// TODO: update this comment as things change.
// The podstore package provides a Store interface interacting with the /pods
// tree. Prior to this package, this is done through the kp.Store interface.
// The kp package is a hodge-podge of a lot of functionality for interacting
// with consul, but is largely used to interface with the /intent and /reality
// trees. /intent and /reality work great for scheduling pod manifests that are
// meant to run on a node "forever" (meaning until another pod manifest is
// scheduled over it) but it doesn't work great for one-off jobs, or for
// reporting back success/failure information.
//
// The podstore package is the future of scheduling pods. It will write pods
// to the /pods tree with a uuid key name. In order to preserve the preparer's
// consul watching functionality, pods written to /pods will have a secondary
// reference in /intent at the normal location pointing to the /pods entry.
// The key in /intent will share the uuid of the /pods key it's referring to.
package podstore

import (
	"github.com/square/p2/pkg/store"
)

type Store interface {
	ReadPod(key store.PodUniqueKey) (Pod, error)
	ReadPodFromIndex(index PodIndex) (Pod, error)
	Schedule(manifest store.Manifest, node store.NodeName) (store.PodUniqueKey, error)
	Unschedule(key store.PodUniqueKey) error

	DeleteRealityIndex(podKey store.PodUniqueKey, node store.NodeName) error
	WriteRealityIndex(podKey store.PodUniqueKey, node store.NodeName) error
}
