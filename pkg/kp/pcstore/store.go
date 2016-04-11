package pcstore

import (
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"

	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

const podClusterTree string = "pod_clusters"

type Store interface {
	Create(
		podId types.PodID,
		availabilityZone fields.AvailabilityZone,
		clusterName fields.ClusterName,
		podSelector klabels.Selector,
		annotations fields.Annotations,
	) (fields.PodCluster, error)
	Get(id fields.ID) (fields.PodCluster, error)
}
