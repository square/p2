/*
Package control wraps a pcstore with convenience functions suitable for operational tasks.
*/
package control

import (
	"github.com/square/p2/pkg/pc/fields"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/pcstore"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	"k8s.io/kubernetes/pkg/labels"
)

type PodClusterStore interface {
	Get(id fields.ID) (fields.PodCluster, error)
	FindWhereLabeled(
		podID types.PodID,
		availabilityZone fields.AvailabilityZone,
		clusterName fields.ClusterName,
	) ([]fields.PodCluster, error)
	Delete(id fields.ID) error
	Create(
		podID types.PodID,
		availabilityZone fields.AvailabilityZone,
		clusterName fields.ClusterName,
		podSelector labels.Selector,
		annotations fields.Annotations,
		allocationStrategy rc_fields.Strategy,
		session pcstore.Session,
	) (fields.PodCluster, error)
	MutatePC(
		id fields.ID,
		mutator func(fields.PodCluster) (fields.PodCluster, error),
	) (fields.PodCluster, error)
}

type PodCluster struct {
	pcStore PodClusterStore

	ID fields.ID

	az       fields.AvailabilityZone
	cn       fields.ClusterName
	podID    types.PodID
	selector labels.Selector
	strategy rc_fields.Strategy
}

func NewPodCluster(
	az fields.AvailabilityZone,
	cn fields.ClusterName,
	podID types.PodID,
	pcstore PodClusterStore,
	selector labels.Selector,
	strategy rc_fields.Strategy,
) *PodCluster {

	pc := &PodCluster{}
	pc.az = az
	pc.cn = cn
	pc.podID = podID
	pc.pcStore = pcstore
	pc.selector = selector
	pc.strategy = strategy

	return pc
}

func NewPodClusterFromID(
	id fields.ID,
	pcStore PodClusterStore,
) *PodCluster {
	pc := &PodCluster{}
	pc.pcStore = pcStore
	pc.ID = id
	return pc
}

func (pccontrol *PodCluster) All() ([]fields.PodCluster, error) {
	if pccontrol.ID != "" {
		pc, err := pccontrol.pcStore.Get(pccontrol.ID)
		if err != nil {
			return nil, err
		}
		return []fields.PodCluster{pc}, nil
	}
	return pccontrol.pcStore.FindWhereLabeled(pccontrol.podID, pccontrol.az, pccontrol.cn)
}

// Best effort delete of the list of podClusterID will not halt on error
func (pccontrol *PodCluster) Delete() (errors []error) {
	podClusterIDs, err := pccontrol.All()
	if err != nil {
		return []error{err}
	}

	for _, pc := range podClusterIDs {
		if err := pccontrol.pcStore.Delete(pc.ID); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func (pccontrol *PodCluster) Create(annotations fields.Annotations, session pcstore.Session) (fields.PodCluster, error) {
	return pccontrol.pcStore.Create(pccontrol.podID, pccontrol.az, pccontrol.cn, pccontrol.selector, annotations, pccontrol.strategy, session)
}

func (pccontrol *PodCluster) Get() (fields.PodCluster, error) {
	pc, err := pccontrol.getExactlyOne()
	if err != nil {
		return fields.PodCluster{}, err
	}
	return pccontrol.pcStore.Get(pc.ID)
}

// UpdateAnnotations replaces the annotations on the pod cluster configured for the pod cluster control structure
func (pccontrol *PodCluster) UpdateAnnotations(annotations fields.Annotations) (fields.PodCluster, error) {
	pc, err := pccontrol.getExactlyOne()
	if err != nil {
		return fields.PodCluster{}, err
	}

	annotationsUpdater := func(pc fields.PodCluster) (fields.PodCluster, error) {
		pc.Annotations = annotations
		return pc, nil
	}

	return pccontrol.pcStore.MutatePC(pc.ID, annotationsUpdater)
}

func (pccontrol *PodCluster) getExactlyOne() (fields.PodCluster, error) {
	labeledPCs, err := pccontrol.All()
	if err != nil {
		return fields.PodCluster{}, err
	}
	if len(labeledPCs) > 1 {
		return fields.PodCluster{}, util.Errorf("More than one PC matches this PodCluster %+v, please be more specific", pccontrol)
	}
	if len(labeledPCs) == 0 {
		return fields.PodCluster{}, util.Errorf("Found no matching PodClusters, please check the labels: %+v", pccontrol)
	}

	return labeledPCs[0], nil
}
