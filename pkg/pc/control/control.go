/*
Package control wraps a pcstore with convenience functions suitable for operational tasks.
*/
package control

import (
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/pcstore"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/util"
	"k8s.io/kubernetes/pkg/labels"
)

type PodCluster struct {
	pcStore pcstore.Store
	session kp.Session

	ID store.PodClusterID

	az       store.AvailabilityZone
	cn       store.PodClusterName
	podID    store.PodID
	selector labels.Selector
}

func NewPodCluster(
	az store.AvailabilityZone,
	cn store.PodClusterName,
	podID store.PodID,
	pcstore pcstore.Store,
	selector labels.Selector,
	session kp.Session,
) *PodCluster {

	pc := &PodCluster{}
	pc.az = az
	pc.cn = cn
	pc.podID = podID
	pc.pcStore = pcstore
	pc.selector = selector
	pc.session = session

	return pc
}

func NewPodClusterFromID(
	id store.PodClusterID,
	session kp.Session,
	pcStore pcstore.Store,
) *PodCluster {
	pc := &PodCluster{}
	pc.session = session
	pc.pcStore = pcStore
	pc.ID = id
	return pc
}

func (pccontrol *PodCluster) All() ([]store.PodCluster, error) {
	if pccontrol.ID != "" {
		pc, err := pccontrol.pcStore.Get(pccontrol.ID)
		if err != nil {
			return nil, err
		}
		return []store.PodCluster{pc}, nil
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

func (pccontrol *PodCluster) Create(annotations store.Annotations) (store.PodCluster, error) {
	return pccontrol.pcStore.Create(pccontrol.podID, pccontrol.az, pccontrol.cn, pccontrol.selector, annotations, pccontrol.session)
}

func (pccontrol *PodCluster) Get() (store.PodCluster, error) {
	pc, err := pccontrol.getExactlyOne()
	if err != nil {
		return store.PodCluster{}, err
	}
	return pccontrol.pcStore.Get(pc.ID)
}

func (pccontrol *PodCluster) Update(annotations store.Annotations) (store.PodCluster, error) {
	pc, err := pccontrol.getExactlyOne()
	if err != nil {
		return store.PodCluster{}, err
	}

	annotationsUpdater := func(pc store.PodCluster) (store.PodCluster, error) {
		pc.Annotations = annotations
		return pc, nil
	}

	return pccontrol.pcStore.MutatePC(pc.ID, annotationsUpdater)
}

func (pccontrol *PodCluster) getExactlyOne() (store.PodCluster, error) {
	labeledPCs, err := pccontrol.All()
	if err != nil {
		return store.PodCluster{}, err
	}
	if len(labeledPCs) > 1 {
		return store.PodCluster{}, util.Errorf("More than one PC matches this PodCluster %+v, please be more specific", pccontrol)
	}
	if len(labeledPCs) == 0 {
		return store.PodCluster{}, util.Errorf("Found no matching PodClusters, please check the labels: %+v", pccontrol)
	}

	return labeledPCs[0], nil
}
