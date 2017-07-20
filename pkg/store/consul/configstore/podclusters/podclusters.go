// podclusters contains code for interacting with pod cluster config in a convenient way

package podclusters

import (
	"context"

	"github.com/pborman/uuid"
	pfields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/store/consul/configstore"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type ConfigStore interface {
	FindWhereLabeled(klabels.Selector) ([]*configstore.Fields, error)
	PutConfigTxn(context.Context, configstore.Fields, *configstore.Version) error
	LabelConfig(configstore.ID, map[string]string) error
}

type PodClusterConfigStore struct {
	configStore ConfigStore
}

func (pccs *PodClusterConfigStore) CreateOrUpdateConfigForPodCluster(
	ctx context.Context,
	podID types.PodID,
	az pfields.AvailabilityZone,
	cn pfields.ClusterName,
	fields configstore.Fields,
	version *configstore.Version,
) (*configstore.Fields, error) {
	sel := klabels.Everything().
		Add(pfields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(pfields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(pfields.ClusterNameLabel, klabels.EqualsOperator, []string{cn.String()})

		// TODO: grab a per-pod-cluster lock here to make sure two concurrent writes don't result in two configs for the same pod cluster
	labeled, err := pccs.configStore.FindWhereLabeled(sel)
	if err != nil {
		return nil, util.Errorf("Got error while fetching labeled objects: %v", err)
	}
	if len(labeled) > 1 {
		fields.ID = labeled[0].ID
		return nil, util.Errorf("More than one pod cluster found for given selectors")
	}
	if len(labeled) == 0 {
		fields.ID = configstore.ID(uuid.New())
		labelsMap := make(map[string]string)
		labelsMap[pfields.PodIDLabel] = podID.String()
		labelsMap[pfields.AvailabilityZoneLabel] = az.String()
		labelsMap[pfields.ClusterNameLabel] = cn.String()

		if err := pccs.configStore.LabelConfig(fields.ID, labelsMap); err != nil {
			return nil, err
		}
	}

	if err := pccs.configStore.PutConfigTxn(ctx, fields, version); err != nil {
		return nil, err
	}

	return &fields, nil
}
