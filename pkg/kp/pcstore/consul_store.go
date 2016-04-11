package pcstore

import (
	"encoding/json"
	"path"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/github.com/pborman/uuid"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

type consulKV interface {
	Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
}

type consulStore struct {
	kv consulKV
}

var _ Store = &consulStore{}

func (s *consulStore) Create(
	podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName,
	podSelector klabels.Selector,
	annotations fields.Annotations,
) (fields.PodCluster, error) {
	id := fields.ID(uuid.New())

	pc := fields.PodCluster{
		ID:               id,
		PodID:            podID,
		AvailabilityZone: availabilityZone,
		Name:             clusterName,
		PodSelector:      podSelector,
		Annotations:      annotations,
	}

	key, err := s.pcPath(id)
	if err != nil {
		return fields.PodCluster{}, err
	}

	jsonPC, err := json.Marshal(pc)
	if err != nil {
		// Probably the annotations don't marshal to JSON
		return fields.PodCluster{}, util.Errorf("Unable to marshal pod cluster as JSON: %s", err)
	}

	// the chance of the UUID already existing is vanishingly small, but
	// technically not impossible, so we should use the CAS index to guard
	// against duplicate UUIDs
	success, _, err := s.kv.CAS(&api.KVPair{
		Key:         key,
		Value:       jsonPC,
		ModifyIndex: 0,
	}, nil)
	if err != nil {
		return fields.PodCluster{}, consulutil.NewKVError("cas", key, err)
	}

	if !success {
		return fields.PodCluster{}, util.Errorf("Could not set pod cluster at path '%s'", key)
	}
	return pc, nil
}

func (s *consulStore) Get(id fields.ID) (fields.PodCluster, error) {
	key, err := s.pcPath(id)
	if err != nil {
		return fields.PodCluster{}, err
	}

	kvp, _, err := s.kv.Get(key, nil)
	if err != nil {
		return fields.PodCluster{}, consulutil.NewKVError("get", key, err)
	}
	if kvp == nil {
		// ID didn't exist
		return fields.PodCluster{}, NoPodCluster
	}

	return kvpToPC(kvp)
}

func (s *consulStore) pcPath(pcID fields.ID) (string, error) {
	if pcID == "" {
		return "", util.Errorf("Path requested for empty pod cluster ID")
	}

	return path.Join(podClusterTree, pcID.String()), nil
}

func kvpToPC(pair *api.KVPair) (fields.PodCluster, error) {
	pc := fields.PodCluster{}
	err := json.Unmarshal(pair.Value, &pc)
	if err != nil {
		return pc, util.Errorf("Could not unmarshal pod cluster ('%s') as json: %s", string(pair.Value), err)
	}

	return pc, nil
}
