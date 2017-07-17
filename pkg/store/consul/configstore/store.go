package configstore

import (
	"context"

	"github.com/square/p2/pkg/util" // TODO this is wrong

	"gopkg.in/v1/yaml"

	"encoding/json"

	"github.com/hashicorp/consul/api"
)

type ID string
type Version uint64

func (id *ID) String() string {
	return string(*id)
}

func (v *Version) uint64() uint64 {
	return uint64(*v)
}

type Fields struct {
	Config map[interface{}]interface{}
	ID     ID
}

// Storer should also have the ability to look things up by pod cluster type things (such as the holy trinity of thingies)
type Storer interface {
	FetchConfig(ID) (Fields, *Version, error)
	PutConfig(context.Context, Fields, *Version) error
	// FetchConfigsForPodClusters([]pcfields.ID) (map[pcfields.ID]Fields, error)
	DeleteConfig(context.Context, ID, *Version) error
}

type ConsulKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	Get(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	CAS(*api.KVPair, *api.WriteOptions) (bool, *api.WriteMeta, error)
	DeleteCAS(*api.KVPair, *api.WriteOptions) (bool, *api.WriteMeta, error)
}

type envelope struct {
	Config string `json:"config"`
}

type ConsulStore struct {
	consulKV ConsulKV
}

var _ Storer = &ConsulStore{}

func NewConsulStore(consulKV ConsulKV) *ConsulStore {
	return &ConsulStore{consulKV: consulKV}
}

func (cs *ConsulStore) FetchConfig(id ID) (Fields, *Version, error) {
	config, consulMetadata, err := cs.consulKV.Get(id.String(), nil)
	if err != nil {
		return Fields{}, nil, util.Errorf("Unable to read config at %v", err)
	}
	if len(config) != 1 {
		return Fields{}, nil, util.Errorf("Unexpected number of configs stored at ID: %s. Got: %d", id, len(config))
	}
	c := config[0]
	env := &envelope{}
	err = json.Unmarshal(c.Value, env)
	if err != nil {
		return Fields{}, nil, nil
	}
	parsedConfig := make(map[interface{}]interface{})
	err = yaml.Unmarshal([]byte(env.Config), &parsedConfig)
	if err != nil {
		return Fields{}, nil, util.Errorf("Config did not unmarshal as YAML! %v", err)
	}

	v := Version(consulMetadata.LastIndex)
	return Fields{Config: parsedConfig, ID: id}, &v, nil
}

func (cs *ConsulStore) PutConfig(ctx context.Context, config Fields, v *Version) error {
	yamlConfig, err := yaml.Marshal(config.Config)
	if err != nil {
		return err
	}
	env := envelope{Config: string(yamlConfig)}

	bs, err := json.Marshal(env)
	kvPair := &api.KVPair{
		Key:         config.ID.String(),
		Value:       bs,
		ModifyIndex: v.uint64(),
	}
	ok, _, err := cs.consulKV.CAS(kvPair, nil)
	if !ok {
		return util.Errorf("CAS Failed! Consider retry")
	}
	if err != nil {
		return util.Errorf("CAS Failed: %v", err)
	}
	return nil
}

func (cs *ConsulStore) DeleteConfig(_ context.Context, id ID, v *Version) error {
	kvPair := &api.KVPair{
		Key:         id.String(),
		ModifyIndex: v.uint64(),
	}

	ok, _, err := cs.consulKV.DeleteCAS(kvPair, nil)
	if !ok {
		return util.Errorf("CAS Delete Failed! Consider retry")
	}
	if err != nil {
		return util.Errorf("CAS Delete Failed: %", err)
	}

	return nil

}
