package configstore

import (
	"context"

	"github.com/square/p2/pkg/util" // TODO this is wrong

	"gopkg.in/v1/yaml"

	"encoding/json"

	"github.com/hashicorp/consul/api"
)

type ID string

func (id *ID) String() string {
	return string(*id)
}

type Version string

type Fields struct {
	Config map[interface{}]interface{}
	ID     ID
}

// Storer should also have the ability to look things up by pod cluster type things (such as the holy trinity of thingies)
type Storer interface {
	FetchConfig(ID) (Fields, Version, error)
	PutConfig(context.Context, Fields, Version) error
	// FetchConfigsForPodClusters([]pcfields.ID) (map[pcfields.ID]Fields, error)
	DeleteConfig(context.Context, ID, Version) error
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

func NewConsulStore(consulKV ConsulKV) *ConsulStore {
	return &ConsulStore{consulKV: consulKV}
}

func (cs *ConsulStore) FetchConfig(id ID) (Fields, Version, error) {
	config, consulMetadata, err := cs.consulKV.Get(id.String(), nil)
	if err != nil {
		return Fields{}, "", util.Errorf("Unable to read config at %v", err)
	}
	if len(config) != 1 {
		return Fields{}, "", util.Errorf("Unexpected number of configs stored at ID: %s", id)
	}
	c := config[0]
	env := &envelope{}
	err = json.Unmarshal(c.Value, env)
	if err != nil {
		return Fields{}, "", nil
	}
	parsedConfig := make(map[interface{}]interface{})
	err = yaml.Unmarshal([]byte(env.Config), &parsedConfig)
	if err != nil {
		return Fields{}, "", util.Errorf("Config did not unmarshal as YAML! %v", err)
	}
	return Fields{Config: parsedConfig, ID: id}, Version(string(consulMetadata.LastIndex)), nil
}

func (cs *ConsulStore) PutConfig(ctx context.Context, config Fields, v Version) error {
	shyaml, err := yaml.Marshal(config.Config)
	if err != nil {
		return err
	}
	env := envelope{Config: string(shyaml)}

	bs, err := json.Marshal(env)
	kvPair := &api.KVPair{
		Key:   config.ID.String(),
		Value: bs,
		// ModifyIndex TODO
	}
	ok, _, err := cs.consulKV.CAS(kvPair, nil)
	if !ok {
		return util.Errorf("CAS Failed! Consider retry")
	}
	if err != nil {
		return util.Errorf("Cas Failed: %v", err)
	}
	return nil
}

func (cs *ConsulStore) DeleteConfig(context.Context, ID, Version) error { return nil }
