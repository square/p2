package configstore

import (
	"context"
	"fmt"

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

type Storer interface {
	FetchConfig(ID) (Fields, Version, error)
	PutConfig(context.Context, ID, Fields, Version) error
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
	fmt.Printf("THE Value: %+v\n", c.Value)
	err = json.Unmarshal(c.Value, env)
	fmt.Printf("THE YAML: %+v\n", env.Config)
	if err != nil {
		return Fields{}, "", nil
	}
	parsedConfig := make(map[interface{}]interface{})
	err = yaml.Unmarshal([]byte(env.Config), &parsedConfig)
	if err != nil {
		return Fields{}, "", util.Errorf("Config did not unmarshal as YAML! %v", err)
	}
	fmt.Println(parsedConfig)
	return Fields{Config: parsedConfig, ID: id}, Version(string(consulMetadata.LastIndex)), nil
}

func (cs *ConsulStore) PutConfig(context.Context, ID, Fields, Version) error { return nil }
func (cs *ConsulStore) DeleteConfig(context.Context, ID, Version) error      { return nil }
