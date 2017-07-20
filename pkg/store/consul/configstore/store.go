package configstore

import (
	"context"
	"encoding/json"
	"path"

	"github.com/square/p2/pkg/util" // TODO this is wrong

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/store/consul/transaction"
	"gopkg.in/yaml.v2"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const ConfigTree string = "configs"

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
	PutConfig(Fields, *Version) error
	// FetchConfigsForPodClusters([]pcfields.ID) (map[pcfields.ID]Fields, error)
	DeleteConfig(ID, *Version) error
	PutConfigTxn(context.Context, Fields, *Version) error
	DeleteConfigTxn(context.Context, ID, *Version) error
	LabelConfig(ID, map[string]string) error
	FindWhereLabeled(klabels.Selector) ([]*Fields, error)
}

type ConsulKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	Get(prefix string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	CAS(*api.KVPair, *api.WriteOptions) (bool, *api.WriteMeta, error)
	DeleteCAS(*api.KVPair, *api.WriteOptions) (bool, *api.WriteMeta, error)
}

type envelope struct {
	Config string `json:"config"`
}

type Labeler interface {
	GetMatches(selector klabels.Selector, labelType labels.Type) ([]labels.Labeled, error)
	SetLabels(labelType labels.Type, id string, labels map[string]string) error
}

type ConsulStore struct {
	consulKV ConsulKV
	labeler  Labeler
}

var _ Storer = &ConsulStore{}

func NewConsulStore(consulKV ConsulKV, labeler Labeler) *ConsulStore {
	return &ConsulStore{consulKV: consulKV, labeler: labeler}
}

func (cs *ConsulStore) FetchConfig(id ID) (Fields, *Version, error) {
	path, err := configPath(id)
	if err != nil {
		return Fields{}, nil, err
	}

	config, consulMetadata, err := cs.consulKV.Get(path, nil)
	if config == nil || err != nil {
		return Fields{}, nil, util.Errorf("Unable to read config at %v", err)
	}
	env := &envelope{}
	err = json.Unmarshal(config.Value, env)
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

func (cs *ConsulStore) PutConfig(config Fields, v *Version) error {
	yamlConfig, err := yaml.Marshal(config.Config)
	if err != nil {
		return err
	}
	env := envelope{Config: string(yamlConfig)}

	bs, err := json.Marshal(env)
	if err != nil {
		return util.Errorf("could not marshal config as JSON: %s", err)
	}

	path, err := configPath(config.ID)
	if err != nil {
		return err
	}

	ok, _, err := cs.consulKV.CAS(
		&api.KVPair{
			Key:         path,
			Value:       bs,
			ModifyIndex: v.uint64(),
		}, nil)
	if !ok {
		return util.Errorf("CAS Failed! Consider retry")
	}
	if err != nil {
		return util.Errorf("CAS Failed: %v", err)
	}
	return nil
}

func (cs *ConsulStore) PutConfigTxn(ctx context.Context, config Fields, v *Version) error {
	yamlConfig, err := yaml.Marshal(config.Config)
	if err != nil {
		return util.Errorf("Failed to marshal configuration into YAML: %v", err)
	}
	env := envelope{Config: string(yamlConfig)}

	bs, err := json.Marshal(env)
	if err != nil {
		return util.Errorf("Failed to marshal configuration and id into JSON: %v", err)
	}
	key, err := configPath(config.ID)
	if err != nil {
		return err
	}

	err = transaction.Add(ctx, api.KVTxnOp{
		Verb:  api.KVCAS,
		Key:   key,
		Value: bs,
		Index: v.uint64(),
	})
	if err != nil {
		return util.Errorf("Failed to add PutConfig operation to the transaction: %v", err)
	}
	return nil
}

func (cs *ConsulStore) DeleteConfig(id ID, v *Version) error {
	key, err := configPath(id)
	if err != nil {
		return err
	}

	ok, _, err := cs.consulKV.DeleteCAS(&api.KVPair{
		Key:         key,
		ModifyIndex: v.uint64(),
	}, nil)
	if err != nil {
		return util.Errorf("CAS Delete Failed: %v", err)
	}
	if !ok {
		return util.Errorf("CAS Delete Failed! Consider retry.")
	}
	return nil
}

func (cs *ConsulStore) DeleteConfigTxn(ctx context.Context, id ID, v *Version) error {
	key, err := configPath(id)
	if err != nil {
		return err
	}

	err = transaction.Add(ctx, api.KVTxnOp{
		Verb:  api.KVDeleteCAS,
		Key:   key,
		Index: v.uint64(),
	})
	if err != nil {
		return util.Errorf("Failed to add DeleteConfig operation to the transaction: %v", err)
	}
	return nil
}

func (cs *ConsulStore) LabelConfig(id ID, labelsToApply map[string]string) error {
	return cs.labeler.SetLabels(labels.Config, id.String(), labelsToApply)
}

func (cs *ConsulStore) FindWhereLabeled(label klabels.Selector) ([]*Fields, error) {
	labeled, err := cs.labeler.GetMatches(label, labels.Config)
	if err != nil {
		return nil, err
	}
	fields := make([]*Fields, 0, len(labeled))
	for _, l := range labeled {
		f, _, err := cs.FetchConfig(ID(l.ID))
		if err != nil {
			return nil, util.Errorf("Failed fetching config id %s: %v", l.ID, err)
		}
		fields = append(fields, &f)
	}
	return fields, nil
}

func configPath(id ID) (string, error) {
	if id == "" {
		return "", util.Errorf("path requested with empty config ID")
	}

	return path.Join(ConfigTree, id.String()), nil
}
