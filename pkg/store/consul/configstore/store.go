package configstore

import (
	"context"
	"encoding/json"

	"github.com/square/p2/pkg/util" // TODO this is wrong

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/store/consul/transaction"
	"gopkg.in/yaml.v2"
	klabels "k8s.io/kubernetes/pkg/labels"
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

type ConsulStore struct {
	consulKV   ConsulKV
	applicator labels.Applicator
}

var _ Storer = &ConsulStore{}

func NewConsulStore(consulKV ConsulKV, applicator labels.Applicator) *ConsulStore {
	return &ConsulStore{consulKV: consulKV, applicator: applicator}
}

func (cs *ConsulStore) FetchConfig(id ID) (Fields, *Version, error) {
	config, consulMetadata, err := cs.consulKV.Get(id.String(), nil)
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
	ok, _, err := cs.consulKV.CAS(
		&api.KVPair{
			Key:         config.ID.String(),
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
	err = transaction.Add(ctx, api.KVTxnOp{
		Verb:  string(api.KVSet),
		Key:   config.ID.String(),
		Value: bs,
		Index: v.uint64(),
	})
	if err != nil {
		return util.Errorf("Failed to add PutConfig operation to the transaction: %v", err)
	}
	return nil
}

func (cs *ConsulStore) DeleteConfig(id ID, v *Version) error {
	ok, _, err := cs.consulKV.DeleteCAS(&api.KVPair{
		Key:         id.String(),
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
	err := transaction.Add(ctx, api.KVTxnOp{
		Verb:  string(api.KVDeleteCAS),
		Key:   id.String(),
		Index: v.uint64(),
	})
	if err != nil {
		return util.Errorf("Failed to add DeleteConfig operation to the transaction: %v", err)
	}
	return nil
}

func (cs *ConsulStore) LabelConfig(id ID, labelsToApply map[string]string) error {
	return cs.applicator.SetLabels(labels.Config, id.String(), labelsToApply)
}

func (cs *ConsulStore) FindWhereLabeled(label klabels.Selector) ([]*Fields, error) {
	labeled, err := cs.applicator.GetMatches(label, labels.Config)
	if err != nil {
		return nil, util.Errorf("Could not query labels for %s, error was: %v", label.String(), err)
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
