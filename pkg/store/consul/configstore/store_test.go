package configstore

import (
	"encoding/json"
	"fmt"
	"testing"

	"gopkg.in/yaml.v2"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/labels"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type FakeConsulKV struct {
	config map[ID][]byte
}

func (kv *FakeConsulKV) insertConfig(id ID, m map[string]interface{}) {
	if kv.config == nil {
		kv.config = make(map[ID][]byte)
	}
	yaml := yamlMarshal(m)
	j := envelope{Config: yaml}
	kv.config[id] = jsonMarshal(j)
}

func (kv *FakeConsulKV) List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	return nil, nil, nil
}

func (kv *FakeConsulKV) Get(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	bs, ok := kv.config[ID(prefix)]
	if !ok {
		return nil, nil, nil
	}
	return api.KVPairs{&api.KVPair{Value: bs}}, &api.QueryMeta{LastIndex: 1}, nil
}

// /config/deadbeef
func (kv *FakeConsulKV) CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	if kv.config == nil {
		kv.config = make(map[ID][]byte)
	}
	kv.config[ID(p.Key)] = p.Value
	return true, nil, nil
}

func (kv *FakeConsulKV) DeleteCAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	id := ID(p.Key)
	delete(kv.config, id)

	return true, nil, nil
}

func yamlMarshal(m map[string]interface{}) string {
	bs, err := yaml.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("I need better YAML, y'all: %v", err))
	}
	return string(bs)
}

func jsonMarshal(j envelope) []byte {
	bs, err := json.Marshal(j)
	if err != nil {
		panic(fmt.Sprintf("bad JSON: %v", err))
	}

	return bs
}

func TestFetchConfig(t *testing.T) {
	fakeConsulKV := FakeConsulKV{}
	consulStore := NewConsulStore(&fakeConsulKV, labels.NewFakeApplicator())

	m := make(map[string]interface{})
	m["configuration"] = "hell yeah"
	id := ID("foo")
	fakeConsulKV.insertConfig(id, m)

	fields, _, err := consulStore.FetchConfig(id)
	if err != nil {
		t.Fatal(err)
	}

	if fields.ID != id {
		t.Errorf("Returned id is not correct. Want: %s, have: %s", id, fields.ID)
	}

	if len(fields.Config) != len(m) {
		t.Errorf("Size of configuration does not match.")
	}

	for k, v := range m {
		if fields.Config[k] != v {
			t.Errorf("Fields do not match on key %s. Wanted: %v have: %v", k, v, fields.Config[k])
		}
	}
}

func version(i uint64) *Version {
	v := Version(i)
	return &v
}

func TestPutConfig(t *testing.T) {
	fakeConsulKV := FakeConsulKV{}
	consulStore := NewConsulStore(&fakeConsulKV, labels.NewFakeApplicator())

	id := ID("foo")
	m := make(map[string]interface{})
	m["configuration"] = "hell yeah"
	f := Fields{
		ID:     id,
		Config: m,
	}

	consulStore.PutConfig(f, version(1))

	fields, _, err := consulStore.FetchConfig(id)
	if err != nil {
		t.Errorf("Could not read config out of datastore")
	}

	if fields.ID != id {
		t.Errorf("Returned id is not correct. Want: %s, have: %s", id, fields.ID)
	}

	if len(fields.Config) != len(m) {
		t.Errorf("Size of configuration does not match.")
	}

	for k, v := range m {
		if fields.Config[k] != v {
			t.Errorf("Fields do not match on key %s. Wanted: %v have: %v", k, v, fields.Config[k])
		}
	}
}

func TestDeleteConfig(t *testing.T) {
	fakeConsulKV := FakeConsulKV{}
	consulStore := NewConsulStore(&fakeConsulKV, labels.NewFakeApplicator())

	id := ID("foo")
	m := make(map[string]interface{})
	m["configuration"] = "hell yeah"
	f := Fields{
		ID:     id,
		Config: m,
	}

	consulStore.PutConfig(f, version(1))

	err := consulStore.DeleteConfig(id, version(1))
	if err != nil {
		t.Fatalf("Error when deleting configuration from store: %v", err)
	}

	fields, _, err := consulStore.FetchConfig(id)
	if err == nil {
		t.Errorf("Expected to receive an error when fetching deleted configuration. Got: %v", fields)
	}
}

func TestLabels(t *testing.T) {
	fakeConsulKV := FakeConsulKV{}
	consulStore := NewConsulStore(&fakeConsulKV, labels.NewFakeApplicator())

	id := ID("foo")
	m := make(map[string]interface{})
	m["configuration"] = "hell yeah"
	f := Fields{
		ID:     id,
		Config: m,
	}

	consulStore.PutConfig(f, version(1))

	labelsToApply := make(map[string]string)
	labelsToApply["a"] = "b"
	labelsToApply["eh"] = "bee"
	err := consulStore.LabelConfig(id, labelsToApply)
	if err != nil {
		t.Errorf("Could not label the new config: %v", err)
	}

	sel := klabels.Everything().Add("a", klabels.EqualsOperator, []string{"b"})
	labeled, err := consulStore.FindWhereLabeled(sel)
	if len(labeled) != 1 {
		t.Errorf("Found wrong number of configs. expected: %d got: %d", 1, len(labeled))
	}

	sel = klabels.Everything().Add("eh", klabels.EqualsOperator, []string{"bee"})
	labeled, err = consulStore.FindWhereLabeled(sel)
	if len(labeled) != 1 {
		t.Errorf("Found wrong number of configs. expected: %d got: %d", 1, len(labeled))
	}
}
