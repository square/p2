package configstore

import (
	"encoding/json"
	"fmt"
	"testing"

	"gopkg.in/v1/yaml"

	"github.com/hashicorp/consul/api"
)

type FakeConsulKV struct {
	config map[ID]map[interface{}]interface{}
}

func (kv *FakeConsulKV) List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	return nil, nil, nil
}

func (kv *FakeConsulKV) Get(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	m := make(map[interface{}]interface{})
	m["foo"] = "bar"
	yaml := yamlMarshal(m)
	j := envelope{Config: yaml}

	return api.KVPairs{&api.KVPair{Value: jsonMarshal(j)}}, &api.QueryMeta{LastIndex: 1}, nil
}

func (kv *FakeConsulKV) CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	return false, nil, nil
}
func (kv *FakeConsulKV) DeleteCAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	return false, nil, nil
}

func yamlMarshal(m map[interface{}]interface{}) string {
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
	consulStore := NewConsulStore(&fakeConsulKV)

	fields, _, err := consulStore.FetchConfig("id doesn't matter for this test... yet")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%+v", fields)

}
