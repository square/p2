package consulutil

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hashicorp/consul/api"
)

type CannedResponseKeyser struct {
	SetKeys []string
	Meta    *api.QueryMeta
	Err     error

	ReceivedPrefix  string
	ReceivedOptions *api.QueryOptions
}

func (k *CannedResponseKeyser) Keys(prefix string, _ string, options *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	k.ReceivedPrefix = prefix
	k.ReceivedOptions = options
	return k.SetKeys, k.Meta, k.Err
}

func TestSafeKeys(t *testing.T) {
	keyser := &CannedResponseKeyser{
		SetKeys: []string{"a", "b"},
		Err:     errors.New("total system breakdown"),
		Meta:    &api.QueryMeta{LastIndex: 1400},
	}

	prefix := "some/pre/fix"
	options := &api.QueryOptions{AllowStale: true}
	keys, meta, err := SafeKeys(keyser, make(chan struct{}), prefix, options)
	if err.(KVError).KVError != keyser.Err {
		t.Errorf("expected err to be %s but was %s", keyser.Err, err)
	}

	if *meta != *keyser.Meta {
		t.Errorf("expected query meta to be %+v but was %+v", *keyser.Meta, *meta)
	}

	if !reflect.DeepEqual(keys, keyser.SetKeys) {
		t.Errorf("expected keys to be %s but was %s", keyser.SetKeys, keys)
	}

	if keyser.ReceivedPrefix != prefix {
		t.Errorf("safe keys passed along prefix %s but should have been %s", keyser.ReceivedPrefix, prefix)
	}

	if *keyser.ReceivedOptions != *options {
		t.Errorf("SafeKeys() passed along options %+v but should have been %+v", *keyser.ReceivedOptions, *options)
	}
}
