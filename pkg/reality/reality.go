// Package reality provides a kv-store agnostic way to get/set manifests
// running in reality
package reality

// If this package ever needs a `Watch` functionality, consider merging with
// the intent package in some way, for the sake of DRY.

import (
	"bytes"
	"fmt"
	"time"

	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

const REALITY_TREE string = "/reality"

type ConsulClient interface {
	KV() *consulapi.KV
}

type Options struct {
	Token   string
	Address string
}

type Store struct {
	Opts       Options
	ConsulOpts *consulapi.Config
	client     ConsulClient
}

func LookupStore(opts Options) (*Store, error) {
	store := &Store{
		Opts:       opts,
		ConsulOpts: consulapi.DefaultConfig(),
	}
	store.ConsulOpts.Address = opts.Address
	var err error
	store.client, err = consulapi.NewClient(store.ConsulOpts)

	if err != nil {
		return nil, util.Errorf("Could not initialize consul client: %s", err)
	}
	return store, nil
}

func (r *Store) RealityKey(node string, manifest pods.PodManifest) string {
	return fmt.Sprintf("%s/%s/%s", REALITY_TREE, node, manifest.ID())
}

func (r *Store) Pod(node, podId string) (*pods.PodManifest, error) {
	key := fmt.Sprintf("%s/%s/%s", REALITY_TREE, node, podId)
	kvPair, _, err := r.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}
	return pods.PodManifestFromBytes(kvPair.Value)
}

func (r *Store) SetPod(node string, manifest pods.PodManifest) (time.Duration, error) {
	buf := bytes.Buffer{}
	err := manifest.Write(&buf)
	if err != nil {
		return 0, err
	}
	keyPair := &consulapi.KVPair{
		Key:   r.RealityKey(node, manifest),
		Value: buf.Bytes(),
	}

	writeMeta, err := r.client.KV().Put(keyPair, nil)
	return writeMeta.RequestTime, err
}
