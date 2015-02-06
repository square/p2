// Package kp provides a generalized API for reading and writing pod manifests
// in consul.
package kp

import (
	"bytes"
	"time"

	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type ManifestResult struct {
	Manifest pods.PodManifest
	Path     string
}
type Options struct {
	Token   string
	Address string
}

type Store struct {
	client *consulapi.Client
}

func NewStore(opts Options) *Store {
	conf := consulapi.DefaultConfig()
	conf.Address = opts.Address
	conf.Token = opts.Token

	// the error is always nil
	client, _ := consulapi.NewClient(conf)
	return &Store{client: client}
}

// SetPod writes a pod manifest into the consul key-value store. The key should
// not have a leading or trailing slash.
func (s *Store) SetPod(key string, manifest pods.PodManifest) (time.Duration, error) {
	buf := bytes.Buffer{}
	err := manifest.Write(&buf)
	if err != nil {
		return 0, err
	}
	keyPair := &consulapi.KVPair{
		Key:   key,
		Value: buf.Bytes(),
	}

	writeMeta, err := s.client.KV().Put(keyPair, nil)
	if writeMeta == nil {
		return 0, err
	}
	return writeMeta.RequestTime, err
}

// Pod reads a pod manifest from the key-value store. If the given key does not
// exist, a nil *PodManifest will be returned, along with a pods.NoCurrentManifest
// error.
func (s *Store) Pod(key string) (*pods.PodManifest, time.Duration, error) {
	kvPair, writeMeta, err := s.client.KV().Get(key, nil)
	if err != nil {
		return nil, 0, err
	}
	if kvPair == nil {
		return nil, writeMeta.RequestTime, pods.NoCurrentManifest
	}
	manifest, err := pods.PodManifestFromBytes(kvPair.Value)
	return manifest, writeMeta.RequestTime, err
}

// ListPods reads all the pod manifests from the key-value store under the given
// key prefix. In the event of an error, the nil slice is returned.
//
// All the values under the given key prefix must be pod manifests.
func (s *Store) ListPods(keyPrefix string) ([]ManifestResult, time.Duration, error) {
	kvPairs, writeMeta, err := s.client.KV().List(keyPrefix, nil)
	if err != nil {
		return nil, 0, err
	}
	var ret []ManifestResult

	for _, kvp := range kvPairs {
		manifest, err := pods.PodManifestFromBytes(kvp.Value)
		if err != nil {
			return nil, writeMeta.RequestTime, err
		}
		ret = append(ret, ManifestResult{*manifest, kvp.Key})
	}

	return ret, writeMeta.RequestTime, nil
}

// WatchPods watches the key-value store for any changes under the given key
// prefix. The resulting manifests are emitted on podChan. WatchPods does not
// return in the event of an error, but it will emit the error on errChan. To
// terminate WatchPods, emit on quitChan.
//
// All the values under the given key prefix must be pod manifests. Emitted
// manifests might be unchanged from the last time they were read. It is the
// caller's responsibility to filter out unchanged manifests.
func (s *Store) WatchPods(keyPrefix string, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- ManifestResult) {
	defer close(podChan)
	defer close(errChan)

	var curIndex uint64 = 0

	for {
		select {
		case <-quitChan:
			return
		case <-time.After(1 * time.Second):
			pairs, meta, err := s.client.KV().List(keyPrefix, &consulapi.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				errChan <- err
			} else {
				curIndex = meta.LastIndex
				for _, pair := range pairs {
					manifest, err := pods.PodManifestFromBytes(pair.Value)
					if err != nil {
						errChan <- util.Errorf("Could not parse pod manifest at %s: %s. Content follows: \n%s", pair.Key, err, pair.Value)
					} else {
						podChan <- ManifestResult{*manifest, pair.Key}
					}
				}
			}
		}

	}
}

// Ping confirms that the store's Consul agent can be reached and it has a
// leader. If the return is nil, then the store should be ready to accept
// requests.
//
// If the return is non-nil, this typically indicates that either Consul is
// unreachable (eg the agent is not listening on the target port) or has not
// found a leader (in which case Consul returns a 500 to all endpoints, except
// the status types).
func (s *Store) Ping() error {
	_, qm, err := s.client.Catalog().Nodes(&consulapi.QueryOptions{RequireConsistent: true})
	if err != nil {
		return err
	}
	if qm == nil || !qm.KnownLeader {
		return util.Errorf("No known leader")
	}
	return nil
}
