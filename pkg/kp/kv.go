// Package kp provides a generalized API for reading and writing pod manifests
// in consul.
package kp

import (
	"bytes"
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type ManifestResult struct {
	Manifest pods.Manifest
	Path     string
}

type Store struct {
	client *api.Client
}

func NewStore(opts Options) *Store {
	return &Store{client: NewConsulClient(opts)}
}

// KVError encapsulates an error in a Store operation. Errors returned from the
// Consul API cannot be exposed because they may contain the URL of the request,
// which includes an ACL token as a query parameter.
type KVError struct {
	Op  string
	Key string
}

func (err KVError) Error() string {
	return fmt.Sprintf("%s failed for path %s", err.Op, err.Key)
}

// SetPod writes a pod manifest into the consul key-value store. The key should
// not have a leading or trailing slash.
func (s *Store) SetPod(key string, manifest pods.Manifest) (time.Duration, error) {
	buf := bytes.Buffer{}
	err := manifest.Write(&buf)
	if err != nil {
		return 0, err
	}
	keyPair := &api.KVPair{
		Key:   key,
		Value: buf.Bytes(),
	}

	writeMeta, err := s.client.KV().Put(keyPair, nil)
	var retDur time.Duration
	if writeMeta != nil {
		retDur = writeMeta.RequestTime
	}
	if err != nil {
		err = KVError{Op: "set", Key: key}
	}
	return retDur, err
}

// Pod reads a pod manifest from the key-value store. If the given key does not
// exist, a nil *PodManifest will be returned, along with a pods.NoCurrentManifest
// error.
func (s *Store) Pod(key string) (*pods.Manifest, time.Duration, error) {
	kvPair, writeMeta, err := s.client.KV().Get(key, nil)
	if err != nil {
		return nil, 0, KVError{Op: "get", Key: key}
	}
	if kvPair == nil {
		return nil, writeMeta.RequestTime, pods.NoCurrentManifest
	}
	manifest, err := pods.ManifestFromBytes(kvPair.Value)
	return manifest, writeMeta.RequestTime, err
}

// ListPods reads all the pod manifests from the key-value store under the given
// key prefix. In the event of an error, the nil slice is returned.
//
// All the values under the given key prefix must be pod manifests.
func (s *Store) ListPods(keyPrefix string) ([]ManifestResult, time.Duration, error) {
	kvPairs, writeMeta, err := s.client.KV().List(keyPrefix, nil)
	if err != nil {
		return nil, 0, KVError{Op: "list", Key: keyPrefix}
	}
	var ret []ManifestResult

	for _, kvp := range kvPairs {
		manifest, err := pods.ManifestFromBytes(kvp.Value)
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

	var curIndex uint64 = 0

	for {
		select {
		case <-quitChan:
			return
		case <-time.After(1 * time.Second):
			pairs, meta, err := s.client.KV().List(keyPrefix, &api.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				errChan <- KVError{Op: "list", Key: keyPrefix}
			} else {
				curIndex = meta.LastIndex
				for _, pair := range pairs {
					manifest, err := pods.ManifestFromBytes(pair.Value)
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
//
// If a cluster is starting for the first time, it may report a leader just
// before beginning raft replication, thus rejecting requests made at that
// exact moment.
func (s *Store) Ping() error {
	_, qm, err := s.client.Catalog().Nodes(&api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return KVError{Op: "ping", Key: "/catalog/nodes"}
	}
	if qm == nil || !qm.KnownLeader {
		return util.Errorf("No known leader")
	}
	return nil
}
