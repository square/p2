// Package kp provides a generalized API for reading and writing pod manifests
// in consul.
package kp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

// Healthcheck TTL
const TTL = 60 * time.Second

type ManifestResult struct {
	Manifest manifest.Manifest
	Path     string
}

type Store interface {
	SetPod(podPrefix PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
	Pod(podPrefix PodPrefix, nodename types.NodeName, podId types.PodID) (manifest.Manifest, time.Duration, error)
	DeletePod(podPrefix PodPrefix, nodename types.NodeName, podId types.PodID) (time.Duration, error)
	PutHealth(res WatchResult) (time.Time, time.Duration, error)
	GetHealth(service string, node types.NodeName) (WatchResult, error)
	GetServiceHealth(service string) (map[string]WatchResult, error)
	WatchPod(podPrefix PodPrefix, nodename types.NodeName, podId types.PodID, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- ManifestResult)
	WatchPods(podPrefix PodPrefix, nodename types.NodeName, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- []ManifestResult)
	Ping() error
	ListPods(podPrefix PodPrefix, nodename types.NodeName) ([]ManifestResult, time.Duration, error)
	AllPods(podPrefix PodPrefix) ([]ManifestResult, time.Duration, error)
	LockHolder(key string) (string, string, error)
	DestroyLockHolder(id string) error
	NewSession(name string, renewalCh <-chan time.Time) (Session, chan error, error)
	NewUnmanagedSession(session, name string) Session
	NewHealthManager(node types.NodeName, logger logging.Logger) HealthManager
}

// HealthManager manages a collection of health checks that share configuration and
// resources.
type HealthManager interface {
	// NewUpdater creates a new object for publishing a single service's health. Each
	// service should have its own updater.
	NewUpdater(pod types.PodID, service string) HealthUpdater

	// Close removes all published health statuses and releases all manager resources.
	Close()
}

// HealthUpdater allows an app's health to be updated.
type HealthUpdater interface {
	// PutHealth updates the health status of the app. Checkers are free to call it after
	// every health check or other status change.
	PutHealth(health WatchResult) error

	// Close removes a service's health check and releases all updater resources. Call this
	// when no more health statuses will be published.
	Close()
}

type WatchResult struct {
	Id      types.PodID
	Node    types.NodeName
	Service string
	Status  string
	Output  string
	Time    time.Time
	Expires time.Time `json:"Expires,omitempty"`
}

// ValueEquiv returns true if the value of the WatchResult--everything except the
// timestamps--is equivalent to another WatchResult.
func (r WatchResult) ValueEquiv(s WatchResult) bool {
	return r.Id == s.Id &&
		r.Node == s.Node &&
		r.Service == s.Service &&
		r.Status == s.Status &&
		r.Output == s.Output
}

// IsStale returns true when the result is stale according to the local clock.
func (r WatchResult) IsStale() bool {
	expires := r.Expires
	if expires.IsZero() {
		expires = r.Time.Add(TTL)
	}
	return time.Now().After(expires)
}

type consulStore struct {
	client *api.Client
}

func NewConsulStore(client *api.Client) Store {
	return &consulStore{
		client: client,
	}
}

func (c consulStore) PutHealth(res WatchResult) (time.Time, time.Duration, error) {
	key := HealthPath(res.Service, res.Node)

	now := time.Now()
	res.Time = now
	res.Expires = now.Add(TTL)
	data, err := json.Marshal(res)
	if err != nil {
		return time.Time{}, 0, err
	}
	keyPair := &api.KVPair{
		Key:   key,
		Value: data,
	}

	writeMeta, err := c.client.KV().Put(keyPair, nil)
	var retDur time.Duration
	if writeMeta != nil {
		retDur = writeMeta.RequestTime
	}
	if err != nil {
		return now, retDur, consulutil.NewKVError("put", key, err)
	}
	return now, retDur, nil
}

func (c consulStore) GetHealth(service string, node types.NodeName) (WatchResult, error) {
	healthRes := &WatchResult{}
	key := HealthPath(service, node)
	res, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return WatchResult{}, consulutil.NewKVError("get", key, err)
	} else if res == nil {
		return WatchResult{}, nil
	}
	err = json.Unmarshal(res.Value, healthRes)
	if err != nil {
		return WatchResult{}, consulutil.NewKVError("get", key, err)
	}
	if healthRes.IsStale() {
		return *healthRes, consulutil.NewKVError("get", key, fmt.Errorf("stale health entry"))
	}
	return *healthRes, nil
}

func (c consulStore) GetServiceHealth(service string) (map[string]WatchResult, error) {
	healthRes := make(map[string]WatchResult)
	key := HealthPath(service, "")
	res, _, err := c.client.KV().List(key, nil)
	if err != nil {
		return healthRes, consulutil.NewKVError("list", key, err)
	} else if res == nil {
		return healthRes, nil
	}
	for _, kvp := range res {
		watch := &WatchResult{}
		err = json.Unmarshal(kvp.Value, watch)
		if err != nil {
			return healthRes, consulutil.NewKVError("get", key, err)
		}
		// maps key to result (eg /health/hello/nodename)
		healthRes[kvp.Key] = *watch
	}

	return healthRes, nil
}

// SetPod writes a pod manifest into the consul key-value store.
func (c consulStore) SetPod(podPrefix PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error) {
	buf := bytes.Buffer{}
	err := manifest.Write(&buf)
	if err != nil {
		return 0, err
	}

	key, err := podPath(podPrefix, nodename, manifest.ID())
	if err != nil {
		return 0, err
	}
	keyPair := &api.KVPair{
		Key:   key,
		Value: buf.Bytes(),
	}

	writeMeta, err := c.client.KV().Put(keyPair, nil)
	var retDur time.Duration
	if writeMeta != nil {
		retDur = writeMeta.RequestTime
	}
	if err != nil {
		return retDur, consulutil.NewKVError("put", key, err)
	}
	return retDur, nil
}

// DeletePod deletes a pod manifest from the key-value store. No error will be
// returned if the key didn't exist.
func (c consulStore) DeletePod(podPrefix PodPrefix, nodename types.NodeName, podId types.PodID) (time.Duration, error) {
	key, err := podPath(podPrefix, nodename, podId)
	if err != nil {
		return 0, err
	}

	writeMeta, err := c.client.KV().Delete(key, nil)
	if err != nil {
		return 0, consulutil.NewKVError("delete", key, err)
	}
	return writeMeta.RequestTime, nil
}

// Pod reads a pod manifest from the key-value store. If the given key does not
// exist, a nil *PodManifest will be returned, along with a pods.NoCurrentManifest
// error.
func (c consulStore) Pod(podPrefix PodPrefix, nodename types.NodeName, podId types.PodID) (manifest.Manifest, time.Duration, error) {
	key, err := podPath(podPrefix, nodename, podId)
	if err != nil {
		return nil, 0, err
	}

	kvPair, writeMeta, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, 0, consulutil.NewKVError("get", key, err)
	}
	if kvPair == nil {
		return nil, writeMeta.RequestTime, pods.NoCurrentManifest
	}
	manifest, err := manifest.ManifestFromBytes(kvPair.Value)
	return manifest, writeMeta.RequestTime, err
}

// ListPods reads all the pod manifests from the key-value store for a
// specified host under a given tree. In the event of an error, the nil slice
// is returned.
//
// All the values under the given path must be pod manifests.
func (c consulStore) ListPods(podPrefix PodPrefix, nodename types.NodeName) ([]ManifestResult, time.Duration, error) {
	keyPrefix, err := nodePath(podPrefix, nodename)
	if err != nil {
		return nil, 0, err
	}

	return c.listPods(keyPrefix + "/")
}

// Lists all pods under a tree regardless of node name
func (c consulStore) AllPods(podPrefix PodPrefix) ([]ManifestResult, time.Duration, error) {
	keyPrefix := string(podPrefix) + "/"
	return c.listPods(keyPrefix)
}

func (c consulStore) listPods(keyPrefix string) ([]ManifestResult, time.Duration, error) {
	kvPairs, queryMeta, err := c.client.KV().List(keyPrefix, nil)
	if err != nil {
		return nil, 0, consulutil.NewKVError("list", keyPrefix, err)
	}
	var ret []ManifestResult

	for _, kvp := range kvPairs {
		manifest, err := manifest.ManifestFromBytes(kvp.Value)
		if err != nil {
			return nil, queryMeta.RequestTime, err
		}
		ret = append(ret, ManifestResult{manifest, kvp.Key})
	}

	return ret, queryMeta.RequestTime, nil
}

// WatchPod is like WatchPods, but for a single key only. The output channel
// may contain nil manifests, if the target key does not exist.
func (c consulStore) WatchPod(
	podPrefix PodPrefix,
	nodename types.NodeName,
	podId types.PodID,
	quitChan <-chan struct{},
	errChan chan<- error,
	podChan chan<- ManifestResult,
) {

	defer close(podChan)

	key, err := podPath(podPrefix, nodename, podId)
	if err != nil {
		select {
		case <-quitChan:
		case errChan <- err:
		}
		return
	}

	kvpChan := make(chan *api.KVPair)
	go consulutil.WatchSingle(key, c.client.KV(), kvpChan, quitChan, errChan)
	for pair := range kvpChan {
		out := ManifestResult{Path: key}
		if pair != nil {
			manifest, err := manifest.ManifestFromBytes(pair.Value)
			if err != nil {
				select {
				case <-quitChan:
					return
				case errChan <- util.Errorf("Could not parse pod manifest at %s: %s. Content follows: \n%s", pair.Key, err, pair.Value):
					continue
				}
			} else {
				out.Manifest = manifest
			}
		}
		select {
		case <-quitChan:
			return
		case podChan <- out:
		}
	}
}

// WatchPods watches the key-value store for any changes to pods for a given
// host under a given tree.  The resulting manifests are emitted on podChan.
// WatchPods does not return in the event of an error, but it will emit the
// error on errChan. To terminate WatchPods, close quitChan.
//
// All the values under the given path must be pod manifests. Emitted
// manifests might be unchanged from the last time they were read. It is the
// caller's responsibility to filter out unchanged manifests.
func (c consulStore) WatchPods(
	podPrefix PodPrefix,
	nodename types.NodeName,
	quitChan <-chan struct{},
	errChan chan<- error,
	podChan chan<- []ManifestResult,
) {
	defer close(podChan)

	keyPrefix, err := nodePath(podPrefix, nodename)
	if err != nil {
		select {
		case <-quitChan:
		case errChan <- err:
		}
		return
	}

	kvPairsChan := make(chan api.KVPairs)
	go consulutil.WatchPrefix(keyPrefix, c.client.KV(), kvPairsChan, quitChan, errChan)
	for kvPairs := range kvPairsChan {
		manifests := make([]ManifestResult, 0, len(kvPairs))
		for _, pair := range kvPairs {
			manifest, err := manifest.ManifestFromBytes(pair.Value)
			if err != nil {
				select {
				case <-quitChan:
					return
				case errChan <- util.Errorf("Could not parse pod manifest at %s: %s. Content follows: \n%s", pair.Key, err, pair.Value):
				}
			} else {
				manifests = append(manifests, ManifestResult{manifest, pair.Key})
			}
		}
		select {
		case <-quitChan:
			return
		case podChan <- manifests:
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
func (c consulStore) Ping() error {
	_, qm, err := c.client.Catalog().Nodes(&api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return consulutil.NewKVError("ping", "/catalog/nodes", err)
	}
	if qm == nil || !qm.KnownLeader {
		return util.Errorf("No known leader")
	}
	return nil
}

func HealthPath(service string, node types.NodeName) string {
	if node == "" {
		return fmt.Sprintf("%s/%s", "health", service)
	}
	return fmt.Sprintf("%s/%s/%s", "health", service, node)
}

func (c consulStore) NewHealthManager(node types.NodeName, logger logging.Logger) HealthManager {
	return c.newSessionHealthManager(node, logger)
}
