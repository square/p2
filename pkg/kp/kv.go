// Package kp provides a generalized API for reading and writing pod manifests
// in consul.
package kp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"

	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

// Healthcheck TTL
const TTL = 60 * time.Second

var showConsulErrors = kingpin.Flag(
	"show-consul-errors-unsafe",
	"Show detailed error messages from Consul (use only when debugging)",
).Default("false").Bool()

type ManifestResult struct {
	Manifest pods.Manifest
	Path     string
}

type Store interface {
	SetPod(key string, manifest pods.Manifest) (time.Duration, error)
	Pod(key string) (*pods.Manifest, time.Duration, error)
	PutHealth(res WatchResult) (time.Time, time.Duration, error)
	GetHealth(service, node string) (WatchResult, error)
	GetServiceHealth(service string) (map[string]WatchResult, error)
	WatchPods(keyPrefix string, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- ManifestResult)
	RegisterService(pods.Manifest, string) error
	Ping() error
	ListPods(keyPrefix string) ([]ManifestResult, time.Duration, error)
	LockHolder(key string) (string, string, error)
	DestroyLockHolder(id string) error
	NewLock(name string) (Lock, error)
}

type WatchResult struct {
	Id      string
	Node    string
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

func NewConsulStore(opts Options) Store {
	return &consulStore{client: NewConsulClient(opts)}
}

// KVError encapsulates an error in a Store operation. Errors returned from the
// Consul API cannot be exposed because they may contain the URL of the request,
// which includes an ACL token as a query parameter.
type KVError struct {
	Op          string
	Key         string
	UnsafeError error
	filename    string
	function    string
	lineNumber  int
}

var _ util.CallsiteError = KVError{}

func (err KVError) Error() string {
	cerr := ""
	if *showConsulErrors {
		cerr = fmt.Sprintf(": %s", err.UnsafeError)
	}
	return fmt.Sprintf("%s failed for path %s%s", err.Op, err.Key, cerr)
}

func (err KVError) Filename() string {
	return err.filename
}

func (err KVError) Function() string {
	return err.function
}

func (err KVError) LineNumber() int {
	return err.lineNumber
}

func NewKVError(op string, key string, unsafeError error) KVError {
	var function string
	// Skip one stack frame to get the file & line number of caller.
	pc, file, line, ok := runtime.Caller(1)
	if ok {
		function = runtime.FuncForPC(pc).Name()
	}
	return KVError{
		Op:          op,
		Key:         key,
		UnsafeError: unsafeError,
		filename:    filepath.Base(file),
		function:    function,
		lineNumber:  line,
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
		return now, retDur, NewKVError("put", key, err)
	}
	return now, retDur, nil
}

func (c consulStore) GetHealth(service, node string) (WatchResult, error) {
	healthRes := &WatchResult{}
	key := HealthPath(service, node)
	res, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return WatchResult{}, NewKVError("get", key, err)
	} else if res == nil {
		return WatchResult{}, nil
	}
	err = json.Unmarshal(res.Value, healthRes)
	if err != nil {
		return WatchResult{}, NewKVError("get", key, err)
	}
	if healthRes.IsStale() {
		return *healthRes, NewKVError("get", key, fmt.Errorf("stale health entry"))
	}
	return *healthRes, nil
}

func (c consulStore) GetServiceHealth(service string) (map[string]WatchResult, error) {
	healthRes := make(map[string]WatchResult)
	key := HealthPath(service, "")
	res, _, err := c.client.KV().List(key, nil)
	if err != nil {
		return healthRes, NewKVError("list", key, err)
	} else if res == nil {
		return healthRes, nil
	}
	for _, kvp := range res {
		watch := &WatchResult{}
		err = json.Unmarshal(kvp.Value, watch)
		if err != nil {
			return healthRes, NewKVError("get", key, err)
		}
		// maps key to result (eg /health/hello/nodename)
		healthRes[kvp.Key] = *watch
	}

	return healthRes, nil
}

// SetPod writes a pod manifest into the consul key-value store. The key should
// not have a leading or trailing slash.
func (c consulStore) SetPod(key string, manifest pods.Manifest) (time.Duration, error) {
	buf := bytes.Buffer{}
	err := manifest.Write(&buf)
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
		return retDur, NewKVError("put", key, err)
	}
	return retDur, nil
}

// Pod reads a pod manifest from the key-value store. If the given key does not
// exist, a nil *PodManifest will be returned, along with a pods.NoCurrentManifest
// error.
func (c consulStore) Pod(key string) (*pods.Manifest, time.Duration, error) {
	kvPair, writeMeta, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, 0, NewKVError("get", key, err)
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
func (c consulStore) ListPods(keyPrefix string) ([]ManifestResult, time.Duration, error) {
	kvPairs, writeMeta, err := c.client.KV().List(keyPrefix, nil)
	if err != nil {
		return nil, 0, NewKVError("list", keyPrefix, err)
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
func (c consulStore) WatchPods(keyPrefix string, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- ManifestResult) {
	defer close(podChan)

	var curIndex uint64 = 0

	for {
		select {
		case <-quitChan:
			return
		case <-time.After(1 * time.Second):
			pairs, meta, err := c.client.KV().List(keyPrefix, &api.QueryOptions{
				WaitIndex: curIndex,
			})
			if err != nil {
				errChan <- NewKVError("list", keyPrefix, err)
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
func (c consulStore) Ping() error {
	_, qm, err := c.client.Catalog().Nodes(&api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return NewKVError("ping", "/catalog/nodes", err)
	}
	if qm == nil || !qm.KnownLeader {
		return util.Errorf("No known leader")
	}
	return nil
}

func HealthPath(service, node string) string {
	if node == "" {
		return fmt.Sprintf("%s/%s", "health", service)
	}
	return fmt.Sprintf("%s/%s/%s", "health", service, node)
}
