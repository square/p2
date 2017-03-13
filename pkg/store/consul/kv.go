// package consul provides a generalized API for reading and writing pod manifests
// in consul.
package consul

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/podstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

// Healthcheck TTL
const (
	TTL = 60 * time.Second

	// Don't change this, it affects where pod status keys are read and written from
	PreparerPodStatusNamespace statusstore.Namespace = "preparer"
)

type ManifestResult struct {
	Manifest    manifest.Manifest
	PodLocation types.PodLocation

	// This is expected to be nil for "legacy" pods that do not have a uuid, and non-nil for uuid pods.
	PodUniqueKey types.PodUniqueKey
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
	Time    time.Time
	Expires time.Time `json:"Expires,omitempty"`
}

// ValueEquiv returns true if the value of the WatchResult--everything except the
// timestamps--is equivalent to another WatchResult.
func (r WatchResult) ValueEquiv(s WatchResult) bool {
	return r.Id == s.Id &&
		r.Node == s.Node &&
		r.Service == s.Service &&
		r.Status == s.Status
}

// IsStale returns true when the result is stale according to the local clock.
func (r WatchResult) IsStale() bool {
	expires := r.Expires
	if expires.IsZero() {
		expires = r.Time.Add(TTL)
	}
	return time.Now().After(expires)
}

type PodStatusStore interface {
	GetStatusFromIndex(index podstore.PodIndex) (podstatus.PodStatus, *api.QueryMeta, error)
}

type consulStore struct {
	client consulutil.ConsulClient

	// The /intent tree can now contain pods that have UUID keys, which
	// means they are managed by a podstore.Store interface. The /intent
	// tree will contain indices that refer to a pod there, and when we
	// find one we'll have to ask the pod store for the full pod info
	podStore podstore.Store

	// The /reality tree can now contain pods that have UUID keys, which
	// means the reality manifest must be fetched from the pod status store
	podStatusStore PodStatusStore
}

func NewConsulStore(client consulutil.ConsulClient) *consulStore {
	statusStore := statusstore.NewConsul(client)
	podStatusStore := podstatus.NewConsul(statusStore, PreparerPodStatusNamespace)
	podStore := podstore.NewConsul(client.KV())
	return &consulStore{
		client:         client,
		podStore:       podStore,
		podStatusStore: podStatusStore,
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
	key := HealthPath(service, "/")
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
	manifest, err := manifest.FromBytes(kvPair.Value)
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
		result, err := c.manifestResultFromPair(kvp)
		if err != nil {
			// Just list all the pods that we can
			continue
		}

		ret = append(ret, result)
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
		out := ManifestResult{}
		if pair != nil {
			out, err = c.manifestResultFromPair(pair)
			if err != nil {
				select {
				case <-quitChan:
					return
				case errChan <- util.Errorf("Could not parse pod manifest at %s: %s. Content follows: \n%s", pair.Key, err, pair.Value):
					continue
				}
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
	go consulutil.WatchPrefix(keyPrefix, c.client.KV(), kvPairsChan, quitChan, errChan, 0)
	for kvPairs := range kvPairsChan {
		manifests := make([]ManifestResult, 0, len(kvPairs))
		for _, pair := range kvPairs {
			manifestResult, err := c.manifestResultFromPair(pair)
			if err != nil {
				select {
				case <-quitChan:
					return
				case errChan <- util.Errorf("Could not parse pod manifest at %s: %s. Content follows: \n%s", pair.Key, err, pair.Value):
				}
			} else {
				manifests = append(manifests, manifestResult)
			}
		}
		select {
		case <-quitChan:
			return
		case podChan <- manifests:
		}
	}
}

// Does the same thing as WatchPods, but does so on all the nodes instead
func (c consulStore) WatchAllPods(
	podPrefix PodPrefix,
	quitChan <-chan struct{},
	errChan chan<- error,
	podChan chan<- []ManifestResult,
	pauseTime time.Duration,
) {
	defer close(podChan)

	kvPairsChan := make(chan api.KVPairs)
	go consulutil.WatchPrefix(string(podPrefix), c.client.KV(), kvPairsChan, quitChan, errChan, pauseTime)
	for kvPairs := range kvPairsChan {
		manifests := make([]ManifestResult, 0, len(kvPairs))
		for _, pair := range kvPairs {
			manifestResult, err := c.manifestResultFromPair(pair)
			if err != nil {
				select {
				case <-quitChan:
					return
				case errChan <- util.Errorf("Could not parse pod manifest at %s: %s. Content follows: \n%s", pair.Key, err, pair.Value):
				}
			} else {
				manifests = append(manifests, manifestResult)
			}
		}
		select {
		case <-quitChan:
			return
		case podChan <- manifests:
		}
	}
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

// Now both pod manifests and indexes may be present in the /intent and
// /reality trees. When an index is present in the /intent tree, the pod store
// must be consulted with the PodUniqueKey to get the manifest the pod was
// scheduled with. In the /reality tree, the pod status store is instead
// consulted.  This function wraps the logic to fetch from the correct place
// based on the key namespace
func (c consulStore) manifestAndNodeFromIndex(pair *api.KVPair) (manifest.Manifest, types.NodeName, error) {
	var podIndex podstore.PodIndex
	err := json.Unmarshal(pair.Value, &podIndex)
	if err != nil {
		return nil, "", util.Errorf("Could not parse '%s' as pod index", pair.Key)
	}

	switch {
	case strings.HasPrefix(pair.Key, INTENT_TREE.String()):
		// fetch from pod store
		// TODO: add caching to pod store, since we're going to be doing a
		// query per index now. Or wait til consul 0.7 and use batch fetch
		pod, err := c.podStore.ReadPodFromIndex(podIndex)
		if err != nil {
			return nil, "", err
		}

		return pod.Manifest, pod.Node, nil
	case strings.HasPrefix(pair.Key, REALITY_TREE.String()):
		status, _, err := c.podStatusStore.GetStatusFromIndex(podIndex)
		if err != nil {
			return nil, "", err
		}
		manifest, err := manifest.FromBytes([]byte(status.Manifest))
		if err != nil {
			return nil, "", err
		}

		// We don't write the node into the pod status object, but we can
		// infer it from the key anyway
		node, err := extractNodeFromKey(pair.Key)
		if err != nil {
			return nil, "", err
		}
		return manifest, node, nil
	default:
		return nil, "", util.Errorf("Cannot determine key prefix for %s, expected %s or %s", pair.Key, INTENT_TREE, REALITY_TREE)
	}
}

func (c consulStore) manifestResultFromPair(pair *api.KVPair) (ManifestResult, error) {
	// As we transition from legacy pods to uuid pods, the /intent and
	// /reality trees will contain both manifests (as they always have) and
	// uuids which refer to consul objects elsewhere in KV tree. Therefore
	// we have to be able to tell which it is based on the key path
	podUniqueKey, err := PodUniqueKeyFromConsulPath(pair.Key)
	if err != nil {
		return ManifestResult{}, err
	}

	var podManifest manifest.Manifest
	var node types.NodeName
	if podUniqueKey != "" {
		var podIndex podstore.PodIndex
		err := json.Unmarshal(pair.Value, &podIndex)
		if err != nil {
			return ManifestResult{}, util.Errorf("Could not parse '%s' as pod index", pair.Key)
		}

		podManifest, node, err = c.manifestAndNodeFromIndex(pair)
		if err != nil {
			return ManifestResult{}, err
		}
	} else {
		podManifest, err = manifest.FromBytes(pair.Value)
		if err != nil {
			return ManifestResult{}, err
		}

		node, err = extractNodeFromKey(pair.Key)
		if err != nil {
			return ManifestResult{}, err
		}
	}

	return ManifestResult{
		Manifest: podManifest,
		PodLocation: types.PodLocation{
			Node:  node,
			PodID: podManifest.ID(),
		},
		PodUniqueKey: podUniqueKey,
	}, nil
}

func extractNodeFromKey(key string) (types.NodeName, error) {
	keyParts := strings.Split(key, "/")

	if len(keyParts) == 0 {
		return "", util.Errorf("Malformed key '%s'", key)
	}

	if keyParts[0] == "hooks" {
		// Hooks are allowed to not have a node, everything else should
		return "", nil
	}

	// A key should look like intent/<node>/<pod_id> OR intent/<node>/<uuid> for example
	if len(keyParts) != 3 {
		return "", util.Errorf("Malformed key '%s'", key)
	}

	return types.NodeName(keyParts[1]), nil
}

// Deduces a PodUniqueKey from a consul path. This is useful as pod keys are transitioned
// from using node name and pod ID to using UUIDs.
// Input is expected to have 3 '/' separated sections, e.g. 'intent/<node>/<pod_id>' or
// 'intent/<node>/<pod_uuid>' if the prefix is "intent" or "reality"
//
// /hooks is also a valid pod prefix and the key under it will not be a uuid.
func PodUniqueKeyFromConsulPath(consulPath string) (types.PodUniqueKey, error) {
	keyParts := strings.Split(consulPath, "/")
	if len(keyParts) == 0 {
		return "", util.Errorf("Malformed key '%s'", consulPath)
	}

	if keyParts[0] == "hooks" {
		return "", nil
	}

	if len(keyParts) != 3 {
		return "", util.Errorf("Malformed key '%s'", consulPath)
	}

	// Unforunately we can't use consul.INTENT_TREE and consul.REALITY_TREE here because of an import cycle
	if keyParts[0] != "intent" && keyParts[0] != "reality" {
		return "", util.Errorf("Unrecognized key tree '%s' (must be intent or reality)", keyParts[0])
	}

	// Parse() returns nil if the input string does not match the uuid spec
	podUniqueKey, err := types.ToPodUniqueKey(keyParts[2])
	switch {
	case err == types.InvalidUUID:
		// this is okay, it's just a legacy pod
		podUniqueKey = ""
	case err != nil:
		return "", util.Errorf("Could not test whether %s is a valid pod unique key: %s", keyParts[2], err)
	}

	return podUniqueKey, nil
}
