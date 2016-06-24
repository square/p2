package kptest

import (
	"path"
	"strings"
	"sync"
	"time"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
)

// In memory kp store useful in tests. Currently does not implement the entire
// kp.Store interface
type FakePodStore struct {
	podResults    map[FakePodStoreKey]pods.Manifest
	healthResults map[string]kp.WatchResult

	// represents locks that are held. Will be shared between any
	// fakeSessions returned by NewSession().  It is the session
	// implementation's responsibility to release locks when destroyed, and
	// to error when a lock is already held
	locks   map[string]bool
	locksMu sync.Mutex
}

var _ kp.Store = &FakePodStore{}

func NewFakePodStore(podResults map[FakePodStoreKey]pods.Manifest, healthResults map[string]kp.WatchResult) *FakePodStore {
	if podResults == nil {
		podResults = make(map[FakePodStoreKey]pods.Manifest)
	}
	if healthResults == nil {
		healthResults = make(map[string]kp.WatchResult)
	}
	return &FakePodStore{
		podResults:    podResults,
		healthResults: healthResults,
		locks:         make(map[string]bool),
	}
}

type FakePodStoreKey struct {
	podPrefix kp.PodPrefix
	hostname  types.NodeName
	podId     types.PodID
}

func FakePodStoreKeyFor(podPrefix kp.PodPrefix, hostname types.NodeName, podId types.PodID) FakePodStoreKey {
	return FakePodStoreKey{
		podPrefix: podPrefix,
		hostname:  hostname,
		podId:     podId,
	}
}

func (f *FakePodStore) SetPod(podPrefix kp.PodPrefix, hostname types.NodeName, manifest pods.Manifest) (time.Duration, error) {
	f.podResults[FakePodStoreKeyFor(podPrefix, hostname, manifest.ID())] = manifest
	return 0, nil
}

func (f *FakePodStore) Pod(podPrefix kp.PodPrefix, hostname types.NodeName, podId types.PodID) (pods.Manifest, time.Duration, error) {
	if pod, ok := f.podResults[FakePodStoreKeyFor(podPrefix, hostname, podId)]; !ok {
		return nil, 0, pods.NoCurrentManifest
	} else {
		return pod, 0, nil
	}
}

func (f *FakePodStore) ListPods(podPrefix kp.PodPrefix, hostname types.NodeName) ([]kp.ManifestResult, time.Duration, error) {
	res := make([]kp.ManifestResult, 0)
	for key, manifest := range f.podResults {
		if key.podPrefix == podPrefix && key.hostname == hostname {
			// TODO(mpuncel) make ManifestResult not contain the path, it's silly to have to do things like this
			path := path.Join(string(podPrefix), hostname.String(), string(manifest.ID()))
			res = append(res, kp.ManifestResult{
				Manifest: manifest,
				Path:     path,
			})
		}
	}
	return res, 0, nil
}

func (f *FakePodStore) AllPods(podPrefix kp.PodPrefix) ([]kp.ManifestResult, time.Duration, error) {
	res := make([]kp.ManifestResult, 0)
	for key, manifest := range f.podResults {
		if key.podPrefix != podPrefix {
			continue
		}
		path := path.Join(string(podPrefix), key.hostname.String(), string(manifest.ID()))
		res = append(res, kp.ManifestResult{
			Manifest: manifest,
			Path:     path,
		})
	}
	return res, 0, nil
}

func (f *FakePodStore) DeletePod(podPrefix kp.PodPrefix, hostname types.NodeName, podId types.PodID) (time.Duration, error) {
	delete(f.podResults, FakePodStoreKeyFor(podPrefix, hostname, podId))
	return 0, nil
}

func (f *FakePodStore) GetHealth(service string, node types.NodeName) (kp.WatchResult, error) {
	return f.healthResults[kp.HealthPath(service, node)], nil
}

func (f *FakePodStore) NewSession(name string, renewalCh <-chan time.Time) (kp.Session, chan error, error) {
	renewalErrCh := make(chan error)
	return newFakeSession(f.locks, f.locksMu, renewalErrCh), renewalErrCh, nil
}

func (*FakePodStore) PutHealth(res kp.WatchResult) (time.Time, time.Duration, error) {
	panic("not implemented")
}

func (f *FakePodStore) GetServiceHealth(service string) (map[string]kp.WatchResult, error) {
	// Is this the best way to emulate recursive Consul queries?
	ret := map[string]kp.WatchResult{}
	prefix := kp.HealthPath(service, "")
	for key, v := range f.healthResults {
		if strings.HasPrefix(key, prefix) {
			ret[key] = v
		}
	}
	return ret, nil
}

func (*FakePodStore) WatchPod(podPrefix kp.PodPrefix, nodename types.NodeName, podId types.PodID, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- kp.ManifestResult) {
	panic("not implemented")
}

func (*FakePodStore) WatchPods(podPrefix kp.PodPrefix, nodename types.NodeName, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- []kp.ManifestResult) {
	panic("not implemented")
}

func (*FakePodStore) Ping() error {
	panic("not implemented")
}

func (*FakePodStore) LockHolder(key string) (string, string, error) {
	panic("not implemented")
}

func (*FakePodStore) DestroyLockHolder(id string) error {
	panic("not implemented")
}

func (*FakePodStore) NewUnmanagedSession(session string, name string) kp.Session {
	panic("not implemented")
}

func (*FakePodStore) NewHealthManager(node types.NodeName, logger logging.Logger) kp.HealthManager {
	panic("not implemented")
}
