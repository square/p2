package consultest

import (
	"path"
	"strings"
	"sync"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
)

// In memory consul store useful in tests. Currently does not implement the entire
// consul.Store interface
type FakePodStore struct {
	podResults    map[FakePodStoreKey]manifest.Manifest
	healthResults map[string]consul.WatchResult

	// represents locks that are held. Will be shared between any
	// fakeSessions returned by NewSession().  It is the session
	// implementation's responsibility to release locks when destroyed, and
	// to error when a lock is already held
	locks   map[string]bool
	locksMu sync.Mutex

	podLock sync.Mutex
}

func NewFakePodStore(podResults map[FakePodStoreKey]manifest.Manifest, healthResults map[string]consul.WatchResult) *FakePodStore {
	if podResults == nil {
		podResults = make(map[FakePodStoreKey]manifest.Manifest)
	}
	if healthResults == nil {
		healthResults = make(map[string]consul.WatchResult)
	}
	return &FakePodStore{
		podResults:    podResults,
		healthResults: healthResults,
		locks:         make(map[string]bool),
	}
}

type FakePodStoreKey struct {
	podPrefix consul.PodPrefix
	hostname  types.NodeName
	podId     types.PodID
}

func FakePodStoreKeyFor(podPrefix consul.PodPrefix, hostname types.NodeName, podId types.PodID) FakePodStoreKey {
	return FakePodStoreKey{
		podPrefix: podPrefix,
		hostname:  hostname,
		podId:     podId,
	}
}

func (f *FakePodStore) SetPod(podPrefix consul.PodPrefix, hostname types.NodeName, manifest manifest.Manifest) (time.Duration, error) {
	f.podLock.Lock()
	defer f.podLock.Unlock()
	f.podResults[FakePodStoreKeyFor(podPrefix, hostname, manifest.ID())] = manifest
	return 0, nil
}

func (f *FakePodStore) Pod(podPrefix consul.PodPrefix, hostname types.NodeName, podId types.PodID) (manifest.Manifest, time.Duration, error) {
	f.podLock.Lock()
	defer f.podLock.Unlock()
	if pod, ok := f.podResults[FakePodStoreKeyFor(podPrefix, hostname, podId)]; !ok {
		return nil, 0, pods.NoCurrentManifest
	} else {
		return pod, 0, nil
	}
}

func (f *FakePodStore) ListPods(podPrefix consul.PodPrefix, hostname types.NodeName) ([]consul.ManifestResult, time.Duration, error) {
	f.podLock.Lock()
	defer f.podLock.Unlock()
	res := make([]consul.ManifestResult, 0)
	for key, manifest := range f.podResults {
		if key.podPrefix == podPrefix && key.hostname == hostname {
			// TODO(mpuncel) make ManifestResult not contain the path, it's silly to have to do things like this
			path := path.Join(string(podPrefix), hostname.String(), string(manifest.ID()))
			uniqueKey, err := consul.PodUniqueKeyFromConsulPath(path)
			if err != nil {
				return nil, 0, err
			}

			res = append(res, consul.ManifestResult{
				Manifest: manifest,
				PodLocation: types.PodLocation{
					Node:  hostname,
					PodID: manifest.ID(),
				},
				PodUniqueKey: uniqueKey,
			})
		}
	}
	return res, 0, nil
}

func (f *FakePodStore) AllPods(podPrefix consul.PodPrefix) ([]consul.ManifestResult, time.Duration, error) {
	f.podLock.Lock()
	defer f.podLock.Unlock()
	res := make([]consul.ManifestResult, 0)
	for key, manifest := range f.podResults {
		if key.podPrefix != podPrefix {
			continue
		}
		path := path.Join(string(podPrefix), key.hostname.String(), string(manifest.ID()))
		uniqueKey, err := consul.PodUniqueKeyFromConsulPath(path)
		if err != nil {
			return nil, 0, err
		}

		res = append(res, consul.ManifestResult{
			Manifest: manifest,
			PodLocation: types.PodLocation{
				Node:  key.hostname,
				PodID: manifest.ID(),
			},
			PodUniqueKey: uniqueKey,
		})
	}
	return res, 0, nil
}

func (f *FakePodStore) DeletePod(podPrefix consul.PodPrefix, hostname types.NodeName, podId types.PodID) (time.Duration, error) {
	f.podLock.Lock()
	defer f.podLock.Unlock()
	delete(f.podResults, FakePodStoreKeyFor(podPrefix, hostname, podId))
	return 0, nil
}

func (f *FakePodStore) GetHealth(service string, node types.NodeName) (consul.WatchResult, error) {
	return f.healthResults[consul.HealthPath(service, node)], nil
}

func (f *FakePodStore) NewSession(name string, renewalCh <-chan time.Time) (consul.Session, chan error, error) {
	renewalErrCh := make(chan error)
	return newFakeSession(f.locks, &f.locksMu, renewalErrCh), renewalErrCh, nil
}

func (*FakePodStore) PutHealth(res consul.WatchResult) (time.Time, time.Duration, error) {
	panic("not implemented")
}

func (f *FakePodStore) GetServiceHealth(service string) (map[string]consul.WatchResult, error) {
	// Is this the best way to emulate recursive Consul queries?
	ret := map[string]consul.WatchResult{}
	prefix := consul.HealthPath(service, "")
	for key, v := range f.healthResults {
		if strings.HasPrefix(key, prefix) {
			ret[key] = v
		}
	}
	return ret, nil
}

func (*FakePodStore) WatchPod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- consul.ManifestResult) {
	panic("not implemented")
}

func (*FakePodStore) WatchPods(podPrefix consul.PodPrefix, nodename types.NodeName, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- []consul.ManifestResult) {
	panic("not implemented")
}

func (*FakePodStore) WatchAllPods(podPrefix consul.PodPrefix, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- []consul.ManifestResult, pauseTime time.Duration) {
	panic("not implemented")
}

func (*FakePodStore) Ping() error {
	panic("not implemented")
}

func (*FakePodStore) LockHolder(key string) (string, string, error) {
	// Not implemented -- for now, this will never produce an error
	return "Happy name", "Happy ID", nil
}

func (*FakePodStore) DestroyLockHolder(id string) error {
	// Not implemented -- for now, this will never produce an error
	return nil
}

func (*FakePodStore) NewUnmanagedSession(session string, name string) consul.Session {
	panic("not implemented")
}

func (*FakePodStore) NewHealthManager(node types.NodeName, logger logging.Logger) consul.HealthManager {
	panic("not implemented")
}
