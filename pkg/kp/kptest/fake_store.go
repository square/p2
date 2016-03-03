package kptest

import (
	"path"
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
	}
}

type FakePodStoreKey struct {
	podPrefix kp.PodPrefix
	hostname  string
	podId     types.PodID
}

func FakePodStoreKeyFor(podPrefix kp.PodPrefix, hostname string, podId types.PodID) FakePodStoreKey {
	return FakePodStoreKey{
		podPrefix: podPrefix,
		hostname:  hostname,
		podId:     podId,
	}
}

func (f *FakePodStore) SetPod(podPrefix kp.PodPrefix, hostname string, manifest pods.Manifest) (time.Duration, error) {
	f.podResults[FakePodStoreKeyFor(podPrefix, hostname, manifest.ID())] = manifest
	return 0, nil
}

func (f *FakePodStore) Pod(podPrefix kp.PodPrefix, hostname string, podId types.PodID) (pods.Manifest, time.Duration, error) {
	if pod, ok := f.podResults[FakePodStoreKeyFor(podPrefix, hostname, podId)]; !ok {
		return nil, 0, pods.NoCurrentManifest
	} else {
		return pod, 0, nil
	}
}

func (f *FakePodStore) ListPods(podPrefix kp.PodPrefix, hostname string) ([]kp.ManifestResult, time.Duration, error) {
	res := make([]kp.ManifestResult, 0)
	for key, manifest := range f.podResults {
		if key.podPrefix == podPrefix && key.hostname == hostname {
			// TODO(mpuncel) make ManifestResult not contain the path, it's silly to have to do things like this
			path := path.Join(string(podPrefix), hostname, string(manifest.ID()))
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
		path := path.Join(string(podPrefix), key.hostname, string(manifest.ID()))
		res = append(res, kp.ManifestResult{
			Manifest: manifest,
			Path:     path,
		})
	}
	return res, 0, nil
}

func (f *FakePodStore) DeletePod(podPrefix kp.PodPrefix, hostname string, podId types.PodID) (time.Duration, error) {
	delete(f.podResults, FakePodStoreKeyFor(podPrefix, hostname, podId))
	return 0, nil
}

func (f *FakePodStore) GetHealth(service, node string) (kp.WatchResult, error) {
	return f.healthResults[kp.HealthPath(service, node)], nil
}

func (f *FakePodStore) NewSession(name string, renewalCh <-chan time.Time) (kp.Session, chan error, error) {
	renewalErrCh := make(chan error)
	close(renewalErrCh)
	return &fakeSession{
		locks: make(map[string]bool),
	}, renewalErrCh, nil
}

func (*FakePodStore) PutHealth(res kp.WatchResult) (time.Time, time.Duration, error) {
	panic("not implemented")
}

func (*FakePodStore) GetServiceHealth(service string) (map[string]kp.WatchResult, error) {
	panic("not implemented")
}

func (*FakePodStore) WatchPod(podPrefix kp.PodPrefix, nodename string, podId types.PodID, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- kp.ManifestResult) {
	panic("not implemented")
}

func (*FakePodStore) WatchPods(podPrefix kp.PodPrefix, nodename string, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- []kp.ManifestResult) {
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

func (*FakePodStore) NewHealthManager(node string, logger logging.Logger) kp.HealthManager {
	panic("not implemented")
}
