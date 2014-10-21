// Package intent provides a kv-store agnostic way to watch a path for changes to
// a collection of pods.
package intent

import (
	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/kv-consul"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type WatchOptions struct {
	Token string
}

type IntentWatcher struct {
	Opts       WatchOptions
	ConsulOpts *consulapi.Config
	WatchFn    func(ppkv.KV, string, consulapi.QueryOptions, chan<- consulapi.KVPairs, chan<- error, <-chan struct{})
}

func NewWatcher(opts WatchOptions) *IntentWatcher {
	return &IntentWatcher{
		Opts:       opts,
		ConsulOpts: consulapi.DefaultConfig(),
		WatchFn:    ppkv.Watch,
	}
}

// Watch the kv-store for changes to any pod under the given path. All pods will be returned
// if any of them change; it is up to the client to ignore unchanged pods sent via this channel.
// Clients that respond to changes to pod manifests should be capable
// of acting on multiple changes at once. The quit channel is used to terminate watching on the
// spawned goroutine. The error channel should be observed for errors from the underlying watcher.
// If an error occurs during watch, it is the caller's responsibility to quit the watcher.
func (i *IntentWatcher) WatchPods(path string, quit <-chan struct{}, errChan chan<- error, podCh chan<- pods.PodManifest) error {
	client, err := consulapi.NewClient(i.ConsulOpts)
	if err != nil {
		return util.Errorf("Could not initialize consul client: %s", err)
	}
	opts := consulapi.QueryOptions{Token: i.Opts.Token}

	defer close(podCh)
	defer close(errChan)

	kvPairCh := make(chan consulapi.KVPairs)
	kvQuitCh := make(chan struct{})
	defer close(kvQuitCh)
	kvErrCh := make(chan error)
	go i.WatchFn(client.KV(), path, opts, kvPairCh, kvErrCh, kvQuitCh)

	for {
		select {
		case <-quit:
			kvQuitCh <- struct{}{}
			return nil
		case err := <-kvErrCh:
			errChan <- err
		case rawManifests := <-kvPairCh:
			for _, pair := range rawManifests {
				manifest, err := pods.PodManifestFromBytes(pair.Value)
				if err != nil {
					errChan <- util.Errorf("Could not parse pod manifest at %s: %s", pair.Key, err)
				} else {
					podCh <- *manifest
				}
			}
		}
	}

}
