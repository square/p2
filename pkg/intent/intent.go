// Package intent provides a kv-store agnostic way to watch a path for changes to
// a collection of pods.
package intent

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/kv-consul"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

const INTENT_TREE string = "/intent"

type ConsulClient interface {
	KV() *consulapi.KV
}

type Options struct {
	Token   string
	Address string
}

type ManifestErr struct {
	Key     string
	Value   string
	Message string
}

func (m *ManifestErr) Error() string {
	return m.Message
}

type Store struct {
	Opts       Options
	ConsulOpts *consulapi.Config
	WatchFn    func(ppkv.KV, string, consulapi.QueryOptions, chan<- consulapi.KVPairs, chan<- error, <-chan struct{})
	client     ConsulClient
}

func LookupStore(opts Options) (*Store, error) {
	watcher := &Store{
		Opts:       opts,
		ConsulOpts: consulapi.DefaultConfig(),
		WatchFn:    ppkv.WatchKV,
	}
	watcher.ConsulOpts.Address = opts.Address
	var err error
	watcher.client, err = consulapi.NewClient(watcher.ConsulOpts)

	if err != nil {
		return nil, util.Errorf("Could not initialize consul client: %s", err)
	}
	return watcher, nil
}

// Watch the kv-store for changes to any pod under the given path. All pods will be returned
// if any of them change; it is up to the client to ignore unchanged pods sent via this channel.
// Clients that respond to changes to pod manifests should be capable
// of acting on multiple changes at once. The quit channel is used to terminate watching on the
// spawned goroutine. The error channel should be observed for errors from the underlying watcher.
// If an error occurs during watch, it is the caller's responsibility to quit the watcher.
func (i *Store) WatchPods(path string, quit <-chan struct{}, errChan chan<- error, podCh chan<- pods.PodManifest) error {
	opts := consulapi.QueryOptions{Token: i.Opts.Token}

	defer close(podCh)
	defer close(errChan)

	kvPairCh := make(chan consulapi.KVPairs)
	kvQuitCh := make(chan struct{})
	defer close(kvQuitCh)
	kvErrCh := make(chan error)
	go i.WatchFn(i.client.KV(), path, opts, kvPairCh, kvErrCh, kvQuitCh)

	for {
		select {
		case <-quit:
			kvQuitCh <- struct{}{}
			return nil
		case err := <-kvErrCh:
			errChan <- err
		case rawManifests := <-kvPairCh:
			for _, pair := range rawManifests {
				str := string(pair.Value)
				if len(str) == 0 {
					errChan <- util.Errorf("An empty string was returned for the manifest %s", pair.Key)
					continue
				}
				if str[0] == '"' { // escaped for JSON YAML leads and ends with a doublequote and has escaped newlines
					str = str[1 : len(str)-1] // remove leading, following double quotes from escaping
					str = strings.Replace(str, `\n`, "\n", -1)
				}
				manifest, err := pods.PodManifestFromString(str)
				if err != nil {
					errChan <- util.Errorf("Could not parse pod manifest at %s: %s. Content follows: \n%s", pair.Key, err, pair.Value)
				} else {
					podCh <- *manifest
				}
			}
		}
	}
}

func (i *Store) IntentKey(node string, manifest pods.PodManifest) string {
	return fmt.Sprintf("%s/%s/%s", INTENT_TREE, node, manifest.ID())
}

func (i *Store) SetPod(node string, manifest pods.PodManifest) (time.Duration, error) {
	buf := bytes.Buffer{}
	err := manifest.Write(&buf)
	if err != nil {
		return 0, err
	}
	keyPair := &consulapi.KVPair{
		Key:   i.IntentKey(node, manifest),
		Value: buf.Bytes(),
	}

	writeMeta, err := i.client.KV().Put(keyPair, nil)
	return writeMeta.RequestTime, err
}
