package intent

import (
	"errors"
	"fmt"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/kv-consul"
	"github.com/square/p2/pkg/pods"
)

type fakeClient struct{}

func (f fakeClient) KV() *consulapi.KV {
	client, _ := consulapi.NewClient(consulapi.DefaultConfig())
	return client.KV()
}

func (f fakeClient) Agent() *consulapi.Agent {
	client, _ := consulapi.NewClient(consulapi.DefaultConfig())
	return client.Agent()
}

func makePodKv(key string, value string) *consulapi.KVPair {
	return &consulapi.KVPair{
		Key:   key,
		Value: []byte(value),
	}
}

func happyWatch(kv ppkv.KV, prefix string, opts consulapi.QueryOptions, kvCh chan<- consulapi.KVPairs, errCh chan<- error, quitCh <-chan struct{}) {
	for {
		kvPairs := consulapi.KVPairs{}
		kvPairs = append(kvPairs, makePodKv("foo", `id: thepod
launchables:
  my-app:
    launchable_type: hoist
    launchable_id: foo
    location: https://localhost:4444/foo/bar/baz/baz.tar.gz
config:
  ENVIRONMENT: staging
`,
		))
		select {
		case <-quitCh:
			return
		case kvCh <- kvPairs:
		}
	}
}

func partiallyHappyWatch(kv ppkv.KV, prefix string, opts consulapi.QueryOptions, kvCh chan<- consulapi.KVPairs, errCh chan<- error, quitCh <-chan struct{}) {
	for {
		kvPairs := consulapi.KVPairs{}
		kvPairs = append(kvPairs, makePodKv("foo", `id: thepod
launchables:
  my-app:
    launchable_type: hoist
    launchable_id: foo
    location: https://localhost:4444/foo/bar/baz/baz.tar.gz
config:
  ENVIRONMENT: staging
`,
		))
		kvPairs = append(kvPairs, makePodKv("invalid", "invalid"))
		select {
		case <-quitCh:
			return
		case kvCh <- kvPairs:
		}
	}
}

func errorWatch(kv ppkv.KV, prefix string, opts consulapi.QueryOptions, kvCh chan<- consulapi.KVPairs, errCh chan<- error, quitCh <-chan struct{}) {
	for {
		select {
		case <-quitCh:
			return
		case errCh <- errors.New("ERROR"):
		}
	}
}

func TestHappyPathPodWatch(t *testing.T) {
	i := Store{Options{}, consulapi.DefaultConfig(), happyWatch, fakeClient{}}

	path := fmt.Sprintf("%s/ama1.dfw.square", INTENT_TREE)
	quit := make(chan struct{})
	defer close(quit)
	errChan := make(chan error)
	podCh := make(chan pods.PodManifest)
	go i.WatchPods(path, quit, errChan, podCh)
	select {
	case err := <-errChan:
		t.Fatalf("Should not have resulted in an error: %s", err)
	case manifest := <-podCh:
		Assert(t).AreEqual("thepod", manifest.ID(), "The ID of the manifest should have matched the document")
	}
}

func TestErrorPath(t *testing.T) {
	i := Store{Options{}, consulapi.DefaultConfig(), errorWatch, fakeClient{}}

	path := fmt.Sprintf("%s/ama1.dfw.square", INTENT_TREE)
	quit := make(chan struct{})
	defer close(quit)
	errChan := make(chan error)
	podCh := make(chan pods.PodManifest)
	go i.WatchPods(path, quit, errChan, podCh)
	select {
	case err := <-errChan:
		Assert(t).AreEqual("ERROR", err.Error(), "The error should have been returned")
	case <-podCh:
		t.Fatal("Should not have received any manifests")
	}
}

// This tests the case where an error occurs when parsing a single
func TestErrorsAndPodsReturned(t *testing.T) {
	i := Store{Options{}, consulapi.DefaultConfig(), partiallyHappyWatch, fakeClient{}}

	path := fmt.Sprintf("%s/ama1.dfw.square", INTENT_TREE)
	quit := make(chan struct{})
	defer close(quit)
	errChan := make(chan error)
	podCh := make(chan pods.PodManifest)
	go i.WatchPods(path, quit, errChan, podCh)
	var foundErr, foundManifests bool
	x := 0
	for x < 2 {
		select {
		case err := <-errChan:
			Assert(t).IsNotNil(err, "The error should have been returned")
			foundErr = true
			x += 1
		case manifest := <-podCh:
			Assert(t).AreEqual("thepod", manifest.ID(), "The ID of the manifest should have matched the document")
			Assert(t).IsFalse(foundManifests, "should not have found more than one manifest")
			foundManifests = true
			x += 1
		}
	}
	Assert(t).IsTrue(foundErr, "Should have seen at least one parsing error")
	Assert(t).IsTrue(foundManifests, "Should have seen at least one manifest")
}
