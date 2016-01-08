package replication

import (
	"fmt"
	"testing"

	"github.com/square/p2/pkg/consultest"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
)

var testNodes = []string{"node1", "node2"}

const (
	testLockMessage      = "lock is held by replicator_test.go"
	testPodId            = "test_pod"
	testPreparerManifest = `id: p2-preparer`
)

func testReplicatorAndServer(t *testing.T) (Replicator, kp.Store, consultest.Fixture) {
	active := 1
	store, f := makeStore(t)
	defer f.StopOnPanic()

	healthChecker := happyHealthChecker()
	threshold := health.Passing
	replicator, err := NewReplicator(
		basicManifest(),
		basicLogger(),
		testNodes,
		active,
		store,
		healthChecker,
		threshold,
		testLockMessage,
	)

	if err != nil {
		t.Fatalf("Unable to initialize replicator: %s", err)
	}
	return replicator, store, f
}

func makeStore(t *testing.T) (kp.Store, consultest.Fixture) {
	f := consultest.NewFixture(t)
	defer f.StopOnPanic()
	store := kp.NewConsulStore(f.Client)
	return store, f
}

// Adds preparer manifest to reality tree to fool replication library into
// thinking it is installed on the test nodes
func setupPreparers(fixture consultest.Fixture) {
	for _, node := range testNodes {
		key := fmt.Sprintf("reality/%s/p2-preparer", node)
		fixture.SetKV(key, []byte(testPreparerManifest))
	}
}

type alwaysHappyHealthChecker struct {
}

func (h alwaysHappyHealthChecker) WatchNodeService(
	nodename string,
	serviceID string,
	resultCh chan<- health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	happyResult := health.Result{
		ID:     testPodId,
		Status: health.Passing,
	}
	for {
		select {
		case <-quitCh:
			return
		case resultCh <- happyResult:
		}
	}
}

func (h alwaysHappyHealthChecker) Service(serviceID string) (map[string]health.Result, error) {
	results := make(map[string]health.Result)
	for _, node := range testNodes {
		results[node] = health.Result{
			ID:     testPodId,
			Status: health.Passing,
		}
	}
	return results, nil
}

func (h alwaysHappyHealthChecker) WatchService(
	serviceID string,
	resultCh chan<- map[string]health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	for {
		select {
		case <-quitCh:
			return
		case resultCh <- map[string]health.Result{}:
		}
	}
}

// creates an implementation of checker.ConsulHealthChecker that always reports
// satisfied health checks for testing purposes
func happyHealthChecker() checker.ConsulHealthChecker {
	return alwaysHappyHealthChecker{}
}

type channelBasedHealthChecker struct {
	// maps node name to a channel on which fake results can be provided
	resultsChans map[string]chan health.Result

	t *testing.T
}

// Pass along whatever results come through c.resultsChan
func (c channelBasedHealthChecker) WatchNodeService(
	nodename string,
	serviceID string,
	resultCh chan<- health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	inputCh, ok := c.resultsChans[nodename]
	if ok {
		for result := range inputCh {
			resultCh <- result
		}
	} else {
		c.t.Fatalf("No results channel configured for %s", nodename)
	}
}

// This is used by the initial health query in the replication library for
// sorting purposes, just return all healthy
func (c channelBasedHealthChecker) Service(serviceID string) (map[string]health.Result, error) {
	results := make(map[string]health.Result)
	for _, node := range testNodes {
		results[node] = health.Result{
			ID:     testPodId,
			Status: health.Passing,
		}
	}
	return results, nil
}

func (h channelBasedHealthChecker) WatchService(
	serviceID string,
	resultCh chan<- map[string]health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	for {
		select {
		case <-quitCh:
			return
		case resultCh <- map[string]health.Result{}:
		}
	}
}

// returns an implementation of checker.ConsulHealthChecker that will provide
// results based on what is passed on the returned  chanel
func channelHealthChecker(nodes []string, t *testing.T) (checker.ConsulHealthChecker, map[string]chan health.Result) {
	resultsChans := make(map[string]chan health.Result)
	for _, node := range nodes {
		resultsChans[node] = make(chan health.Result)
	}
	return channelBasedHealthChecker{
		resultsChans: resultsChans,
		t:            t,
	}, resultsChans
}

func basicLogger() logging.Logger {
	return logging.NewLogger(
		logrus.Fields{
			"pod": "testpod",
		},
	)
}

func basicManifest() pods.Manifest {
	builder := pods.NewManifestBuilder()
	builder.SetID(testPodId)
	return builder.GetManifest()
}
