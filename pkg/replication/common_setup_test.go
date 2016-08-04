package replication

import (
	"fmt"
	"testing"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"

	"github.com/Sirupsen/logrus"
)

var testNodes = []types.NodeName{"node1", "node2"}

const (
	testLockMessage      = "lock is held by replicator_test.go"
	testPodId            = "test_pod"
	testPreparerManifest = `id: p2-preparer`
)

func testReplicatorAndServer(t *testing.T) (Replicator, kp.Store, consulutil.Fixture) {
	active := 1
	store, f := makeStore(t)

	healthChecker := happyHealthChecker()
	threshold := health.Passing
	replicator, err := NewReplicator(
		basicManifest(),
		basicLogger(),
		testNodes,
		active,
		store,
		labels.NewConsulApplicator(f.Client, 1),
		healthChecker,
		threshold,
		testLockMessage,
	)

	if err != nil {
		t.Fatalf("Unable to initialize replicator: %s", err)
	}
	return replicator, store, f
}

func makeStore(t *testing.T) (kp.Store, consulutil.Fixture) {
	f := consulutil.NewFixture(t)
	store := kp.NewConsulStore(f.Client)
	return store, f
}

// Adds preparer manifest to reality tree to fool replication library into
// thinking it is installed on the test nodes
func setupPreparers(fixture consulutil.Fixture) {
	for _, node := range testNodes {
		key := fmt.Sprintf("reality/%s/p2-preparer", node)
		fixture.SetKV(key, []byte(testPreparerManifest))
	}
}

// TODO: these health checkers could be move to the health/checker/test package.

type alwaysHappyHealthChecker struct {
	allNodes []types.NodeName
}

func (h alwaysHappyHealthChecker) WatchNodeService(
	nodeName types.NodeName,
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

func (h alwaysHappyHealthChecker) Service(serviceID string) (map[types.NodeName]health.Result, error) {
	results := make(map[types.NodeName]health.Result)
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
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	allHappy := make(map[types.NodeName]health.Result)
	for _, node := range h.allNodes {
		allHappy[node] = health.Result{
			ID:     testPodId,
			Status: health.Passing,
		}
	}
	for {
		select {
		case <-quitCh:
			return
		case resultCh <- allHappy:
		}
	}
}

func (h alwaysHappyHealthChecker) WatchHealth(_ chan []*health.Result,
	errCh chan<- error,
	quitCh <-chan struct{}) {
	panic("not implemented")
}

// creates an implementation of checker.ConsulHealthChecker that always reports
// satisfied health checks for testing purposes
func happyHealthChecker() checker.ConsulHealthChecker {
	return alwaysHappyHealthChecker{testNodes}
}

type channelBasedHealthChecker struct {
	// maps node name to a channel on which fake results can be provided
	resultsChans chan map[types.NodeName]health.Result

	t *testing.T
}

// Pass along whatever results come through c.resultsChan
func (c channelBasedHealthChecker) WatchNodeService(
	nodeName types.NodeName,
	serviceID string,
	resultCh chan<- health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	panic("not implemented")
}

// This is used by the initial health query in the replication library for
// sorting purposes, just return all healthy
func (c channelBasedHealthChecker) Service(serviceID string) (map[types.NodeName]health.Result, error) {
	results := make(map[types.NodeName]health.Result)
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
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
) {
	var results map[types.NodeName]health.Result
	select {
	case results = <-h.resultsChans:
	case <-quitCh:
		return
	}
	for {
		select {
		case <-quitCh:
			return
		case results = <-h.resultsChans:
		case resultCh <- results:
		}
	}
}

func (h channelBasedHealthChecker) WatchHealth(
	resultCh chan []*health.Result,
	errCh chan<- error,
	quitCh <-chan struct{}) {
	panic("not implemented")
}

// returns an implementation of checker.ConsulHealthChecker that will provide
// results based on what is passed on the returned  chanel
func channelHealthChecker(nodes []types.NodeName, t *testing.T) (checker.ConsulHealthChecker, chan map[types.NodeName]health.Result) {
	resultsChans := make(chan map[types.NodeName]health.Result)
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

func basicManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID(testPodId)
	return builder.GetManifest()
}
