package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	fake_checker "github.com/square/p2/pkg/health/checker/test"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/types"

	"github.com/Sirupsen/logrus"
)

var testNodes = []types.NodeName{"node1", "node2"}

const (
	testLockMessage      = "lock is held by replicator_test.go"
	testPodId            = "test_pod"
	testPreparerManifest = `id: p2-preparer`
)

func testReplicatorAndServer(t *testing.T) (Replicator, Store, consulutil.Fixture) {
	active := 1
	store, f := makeStore(t)

	healthChecker := fake_checker.HappyHealthChecker(testNodes)
	threshold := health.Passing
	replicator, err := NewReplicator(
		basicManifest(),
		basicLogger(),
		testNodes,
		active,
		store,
		f.Client.KV(),
		labels.NewConsulApplicator(f.Client, 1, 0),
		healthChecker,
		threshold,
		testLockMessage,
		NoTimeout,
		0,
	)

	if err != nil {
		t.Fatalf("Unable to initialize replicator: %s", err)
	}
	return replicator, store, f
}

func makeStore(t *testing.T) (Store, consulutil.Fixture) {
	f := consulutil.NewFixture(t)
	store := consul.NewConsulStore(f.Client)
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

type channelBasedHealthChecker struct {
	// maps node name to a channel on which fake results can be provided
	resultsChans chan map[types.NodeName]health.Result

	t *testing.T
}

// Pass along whatever results come through c.resultsChan
func (c channelBasedHealthChecker) WatchPodOnNode(
	nodeName types.NodeName,
	podID types.PodID,
	quitCh <-chan struct{},
) (chan health.Result, chan error) {
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
	ctx context.Context,
	serviceID string,
	resultCh chan<- map[types.NodeName]health.Result,
	errCh chan<- error,
	watchDelay time.Duration,
) {
	var results map[types.NodeName]health.Result
	select {
	case results = <-h.resultsChans:
	case <-ctx.Done():
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case results = <-h.resultsChans:
		case resultCh <- results:
		}
	}
}

func (h channelBasedHealthChecker) WatchHealth(
	resultCh chan []*health.Result,
	errCh chan<- error,
	quitCh <-chan struct{},
	_ time.Duration,
) {
	panic("not implemented")
}

// returns an implementation of checker.HealthChecker that will provide
// results based on what is passed on the returned  chanel
func channelHealthChecker(nodes []types.NodeName, t *testing.T) (checker.HealthChecker, chan map[types.NodeName]health.Result) {
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
