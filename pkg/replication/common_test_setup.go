package replication

import (
	"fmt"
	"io/ioutil"
	"sync/atomic"
	"testing"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/testutil"
)

var testNodes = []string{"node1", "node2"}

const (
	testLockMessage      = "lock is held by replicator_test.go"
	testPodId            = "test_pod"
	testPreparerManifest = `id: p2-preparer`
)

func testReplicatorAndServer(t *testing.T) (Replicator, kp.Store, *testutil.TestServer) {
	active := 1
	store, server := makeStore(t)

	healthChecker := happyHealthChecker()
	threshold := health.Passing
	return NewReplicator(
		basicManifest(),
		basicLogger(),
		testNodes,
		active,
		store,
		healthChecker,
		threshold,
		testLockMessage,
	), store, server
}

func makeStore(t *testing.T) (kp.Store, *testutil.TestServer) {
	if testing.Short() {
		t.Skip("skipping test dependendent on consul because of short mode")
	}

	defer func() {
		if t.Skipped() {
			t.Fatalf("test skipped by testutil package")
		}
	}()

	// Create server
	server := testutil.NewTestServerConfig(t, func(c *testutil.TestServerConfig) {
		// consul output in test output is noisy
		c.Stdout = ioutil.Discard
		c.Stderr = ioutil.Discard

		// If ports are left to their defaults, this test conflicts
		// with the test consul servers in pkg/kp
		var offset uint64
		idx := int(atomic.AddUint64(&offset, 1))
		c.Ports = &testutil.TestPortConfig{
			DNS:     26000 + idx,
			HTTP:    27000 + idx,
			RPC:     28000 + idx,
			SerfLan: 29000 + idx,
			SerfWan: 30000 + idx,
			Server:  31000 + idx,
		}
	})

	store := kp.NewConsulStore(kp.Options{
		Address: server.HTTPAddr,
	})
	return store, server
}

// Adds preparer manifest to reality tree to fool replication library into
// thinking it is installed on the test nodes
func setupPreparers(server *testutil.TestServer) {
	for _, node := range testNodes {
		key := fmt.Sprintf("reality/%s/p2-preparer", node)
		server.SetKV(key, []byte(testPreparerManifest))
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

// creates an implementation of health.ConsulHealthChecker that always reports
// satisfied health checks for testing purposes
func happyHealthChecker() health.ConsulHealthChecker {
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

// returns an implementation of health.ConsulHealthChecker that will provide
// results based on what is passed on the returned  chanel
func channelHealthChecker(nodes []string, t *testing.T) (health.ConsulHealthChecker, map[string]chan health.Result) {
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
	return pods.Manifest{
		Id: testPodId,
	}
}
