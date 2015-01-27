package replication

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/allocation"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"gopkg.in/yaml.v2"
)

type fakeChecker struct {
	resp *health.ServiceStatus
	err  error
}

func (f *fakeChecker) LookupHealth(serviceID string) (*health.ServiceStatus, error) {
	return f.resp, f.err
}

type fakeIntent struct {
	releaseGroups        [][]string
	sleepTime            time.Duration
	concurrentWorkers    int
	maxConcurrentWorkers int
	counterMutex         sync.RWMutex
	workerCountExceeded  bool
	hostTrace            map[string]fakeIntentNodeInfo
	manifestUpdated      func(string, string)
}

// describes what was provided and what was happening at the time of the Set
type fakeIntentNodeInfo struct {
	activeHosts int
	sha         string
}

// Track the number of concurrent updates are allowed. the hostTrace can be inspected
// to see how many hosts were being updated at the time a particular host was updated.
func (i *fakeIntent) SetPod(node string, manifest pods.PodManifest) (time.Duration, error) {
	i.counterMutex.Lock()
	sha, _ := manifest.SHA()
	fmt.Printf("Setting %s to %s:%s\n", node, manifest.ID(), sha)
	i.concurrentWorkers = i.concurrentWorkers + 1
	i.hostTrace[node] = fakeIntentNodeInfo{
		activeHosts: i.concurrentWorkers,
		sha:         sha,
	}
	i.workerCountExceeded = i.workerCountExceeded || i.concurrentWorkers > i.maxConcurrentWorkers
	i.manifestUpdated(node, manifest.ID())
	i.counterMutex.Unlock()
	time.Sleep(i.sleepTime)
	i.counterMutex.Lock()
	defer i.counterMutex.Unlock()
	fmt.Println("Decrementing concurrent workers")
	i.concurrentWorkers = i.concurrentWorkers - 1
	return i.sleepTime, nil
}

func pausingIntentStore(maxConcurrentWorkers int, sleepTime time.Duration) *fakeIntent {
	return &fakeIntent{
		maxConcurrentWorkers: maxConcurrentWorkers,
		hostTrace:            make(map[string]fakeIntentNodeInfo),
		manifestUpdated: func(_, _ string) {
			// no-op
		},
	}
}

func podManifest(t *testing.T, serviceID string, version string) *pods.PodManifest {
	manifest, err := pods.PodManifestFromString(fmt.Sprintf(`
id: %s
launchables:
  web:
    type: hoist
    location: file:///foo_%s.tar.gz
config:
  foo: master
`, serviceID, version))
	Assert(t).IsNil(err, "Could not marshal manifest")
	return manifest
}

func serviceCheckerThatSays(t *testing.T, yamlRep string) *fakeChecker {
	var status health.ServiceStatus
	buf := bytes.Buffer{}
	buf.WriteString(yamlRep)
	err := yaml.Unmarshal(buf.Bytes(), &status)
	Assert(t).IsNil(err, fmt.Sprintf("Test setup err: \n%s\n is not valid JSON:", err))
	return &fakeChecker{&status, nil}
}

func fakeAllocation(nodes ...string) *allocation.Allocation {
	all := allocation.NewAllocation(nodes...)
	return &all
}

func TestDeployExistingAppWithThreeHealthyNodes(t *testing.T) {
	checker := serviceCheckerThatSays(t, `
statuses:
  host1.domain:
    node: host1.domain
    version: abc123
    healthy: true
  host2.domain:
    node: host2.domain
    version: abc123
    healthy: true
  host3.domain:
    node: host3.domain
    version: abc123
    healthy: true
`)
	allocated := fakeAllocation("host1.domain", "host2.domain", "host3.domain")
	manifest := podManifest(t, "foo", "def345")
	newManSha, _ := manifest.SHA()
	store := pausingIntentStore(2, 0)
	store.manifestUpdated = func(key, manId string) {
		node := strings.Split(key, "/")[1]
		_, err := checker.resp.ForNode(node)
		if err != nil {
			t.Fatalf("Wrong node updated: %s", node)
		}
		checker.resp.Statuses[node] = &health.ServiceNodeStatus{
			Version: newManSha,
			Node:    node,
			Healthy: true,
		}
	}

	replicator := NewReplicator(*manifest, *allocated)
	replicator.MinimumNodes = 1
	replicator.Logger = logging.TestLogger()
	replicator.NodePauseTime = 0
	stop := make(chan struct{})
	replicator.Enact(store, checker, stop)

	Assert(t).AreEqual(3, len(store.hostTrace), "3 hosts should have been updated")
}
