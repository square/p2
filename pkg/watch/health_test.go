package watch

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/Sirupsen/logrus"
	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
)

type MockHealthManager struct {
	UpdaterCreated int
}

func (m *MockHealthManager) Reset() {
	*m = MockHealthManager{}
}

func (m *MockHealthManager) NewUpdater(pod types.PodID, service string) consul.HealthUpdater {
	m.UpdaterCreated += 1
	return m
}

func (m *MockHealthManager) PutHealth(health consul.WatchResult) error {
	return fmt.Errorf("PutHealth() not implemented")
}

func (*MockHealthManager) Close() {}

// UpdatePods looks at the pods currently being monitored and
// compares that to what the reality store indicates should be
// running. UpdatePods then shuts down the monitors for dead
// pods and creates PodWatch structs for new pods.
func TestUpdatePods(t *testing.T) {
	var current []PodWatch
	var reality []consul.ManifestResult
	// ids for current: 0, 1, 2, 3
	for i := 0; i < 4; i++ {
		current = append(current, *newWatch(types.PodID(strconv.Itoa(i))))
	}
	// ids for reality: 1, 2, test
	for i := 1; i < 3; i++ {
		// Health checking is not supported for uuid pods, so ensure that even
		// if /reality contains a uuid pod we don't actually watch its health
		uuidKeyResult := newManifestResult("some_uuid_pod")
		uuidKeyResult.PodUniqueKey = types.NewPodUUID()
		reality = append(reality, uuidKeyResult)
		reality = append(reality, newManifestResult(current[i].manifest.ID()))
	}
	reality = append(reality, newManifestResult("test"))

	// ids for pods: 1, 2, test
	// 0, 3 should have values in their shutdownCh
	logger := logging.NewLogger(logrus.Fields{})
	pods := updatePods(&MockHealthManager{}, nil, nil, current, reality, "", &logger)
	Assert(t).AreEqual(true, <-current[0].shutdownCh, "this PodWatch should have been shutdown")
	Assert(t).AreEqual(true, <-current[3].shutdownCh, "this PodWatch should have been shutdown")

	Assert(t).AreEqual(current[1].manifest.ID(), pods[0].manifest.ID(), "pod with id:1 should have been returned")
	Assert(t).AreEqual(current[2].manifest.ID(), pods[1].manifest.ID(), "pod with id:1 should have been returned")
	Assert(t).AreEqual("test", string(pods[2].manifest.ID()), "should have added pod with id:test to list")
}

func TestUpdateStatus(t *testing.T) {
	logger := logging.TestLogger()
	healthManager := &MockHealthManager{}

	reality := []consul.ManifestResult{newManifestResult("foo"), newManifestResult("bar")}
	pods1 := updatePods(healthManager, nil, nil, []PodWatch{}, reality, "", &logger)
	Assert(t).AreEqual(2, len(pods1), "new pods were not added")
	Assert(t).AreEqual(2, healthManager.UpdaterCreated, "new pods did not create an updaters")

	// Change the status port, expect one pod to change
	healthManager.Reset()
	builder := reality[0].Manifest.GetBuilder()
	builder.SetStatusPort(2)
	reality[0].Manifest = builder.GetManifest()
	pods2 := updatePods(healthManager, nil, nil, pods1, reality, "", &logger)
	Assert(t).AreEqual(2, len(pods2), "updatePods() changed the number of pods")
	Assert(t).AreEqual(1, healthManager.UpdaterCreated, "one pod should have been refreshed")
}

func TestUpdatePath(t *testing.T) {
	logger := logging.TestLogger()
	healthManager := &MockHealthManager{}

	reality := []consul.ManifestResult{newManifestResult("foo"), newManifestResult("bar")}
	pods1 := updatePods(healthManager, nil, nil, []PodWatch{}, reality, "bobnode", &logger)
	Assert(t).AreEqual(2, len(pods1), "new pods were not added")
	Assert(t).AreEqual(2, healthManager.UpdaterCreated, "new pods did not create an updaters")

	// Change the status port, expect one pod to change
	healthManager.Reset()
	builder := reality[0].Manifest.GetBuilder()
	builder.SetStatusPath("/_foobar")
	reality[0].Manifest = builder.GetManifest()
	pods2 := updatePods(healthManager, nil, nil, pods1, reality, "bobnode", &logger)
	Assert(t).AreEqual(2, len(pods2), "updatePods() changed the number of pods")
	Assert(t).AreEqual(1, healthManager.UpdaterCreated, "one pod should have been refreshed")
	Assert(t).AreEqual("https://bobnode:1/_status", pods2[0].statusChecker.URI, "pod should be checking correct path")
	Assert(t).AreEqual("https://bobnode:1/_foobar", pods2[1].statusChecker.URI, "pod should be checking correct path")
}

func TestResultFromCheck(t *testing.T) {
	sc := StatusChecker{}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader([]byte(`HTTP/1.1 200 OK
Content-Length: 6
Content-Type: text/plain; charset=utf-8
Date: Thu, 05 Nov 2015 18:54:23 GMT

output`))), nil)
	Assert(t).IsNil(err, "should have had no error reading from bytes buffer")

	val, _ := sc.resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Passing, val.Status, "200 should correspond to health.Passing")

	resp.StatusCode = 282
	val, _ = sc.resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Passing, val.Status, "2** should correspond to health.Passing")

	resp.StatusCode = 1000000
	val, _ = sc.resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Critical, val.Status, "!2** should correspond to health.Critical")

	val, _ = sc.resultFromCheck(nil, nil)
	Assert(t).AreEqual(health.Critical, val.Status, "resp == nil should correspond to health.Critical")

	val, _ = sc.resultFromCheck(nil, fmt.Errorf("an error"))
	Assert(t).AreEqual(health.Critical, val.Status, "err != nil should correspond to health.Critical")
}

func newWatch(id types.PodID) *PodWatch {
	ch := make(chan bool, 1)
	return &PodWatch{
		manifest:   newManifestResult(id).Manifest,
		shutdownCh: ch,
	}
}

func newManifestResult(id types.PodID) consul.ManifestResult {
	builder := manifest.NewBuilder()
	builder.SetID(id)
	builder.SetStatusPort(1) // StatusPort must != 0 for updatePods to use it
	return consul.ManifestResult{
		Manifest: builder.GetManifest(),
	}
}
