package watch

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
)

// UpdatePods looks at the pods currently being monitored and
// compares that to what the reality store indicates should be
// running. UpdatePods then shuts down the monitors for dead
// pods and creates PodWatch structs for new pods.
func TestUpdatePods(t *testing.T) {
	var current []PodWatch
	var reality []kp.ManifestResult
	// ids for current: 0, 1, 2, 3
	for i := 0; i < 4; i++ {
		current = append(current, *newWatch(strconv.Itoa(i)))
	}
	// ids for reality: 1, 2, test
	for i := 1; i < 3; i++ {
		reality = append(reality, newManifestResult(current[i].manifest.Id))
	}
	reality = append(reality, newManifestResult("test"))

	// ids for pods: 1, 2, test
	// 0, 3 should have values in their shutdownCh
	pods := updatePods(current, reality, nil, nil, "")
	Assert(t).AreEqual(true, <-current[0].shutdownCh, "this PodWatch should have been shutdown")
	Assert(t).AreEqual(true, <-current[3].shutdownCh, "this PodWatch should have been shutdown")

	Assert(t).AreEqual(current[1].manifest.Id, pods[0].manifest.Id, "pod with id:1 should have been returned")
	Assert(t).AreEqual(current[2].manifest.Id, pods[1].manifest.Id, "pod with id:1 should have been returned")
	Assert(t).AreEqual("test", pods[2].manifest.Id, "should have added pod with id:test to list")
}

func TestUpdateNeeded(t *testing.T) {
	p := newWatch("test")
	ti := time.Now()
	p.lastCheck = ti
	p.lastStatus = health.Passing
	res := health.Result{
		Status: health.Critical,
	}
	Assert(t).AreEqual(true, p.updateNeeded(res, 1000), "should need update since Result.Status changed")

	res.Status = health.Passing
	Assert(t).AreEqual(true, p.updateNeeded(res, 0), "TTL is 0 so should always need update")
	Assert(t).AreEqual(false, p.updateNeeded(res, 1000), "TTL is >> than time since ti was created and status is unchanged")
}

func TestResultFromCheck(t *testing.T) {
	client := http.DefaultClient
	resp, _ := client.Get("http://ifconfig.co/all.json")
	val, _ := resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Passing, val.Status, "200 should correspond to health.Passing")

	resp.StatusCode = 282
	val, _ = resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Passing, val.Status, "2** should correspond to health.Passing")

	resp.StatusCode = 1000000
	val, _ = resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Critical, val.Status, "!2** should correspond to health.Critical")

	resp.StatusCode = 400
	val, _ = resultFromCheck(nil, nil)
	Assert(t).AreEqual(health.Critical, val.Status, "resp == nil should correspond to health.Critical")

	resp.StatusCode = 400
	err := fmt.Errorf("an error")
	val, _ = resultFromCheck(nil, err)
	Assert(t).AreEqual(health.Critical, val.Status, "err != nil should correspond to health.Critical")
}

func newWatch(id string) *PodWatch {
	ch := make(chan bool, 1)
	return &PodWatch{
		manifest:   newManifestResult(id).Manifest,
		shutdownCh: ch,
	}
}

func newManifestResult(id string) kp.ManifestResult {
	return kp.ManifestResult{
		Manifest: pods.Manifest{
			Id: id,
		},
	}
}
