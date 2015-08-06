package watch

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
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
	logger := logging.NewLogger(logrus.Fields{})
	pods := updatePods(nil, nil, current, reality, "", &logger)
	Assert(t).AreEqual(true, <-current[0].shutdownCh, "this PodWatch should have been shutdown")
	Assert(t).AreEqual(true, <-current[3].shutdownCh, "this PodWatch should have been shutdown")

	Assert(t).AreEqual(current[1].manifest.Id, pods[0].manifest.Id, "pod with id:1 should have been returned")
	Assert(t).AreEqual(current[2].manifest.Id, pods[1].manifest.Id, "pod with id:1 should have been returned")
	Assert(t).AreEqual("test", pods[2].manifest.Id, "should have added pod with id:test to list")
}

func TestUpdateNeeded(t *testing.T) {
	p := PodWatch{
		lastCheck:  time.Now(),
		lastStatus: health.Passing,
	}
	res := health.Result{
		Status: health.Critical,
	}
	Assert(t).AreEqual(true, p.updateNeeded(res, 1000), "should need update since Result.Status changed")

	res.Status = health.Passing
	Assert(t).AreEqual(true, p.updateNeeded(res, 0), "TTL is 0 so should always need update")
	Assert(t).AreEqual(false, p.updateNeeded(res, 1000), "TTL is >> than time since ti was created and status is unchanged")
}

func TestResultFromCheck(t *testing.T) {
	http.HandleFunc("/_status", statusHandler)
	go http.ListenAndServe(":8080", nil)
	client := http.DefaultClient
	sc := StatusChecker{
		ID:   "hello",
		Node: "localhost:8080",
	}

	resp, _ := client.Get("http://localhost:8080/_status")
	val, _ := sc.resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Passing, val.Status, "200 should correspond to health.Passing")

	resp.StatusCode = 282
	val, _ = sc.resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Passing, val.Status, "2** should correspond to health.Passing")

	resp.StatusCode = 1000000
	val, _ = sc.resultFromCheck(resp, nil)
	Assert(t).AreEqual(health.Critical, val.Status, "!2** should correspond to health.Critical")

	resp.StatusCode = 400
	val, _ = sc.resultFromCheck(nil, nil)
	Assert(t).AreEqual(health.Critical, val.Status, "resp == nil should correspond to health.Critical")

	resp.StatusCode = 400
	err := fmt.Errorf("an error")
	val, _ = sc.resultFromCheck(nil, err)
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
			Id:         id,
			StatusPort: 1, // StatusPort must != 0 for updatePods to use it
		},
	}
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Handler: statusHandler")
	fmt.Fprintf(w, "ok")
}
