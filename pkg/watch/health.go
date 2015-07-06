package watch

import (
	"net/http"
	"time"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util/net"
)

// number of milliseconds between reality store checks
const TIMEOUT = 1000

// number of milliseconds between health checks
const HEALTHCHECK_INTERVAL = 2000

// Contains method for watching the consul reality store to
// track services running on a node. Also contains manager
// method that performs health checks on the services the
// watch method indicates shoudl be running on the node

// PodWatch houses a pod's manifest, a channel to kill the
// pod's goroutine if the pod is removed from the reality
// tree, and a bool that indicates whether or not the pod
// has a running MonitorHealth go routine
type PodWatch struct {
	manifest   kp.ManifestResult
	shutdownCh chan bool
	hasMonitor bool // indicates whether this pod is being monitored
}

// WatchHealth is meant to be a long running go routine.
// WatchHealth reads from a consul store to determine which
// services should be running on the host. WatchHealth
// runs a CheckHealth routine to monitor the health of each
// service and kills routines for services that should no
// longer be running.
func WatchHealth(node, authtoken string) error {
	tochan := make(chan bool)
	// node is the address of the node whos services are
	// being watched. the node string is used to construct
	// curl commands that get status information for a service
	pods := []*PodWatch{}
	store := kp.NewConsulStore(kp.Options{
		Address: node,
		HTTPS:   true,
		Token:   authtoken,
		Client:  net.NewHeaderClient(nil, http.DefaultTransport),
	})

	go startTimer(tochan, TIMEOUT)
	for {
		select {
		case _ = <-tochan:
			err := updateHealthMonitors(store, pods, node)
			if err != nil {
				return err
			}
			// start timer again
			go startTimer(tochan, TIMEOUT)
		}
	}
	return nil
}

func (p *PodWatch) MonitorHealth(node string) {
	tochan := make(chan bool)
	go startTimer(tochan, HEALTHCHECK_INTERVAL)
	for {
		select {
		case _ = <-tochan:
			go checkHealth(node, p.manifest.Manifest.Id)
			go startTimer(tochan, HEALTHCHECK_INTERVAL)
		}
	}
}

// TODO performs health check for service and writes result to consul
func checkHealth(node, service string) {

}

func updateHealthMonitors(store kp.Store, pods []*PodWatch, node string) error {
	reality, _, err := store.ListPods(node)
	if err != nil {
		return err
	}
	// update list of pods to be monitored
	pods = updatePods(pods, reality)
	for _, pod := range pods {
		if pod.hasMonitor == false {
			go pod.MonitorHealth(node)
		}
	}
	return nil
}

// compares services being monitored with services that
// need to be monitored.
func updatePods(current []*PodWatch, reality []kp.ManifestResult) []*PodWatch {
	newCurrent := []*PodWatch{}
	// for pod in current if pod not in reality: kill
	for _, pod := range current {
		inReality := false
		for _, man := range reality {
			if pod.manifest.Path == man.Path {
				inReality = true
				break
			}
		}

		// if this podwatch is not in the reality store kill its go routine
		// else add this podwatch to newCurrent
		if inReality == false {
			pod.shutdownCh <- true
		} else {
			newCurrent = append(newCurrent, pod)
		}
	}
	// for pod in reality if pod not in current: create podwatch and
	// append to current
	for _, man := range reality {
		missing := true
		for _, pod := range current {
			if man.Path == pod.manifest.Path {
				missing = false
			}
		}

		// if a manifest is in reality but not current a podwatch is created
		// with that manifest and added to newCurrent
		if missing == true {
			newCurrent = append(newCurrent, &PodWatch{
				manifest:   man,
				shutdownCh: make(chan bool),
				hasMonitor: false,
			})
		}
	}
	return newCurrent
}

func startTimer(toChan chan bool, milliInterval time.Duration) {
	to := milliInterval * time.Millisecond
	time.Sleep(to)
	toChan <- true
}
