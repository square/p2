package watch

import (
	"bytes"
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util/net"
)

// These constants should probably all be something the p2 user can set
// in their preparer config...

// number of milliseconds between reality store checks
const POLL_KV_FOR_PODS = 4000

// number of milliseconds between health checks
const HEALTHCHECK_INTERVAL = 200

// TTL on healthchecks in seconds
const TTL = 120

// Contains method for watching the consul reality store to
// track services running on a node. A manager method:
// WatchHealth tracks the reality store and manages
// a health checking go routine for each service in the
// reality store

// PodWatch houses a pod's manifest, a channel to kill the
// pod's goroutine if the pod is removed from the reality
// tree, and a bool that indicates whether or not the pod
// has a running MonitorHealth go routine
type PodWatch struct {
	manifest kp.ManifestResult

	// For tracking/controlling the go routine that performs health checks
	// on the pod associated with this PodWatch
	shutdownCh chan bool
	hasMonitor bool // indicates whether this pod is being monitored

	// the fields are provided so it can be determined if new health checks
	// actually need to be sent to consul. If newT - oldT << TTL and status
	// has not changed there is no reason to update consul
	lastCheck  time.Time          // time of last health check
	lastStatus health.HealthState // status of last health check

	logger *logging.Logger
}

// WatchHealth is meant to be a long running go routine.
// WatchHealth reads from a consul store to determine which
// services should be running on the host. WatchHealth
// runs a CheckHealth routine to monitor the health of each
// service and kills routines for services that should no
// longer be running.
func WatchHealth(node, consul, authtoken string, logger *logging.Logger, shutdownCh chan struct{}) {
	var store kp.Store
	pods := []*PodWatch{}
	if authtoken != "" {
		store = kp.NewConsulStore(kp.Options{
			Address: consul,
			HTTPS:   false,
			Token:   authtoken,
			Client:  net.NewHeaderClient(nil, http.DefaultTransport),
		})
	} else {
		store = kp.NewConsulStore(kp.Options{
			Address: consul,
			HTTPS:   false,
			Client:  net.NewHeaderClient(nil, http.DefaultTransport),
		})
	}
	for {
		select {
		case _ = <-time.After(POLL_KV_FOR_PODS):
			// check if pods have been added or removed
			// starts monitor routine for new pods
			// kills monitor routine for removed pods
			pods = updateHealthMonitors(store, pods, node, logger)
		case _ = <-shutdownCh:
			return
		}
	}
}

// Monitor Health is a go routine that runs as long as the
// service it is monitoring. Every HEALTHCHECK_INTERVAL it
// performs a health check and writes that information to
// consul
func (p *PodWatch) MonitorHealth(node string, store kp.Store, shutdownCh chan bool) {
	statusCheck := fmt.Sprintf(kp.GetStatusCheck(), p.manifest.Manifest.StatusPort)
	for {
		select {
		case _ = <-time.After(POLL_KV_FOR_PODS):
			p.checkHealth(statusCheck, node, store)
		case _ = <-shutdownCh:
			return
		}
	}
}

func (p *PodWatch) checkHealth(healthCheck, node string, store kp.Store) {
	healthstate, res, err := check(healthCheck)
	health := health.Result{
		ID:      p.manifest.Manifest.Id,
		Node:    node,
		Service: p.manifest.Manifest.Id,
		Status:  healthstate,
		Output:  res,
	}

	if p.updateNeeded(health) == true {
		p.lastCheck, err = writeToConsul(health, store)
		p.lastStatus = health.Status
		if err != nil {
			p.logger.WithField("err", err).Fatalln("failed to write health data to consul")
		}
	}
}

// check is invoked periodically and runs the health check
// string c as a shell script
func check(c string) (health.HealthState, string, error) {
	output := new(bytes.Buffer)
	cmd, err := RunScript(c)

	cmd.Stdout = output
	cmd.Stderr = output
	err = cmd.Start()
	_ = cmd.Wait()
	if cmd.ProcessState.Success() == true {
		return health.Passing, output.String(), err
	} else {
		return health.Critical, output.String(), err
	}
}

// once we get health data we need to make a put request
// to consul to put the data in the KV Store
func writeToConsul(res health.Result, store kp.Store) (time.Time, error) {
	var value string
	// key =  service/node
	// if status == passing: value = status
	// else: values = health.Status/health.Output
	if res.Status == health.Passing {
		value = "passing"
	} else {
		value = fmt.Sprintf("%s/%s", res.Status, res.Output)
	}
	timeOfPut, _, err := store.PutHealth(res.Service, res.Node, value)
	return timeOfPut, err
}

func (p *PodWatch) updateNeeded(res health.Result) bool {
	// if status has changed indicate that consul needs to be updated
	if p.lastStatus != res.Status {
		return true
	}
	// if more than TTL / 4 seconds have elapsed since previous check
	// indicate that consul needs to be updated
	if time.Since(p.lastCheck) > TTL/4*time.Second {
		return true
	}

	return false
}

//
// Methods for tracking pods that should be monitored
//

func updateHealthMonitors(store kp.Store, watchedPods []*PodWatch, node string, logger *logging.Logger) []*PodWatch {
	path := kp.RealityPath(node)
	reality, _, err := store.ListPods(path)
	if err != nil {
		logger.WithField("inner_err", err).Warningln("failed to get pods from reality store")
	}

	// update list of pods to be monitored
	watchedPods = updatePods(watchedPods, reality, logger)
	for _, pod := range watchedPods {
		if pod.logger == nil {
			pod.logger = logger
		}
		if pod.hasMonitor == false {
			go pod.MonitorHealth(node, store, pod.shutdownCh)
		}
	}

	return watchedPods
}

// compares services being monitored with services that
// need to be monitored.
func updatePods(current []*PodWatch, reality []kp.ManifestResult, logger *logging.Logger) []*PodWatch {
	newCurrent := []*PodWatch{}
	// for pod in current if pod not in reality: kill
	for _, pod := range current {
		inReality := false
		for _, man := range reality {
			if man.Manifest.Id == pod.manifest.Manifest.Id {
				inReality = true
				break
			}
		}

		// if this podwatch is not in the reality store kill its go routine
		// else add this podwatch to newCurrent
		if inReality == false {
			pod.shutdownCh <- true
		} else {
			pod.hasMonitor = true
			newCurrent = append(newCurrent, pod)
		}
	}
	// for pod in reality if pod not in current: create podwatch and
	// append to current
	for _, man := range reality {
		missing := true
		for _, pod := range newCurrent {
			if man.Manifest.Id == pod.manifest.Manifest.Id {
				missing = false
				break
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

// After milliInterval milliseconds elapse a true value is placed in toChan.
// By waiting for a value in tochan (ie via a select statement), actions
// can be triggered on a given interval.
func startTimer(toChan chan bool, milliInterval time.Duration) {
	to := milliInterval * time.Millisecond
	time.Sleep(to)
	toChan <- true
}

func RunScript(script string) (*exec.Cmd, error) {
	shell := "/bin/sh"
	flag := "-c"
	cmd := exec.Command(shell, flag, script)
	return cmd, nil
}
