package watch

import (
	"bytes"
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util/net"
)

// number of milliseconds between reality store checks
const POLL_KV_FOR_PODS = 2000

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
func WatchHealth(node, consul, authtoken string, shutdownCh chan struct{}) error {
	tochan := make(chan bool)
	pods := []*PodWatch{}
	store := kp.NewConsulStore(kp.Options{
		Address: consul,
		HTTPS:   true,
		Token:   authtoken,
		Client:  net.NewHeaderClient(nil, http.DefaultTransport),
	})

	go startTimer(tochan, POLL_KV_FOR_PODS)
	for {
		select {
		case _ = <-tochan:
			err := updateHealthMonitors(store, pods, node)
			if err != nil {
				return err
			}
			// start timer again
			go startTimer(tochan, POLL_KV_FOR_PODS)
		case _ = <-shutdownCh:
			return nil
		}
	}
}

// Monitor Health is a go routine that runs as long as the
// service it is monitoring. Every HEALTHCHECK_INTERVAL it
// performs a health check and writes that information to
// consul
func (p *PodWatch) MonitorHealth(node string, store kp.Store) {
	statusCheck := fmt.Sprintf(kp.GetStatusCheck(), p.manifest.Manifest.StatusPort)
	tochan := make(chan bool)

	go startTimer(tochan, HEALTHCHECK_INTERVAL)
	for {
		select {
		case _ = <-tochan:
			go p.checkHealth(statusCheck, node, store)
			go startTimer(tochan, HEALTHCHECK_INTERVAL)
		}
	}
}

func (p *PodWatch) checkHealth(healthCheck, node string, store kp.Store) {
	healthstate, res, err := check(healthCheck)
	if err != nil {
		// TODO
	}
	health := health.Result{
		ID:      p.manifest.Manifest.Id,
		Node:    node,
		Service: p.manifest.Manifest.Id,
		Status:  healthstate,
		Output:  res,
	}
	err = writeToConsul(health, store)
	if err != nil {
		// TODO
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
	if err != nil {
		return "", "", err
	}
	err = cmd.Wait()
	if err != nil {
		return "", "", err
	}

	if cmd.ProcessState.Success() == true {
		return health.Passing, output.String(), nil
	} else {
		return health.Critical, output.String(), nil
	}
}

// once we get health data we need to make a put request
// to consul to put the data in the KV Store
func writeToConsul(res health.Result, store kp.Store) error {
	var value string
	// key =  service/node
	// if status == passing: value = status
	// else: values = health.Status/health.Output
	key := fmt.Sprintf("%s/%s", res.Service, res.Node)
	if res.Status == health.Passing {
		value = "passing"
	} else {
		value = fmt.Sprintf("%s/%s", res.Status, res.Output)
	}
	_, err := store.Put(key, value)
	if err != nil {
		return err
	}
	return nil
}

//
// Methods for tracking pods that should be monitored
//

func updateHealthMonitors(store kp.Store, pods []*PodWatch, node string) error {
	reality, _, err := store.ListPods(node) // TODO ensure path (node) is correct
	if err != nil {
		return err
	}
	// update list of pods to be monitored
	pods = updatePods(pods, reality)
	for _, pod := range pods {
		if pod.hasMonitor == false {
			go pod.MonitorHealth(node, store)
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
