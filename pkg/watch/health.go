package watch

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/util/net"
)

// These constants should probably all be something the p2 user can set
// in their preparer config...

// number of milliseconds between reality store checks
const POLL_KV_FOR_PODS = 3000

// number of milliseconds between health checks
const HEALTHCHECK_INTERVAL = 5000

// TTL on healthchecks in seconds
const TTL = 60

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
func WatchHealth(config *preparer.PreparerConfig, logger *logging.Logger, shutdownCh chan struct{}) {
	var store kp.Store

	consul := config.ConsulAddress
	node := config.NodeName
	pods := []*PodWatch{}
	authtoken, err := preparer.LoadConsulToken(config.ConsulTokenPath)
	if err != nil {
		logger.WithField("inner_err", err).Warningln("Could not load consul token")
	}

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
	for {
		select {
		case _ = <-time.After(POLL_KV_FOR_PODS):
			p.checkHealth(node, p.manifest.Manifest.StatusPort, store)
		case _ = <-shutdownCh:
			return
		}
	}
}

func (p *PodWatch) checkHealth(node string, port int, store kp.Store) {
	resp, err := kp.HttpStatusCheck(node, port)
	if err != nil {
		p.logger.WithField("err", err).Warningln("Status check failed")
	}
	health, err := resultFromCheck(resp)
	if err != nil {
		p.logger.WithField("err", err).Warningln("Failed to read status check response")
	}
	health.ID = p.manifest.Manifest.Id
	health.Node = node
	health.Service = p.manifest.Manifest.Id

	if p.updateNeeded(health, TTL) == true {
		p.lastCheck, err = writeToConsul(health, store)
		p.lastStatus = health.Status
		if err != nil {
			p.logger.WithField("err", err).Warningln("failed to write health data to consul")
		}
	}
}

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

func resultFromCheck(resp *http.Response) (health.Result, error) {
	var err error
	res := health.Result{}
	if resp.StatusCode == 200 {
		res.Status = health.Passing
	} else if resp.StatusCode > 200 && resp.StatusCode < 300 {
		res.Status = health.Warning
		res.Output, err = getBody(resp)
	} else {
		res.Status = health.Critical
		res.Output, err = getBody(resp)
	}
	return res, err
}

func getBody(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// once we get health data we need to make a put request
// to consul to put the data in the KV Store
func writeToConsul(res health.Result, store kp.Store) (time.Time, error) {
	timeOfPut, _, err := store.PutHealth(resToMap(res))
	return timeOfPut, err
}

func resToMap(res health.Result) map[string]string {
	m := make(map[string]string)
	m["service"] = res.Service
	m["node"] = res.Node
	m["id"] = res.ID
	m["status"] = string(res.Status)
	m["output"] = res.Output

	return m
}

func (p *PodWatch) updateNeeded(res health.Result, ttl int) bool {
	// if status has changed indicate that consul needs to be updated
	if p.lastStatus != res.Status {
		return true
	}
	// if more than TTL / 4 seconds have elapsed since previous check
	// indicate that consul needs to be updated
	if time.Since(p.lastCheck) > time.Duration(ttl/4)*time.Second {
		return true
	}

	return false
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
				shutdownCh: make(chan bool, 1),
				hasMonitor: false,
			})
		}
	}
	return newCurrent
}
