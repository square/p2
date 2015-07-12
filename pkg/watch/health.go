package watch

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/util/net"
)

// These constants should probably all be something the p2 user can set
// in their preparer config...

// Duration between reality store checks
const POLL_KV_FOR_PODS = 3 * time.Second

// Duration between health checks
const HEALTHCHECK_INTERVAL = 1 * time.Second

// Healthcheck TTL
const TTL = 60 * time.Second

// Contains method for watching the consul reality store to
// track services running on a node. A manager method:
// MonitorPodHealth tracks the reality store and manages
// a health checking go routine for each service in the
// reality store

// PodWatch houses a pod's manifest, a channel to kill the
// pod's goroutine if the pod is removed from the reality
// tree, and a bool that indicates whether or not the pod
// has a running MonitorHealth go routine
type PodWatch struct {
	manifest pods.Manifest

	// For tracking/controlling the go routine that performs health checks
	// on the pod associated with this PodWatch
	shutdownCh chan bool

	// the fields are provided so it can be determined if new health checks
	// actually need to be sent to consul. If newT - oldT << TTL and status
	// has not changed there is no reason to update consul
	lastCheck  time.Time          // time of last health check
	lastStatus health.HealthState // status of last health check

	logger *logging.Logger
}

// MonitorPodHealth is meant to be a long running go routine.
// MonitorPodHealth reads from a consul store to determine which
// services should be running on the host. MonitorPodHealth
// runs a CheckHealth routine to monitor the health of each
// service and kills routines for services that should no
// longer be running.
func MonitorPodHealth(config *preparer.PreparerConfig, logger *logging.Logger, shutdownCh chan struct{}) {
	var store kp.Store

	consul := config.ConsulAddress
	node := config.NodeName
	pods := []PodWatch{}
	authtoken, err := preparer.LoadConsulToken(config.ConsulTokenPath)
	if err != nil {
		logger.WithField("inner_err", err).Warningln("Could not load consul token")
	}

	store = kp.NewConsulStore(kp.Options{
		Address: consul,
		HTTPS:   false,
		Token:   authtoken,
		Client:  net.NewHeaderClient(nil, http.DefaultTransport),
	})
	pods = updateHealthMonitors(store, pods, node, logger)
	for {
		select {
		case <-time.After(POLL_KV_FOR_PODS):
			// check if pods have been added or removed
			// starts monitor routine for new pods
			// kills monitor routine for removed pods
			pods = updateHealthMonitors(store, pods, node, logger)
		case <-shutdownCh:
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
		case <-time.After(HEALTHCHECK_INTERVAL):
			p.checkHealth(node, p.manifest.StatusPort, store)
		case <-shutdownCh:
			return
		}
	}
}

func (p *PodWatch) checkHealth(node string, port int, store kp.Store) {
	resp, err := kp.HttpStatusCheck(node, port)
	health, err := resultFromCheck(resp, err)
	if err != nil {
		return
	}
	health.ID = p.manifest.Id
	health.Node = node
	health.Service = p.manifest.Id

	if p.updateNeeded(health, TTL) {
		p.lastCheck, err = writeToConsul(health, store)
		p.lastStatus = health.Status
		if err != nil {
			p.logger.WithField("err", err).Warningln("failed to write health data to consul")
		}
	}
}

func updateHealthMonitors(store kp.Store, watchedPods []PodWatch, node string, logger *logging.Logger) []PodWatch {
	path := kp.RealityPath(node)
	reality, _, err := store.ListPods(path)
	if err != nil {
		logger.WithField("inner_err", err).Warningln("failed to get pods from reality store")
	}

	return updatePods(watchedPods, reality, logger, store, node)
}

func resultFromCheck(resp *http.Response, err error) (health.Result, error) {
	res := health.Result{}
	if err != nil || resp == nil {
		res.Status = health.Critical
		if err != nil {
			res.Output = err.Error()
		}
		return res, nil
	}

	res.Output, err = getBody(resp)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		res.Status = health.Passing
	} else {
		res.Status = health.Critical
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
	timeOfPut, _, err := store.PutHealth(resToKPRes(res))
	return timeOfPut, err
}

func resToKPRes(res health.Result) kp.WatchResult {
	return kp.WatchResult{
		Service: res.Service,
		Node:    res.Node,
		Id:      res.ID,
		Status:  string(res.Status),
		Output:  res.Output,
	}
}

func (p *PodWatch) updateNeeded(res health.Result, ttl time.Duration) bool {
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
func updatePods(current []PodWatch, reality []kp.ManifestResult, logger *logging.Logger, store kp.Store, node string) []PodWatch {
	newCurrent := []PodWatch{}
	// for pod in current if pod not in reality: kill
	for _, pod := range current {
		inReality := false
		for _, man := range reality {
			if man.Manifest.Id == pod.manifest.Id {
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
		for _, pod := range newCurrent {
			if man.Manifest.Id == pod.manifest.Id {
				missing = false
				break
			}
		}

		// if a manifest is in reality but not current a podwatch is created
		// with that manifest and added to newCurrent
		if missing == true {
			newPod := PodWatch{
				manifest:   man.Manifest,
				shutdownCh: make(chan bool, 1),
			}
			go newPod.MonitorHealth(node, store, newPod.shutdownCh)
			newCurrent = append(newCurrent, newPod)
		}
	}
	return newCurrent
}
