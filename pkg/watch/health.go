package watch

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util/param"
)

// These constants should probably all be something the p2 user can set
// in their preparer config...

// Duration between health checks
const HEALTHCHECK_INTERVAL = 1 * time.Second

// Maximum allowed time for a single check, in seconds
var HEALTHCHECK_TIMEOUT = param.Int64("healthcheck_timeout", 5)

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
	manifest      manifest.Manifest
	updater       kp.HealthUpdater
	statusChecker StatusChecker

	// For tracking/controlling the go routine that performs health checks
	// on the pod associated with this PodWatch
	shutdownCh chan bool

	logger *logging.Logger
}

// StatusChecker holds all the data required to perform
// a status check on a particular service
type StatusChecker struct {
	ID     types.PodID
	Node   types.NodeName
	URI    string
	Client *http.Client
}

type store interface {
	NewHealthManager(node types.NodeName, logger logging.Logger) kp.HealthManager
	WatchPods(podPrefix kp.PodPrefix, nodename types.NodeName, quitChan <-chan struct{}, errChan chan<- error, podChan chan<- []kp.ManifestResult)
}

// MonitorPodHealth is meant to be a long running go routine.
// MonitorPodHealth reads from a consul store to determine which
// services should be running on the host. MonitorPodHealth
// runs a CheckHealth routine to monitor the health of each
// service and kills routines for services that should no
// longer be running.
func MonitorPodHealth(config *preparer.PreparerConfig, logger *logging.Logger, shutdownCh chan struct{}) {
	client, err := config.GetConsulClient()
	if err != nil {
		// A bad config should have already produced a nice, user-friendly error message.
		logger.WithError(err).Fatalln("error creating health monitor KV client")
	}
	store := kp.NewConsulStore(client)
	healthManager := store.NewHealthManager(config.NodeName, *logger)

	node := config.NodeName
	pods := []PodWatch{}

	watchQuitCh := make(chan struct{})
	watchErrCh := make(chan error)
	watchPodCh := make(chan []kp.ManifestResult)
	go store.WatchPods(
		kp.REALITY_TREE,
		node,
		watchQuitCh,
		watchErrCh,
		watchPodCh,
	)

	// if GetClient fails it means the certfile/keyfile/cafile were
	// invalid or did not exist. It makes sense to throw a fatal error
	secureClient, err := config.GetClient(time.Duration(*HEALTHCHECK_TIMEOUT) * time.Second)
	if err != nil {
		logger.WithError(err).Fatalln("failed to get http client for this preparer")
	}

	insecureClient, err := config.GetInsecureClient(time.Duration(*HEALTHCHECK_TIMEOUT) * time.Second)
	if err != nil {
		logger.WithError(err).Fatalln("failed to get http client for this preparer")
	}

	for {
		select {
		case results := <-watchPodCh:
			// check if pods have been added or removed
			// starts monitor routine for new pods
			// kills monitor routine for removed pods
			pods = updatePods(healthManager, secureClient, insecureClient, pods, results, node, logger)
		case err := <-watchErrCh:
			logger.WithError(err).Errorln("there was an error reading reality manifests for health monitor")
		case <-shutdownCh:
			for _, pod := range pods {
				pod.shutdownCh <- true
			}
			close(watchQuitCh)
			healthManager.Close()
			return
		}
	}
}

// compares services being monitored with services that
// need to be monitored.
func updatePods(
	healthManager kp.HealthManager,
	secureClient *http.Client,
	insecureClient *http.Client,
	current []PodWatch,
	reality []kp.ManifestResult,
	node types.NodeName,
	logger *logging.Logger,
) []PodWatch {
	newCurrent := []PodWatch{}
	// for pod in current if pod not in reality: kill
	for _, pod := range current {
		inReality := false
		for _, man := range reality {
			if man.PodUniqueKey != nil {
				// We don't health check uuid pods
				continue
			}
			if man.Manifest.ID() == pod.manifest.ID() &&
				man.Manifest.GetStatusHTTP() == pod.manifest.GetStatusHTTP() &&
				man.Manifest.GetStatusLocalhostOnly() == pod.manifest.GetStatusLocalhostOnly() &&
				man.Manifest.GetStatusPath() == pod.manifest.GetStatusPath() &&
				man.Manifest.GetStatusPort() == pod.manifest.GetStatusPort() {
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
		if man.PodUniqueKey != nil {
			// We don't health check uuid pods
			continue
		}

		missing := true
		for _, pod := range newCurrent {
			if man.Manifest.ID() == pod.manifest.ID() {
				missing = false
				break
			}
		}

		var client *http.Client
		var statusHost types.NodeName
		if man.Manifest.GetStatusLocalhostOnly() {
			statusHost = "localhost"
			client = insecureClient
		} else {
			statusHost = node
			client = secureClient
		}

		// if a manifest is in reality but not current a podwatch is created
		// with that manifest and added to newCurrent
		if missing {
			sc := StatusChecker{
				ID:     man.Manifest.ID(),
				Node:   node,
				Client: client,
			}
			if man.Manifest.GetStatusPort() == 0 {
				sc.URI = ""
			} else if man.Manifest.GetStatusHTTP() {
				sc.URI = fmt.Sprintf("http://%s:%d%s", statusHost, man.Manifest.GetStatusPort(), man.Manifest.GetStatusPath())
			} else {
				sc.URI = fmt.Sprintf("https://%s:%d%s", statusHost, man.Manifest.GetStatusPort(), man.Manifest.GetStatusPath())
			}
			newPod := PodWatch{
				manifest:      man.Manifest,
				updater:       healthManager.NewUpdater(man.Manifest.ID(), string(man.Manifest.ID())),
				statusChecker: sc,
				shutdownCh:    make(chan bool, 1),
				logger:        logger,
			}

			// Each health monitor will have its own statusChecker
			go newPod.MonitorHealth()
			newCurrent = append(newCurrent, newPod)
		}
	}
	return newCurrent
}

// Monitor Health is a go routine that runs as long as the
// service it is monitoring. Every HEALTHCHECK_INTERVAL it
// performs a health check and writes that information to
// consul
func (p *PodWatch) MonitorHealth() {
	for {
		select {
		case <-time.After(HEALTHCHECK_INTERVAL):
			p.checkHealth()
		case <-p.shutdownCh:
			p.updater.Close()
			return
		}
	}
}

func (p *PodWatch) checkHealth() {
	health, err := p.statusChecker.Check()
	if err != nil {
		p.logger.WithError(err).Warningln("health check failed")
		return
	}

	if err = p.updater.PutHealth(resToKPRes(health)); err != nil {
		p.logger.WithError(err).Warningln("failed to write health")
	}
}

// Given the result of a status check this method
// creates a health.Result for that node/service/result
func (sc *StatusChecker) Check() (health.Result, error) {
	if sc.URI != "" {
		return sc.resultFromCheck(sc.StatusCheck())
	} else {
		// "unknown" is probably more accurate, but automated tools can't handle an app that is
		// always non-"passing". For instance, p2-replicate by default waits for a node to
		// become "passing" before it considers the deployment a success.
		//
		// TODO: P2 has the capacity to check whether the app's process is running. This would
		// make a great default status check! However, that information isn't easily accessible
		// over here in the watch package. It would take a lot of refactoring to make this
		// happen.
		return health.Result{
			ID:      sc.ID,
			Node:    sc.Node,
			Service: string(sc.ID),
			Status:  health.Passing,
		}, nil
	}
}

func (sc *StatusChecker) resultFromCheck(resp *http.Response, err error) (health.Result, error) {
	res := health.Result{
		ID:      sc.ID,
		Node:    sc.Node,
		Service: string(sc.ID),
	}
	if err != nil || resp == nil {
		res.Status = health.Critical
		return res, nil
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		res.Status = health.Passing
	} else {
		res.Status = health.Critical
	}
	return res, err
}

// Go version of http status check
func (sc *StatusChecker) StatusCheck() (*http.Response, error) {
	return sc.Client.Head(sc.URI)
}

func getBody(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func resToKPRes(res health.Result) kp.WatchResult {
	return kp.WatchResult{
		Service: res.Service,
		Node:    res.Node,
		Id:      res.ID,
		Status:  string(res.Status),
	}
}
