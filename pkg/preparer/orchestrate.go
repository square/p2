package preparer

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/intent"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
)

type Pod interface {
	Launch(*pods.PodManifest) (bool, error)
	Install(*pods.PodManifest) error
	CurrentManifest() (*pods.PodManifest, error)
	Halt() (bool, error)
}

func WatchForPodManifestsForNode(nodeName string, consulAddress string, hooksDirectory string, logger logging.Logger) {
	pods.Log = logger // make it clear that it's
	hooks := pods.Hooks(hooksDirectory)
	watchOpts := intent.WatchOptions{
		Token:   nodeName,
		Address: consulAddress,
	} // placeholder for now
	watcher := intent.NewWatcher(watchOpts)

	path := fmt.Sprintf("nodes/%s", nodeName)

	// This allows us to signal the goroutine watching consul to quit
	watcherQuit := make(<-chan struct{})
	errChan := make(chan error)
	podChan := make(chan pods.PodManifest)

	go watcher.WatchPods(path, watcherQuit, errChan, podChan)

	// we will have one long running goroutine for each app installed on this
	// host. We keep a map of podId => podChan so we can send the new manifests
	// that come in to the appropriate goroutine
	podChanMap := make(map[string]chan pods.PodManifest)
	quitChanMap := make(map[string]chan struct{})

	for {
		select {
		case err := <-errChan:
			logger.WithFields(logrus.Fields{
				"inner_err": err,
			}).Errorln("there was an error reading the manifest")
		case manifest := <-podChan:
			podId := manifest.ID()
			if podChanMap[podId] == nil {
				// No goroutine is servicing this app currently, let's start one
				podChanMap[podId] = make(chan pods.PodManifest)
				quitChanMap[podId] = make(chan struct{})
				go handlePods(hooks, podChanMap[podId], quitChanMap[podId], logger)
			}
			podChanMap[podId] <- manifest
		}
	}
}

// no return value, no output channels. This should do everything it needs to do
// without outside intervention (other than being signalled to quit)
func handlePods(hooks *pods.HookDir, podChan <-chan pods.PodManifest, quit <-chan struct{}, logger logging.Logger) {
	// install new launchables
	var manifestToLaunch pods.PodManifest

	// used to track if we have work to do (i.e. pod manifest came through channel
	// and we have yet to operate on it)
	working := false
	var manifestLogger logging.Logger
	for {
		select {
		case <-quit:
			return
		case manifestToLaunch = <-podChan:
			sha, err := manifestToLaunch.SHA()
			manifestLogger = logger.SubLogger(logrus.Fields{
				"manifest": manifestToLaunch.ID(),
				"sha":      sha,
				"sha_err":  err,
			})
			manifestLogger.NoFields().Infoln("New manifest received")
			working = true
		case <-time.After(1 * time.Second):
			if working {
				pod := pods.PodFromManifestId(manifestToLaunch.ID())
				err := hooks.RunBefore(pod, &manifestToLaunch)
				if err != nil {
					manifestLogger.WithFields(logrus.Fields{
						"err":   err,
						"hooks": "before",
					}).Warnln("Could not run before hooks")
				}
				ok := installAndLaunchPod(&manifestToLaunch, pod)
				if ok {
					manifestToLaunch = pods.PodManifest{}
					working = false
				}
				err = hooks.RunAfter(pod, &manifestToLaunch)
				if err != nil {
					manifestLogger.WithFields(logrus.Fields{
						"err":   err,
						"hooks": "after",
					}).Warnln("Could not run after hooks")
				}
			}
		}
	}
}

func installAndLaunchPod(newManifest *pods.PodManifest, pod Pod) bool {
	err := pod.Install(newManifest)
	if err != nil {
		// install failed, abort and retry
		return false
	}

	// get currently running pod to compare with the new pod
	currentManifest, err := pod.CurrentManifest()
	currentSHA, _ := currentManifest.SHA()
	newSHA, _ := newManifest.SHA()

	// if new or the manifest is different, launch
	newOrDifferent := err == pods.NoCurrentManifest || currentSHA != newSHA

	// if the old manifest is corrupted somehow, re-launch since we don't know if this is an update.
	problemReadingCurrentManifest := (err != nil && err != pods.NoCurrentManifest)

	if newOrDifferent || problemReadingCurrentManifest {
		ok, err := pod.Launch(newManifest)
		return err == nil && ok
	}

	return true
}
