package preparer

import (
	"fmt"
	"io"
<<<<<<< HEAD
	"os"
	"path"
||||||| merged common ancestors
	"os"
=======
	// "os"
>>>>>>> Use ID() getter for manifests
	"time"

	"github.com/square/p2/pkg/intent"
	"github.com/square/p2/pkg/pods"
)

<<<<<<< HEAD:bin/preparer/prepare.go
func watchForPodManifestsForNode(nodeName string, consulAddress string, hooksDirectory string, logFile io.Writer) {
||||||| merged common ancestors
func watchForPodManifestsForNode(nodeName string, consulAddress string, logFile io.Writer) {
=======
type Pod interface {
	Launch(*pods.PodManifest) (bool, error)
	Install(*pods.PodManifest) error
	CurrentManifest() (*pods.PodManifest, error)
	Halt() (bool, error)
}

func WatchForPodManifestsForNode(nodeName string, consulAddress string, logFile io.Writer) {
>>>>>>> Use correct runit errors:pkg/preparer/orchestrate.go
	pods.SetLogOut(logFile)
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
			fmt.Printf("Manifest error encountered: %s", err) // change to logrus output
		case manifest := <-podChan:
			podId := manifest.ID()
			if podChanMap[podId] == nil {
				// No goroutine is servicing this app currently, let's start one
				podChanMap[podId] = make(chan pods.PodManifest)
				quitChanMap[podId] = make(chan struct{})
				go handlePods(hooksDirectory, podChanMap[podId], quitChanMap[podId])
			}

			podChanMap[podId] <- manifest
		}
	}
}

// no return value, no output channels. This should do everything it needs to do
// without outside intervention (other than being signalled to quit)
func handlePods(hooksDirectory string, podChan <-chan pods.PodManifest, quit <-chan struct{}) {
	// install new launchables
	var manifestToLaunch pods.PodManifest

	// used to track if we have work to do (i.e. pod manifest came through channel
	// and we have yet to operate on it)
	working := false
	for {
		select {
		case <-quit:
			return
		case manifestToLaunch = <-podChan:
			working = true
		case <-time.After(1 * time.Second):
			if working {
				ok := installAndLaunchPod(&manifestToLaunch, pods.PodFromManifestId(manifestToLaunch.ID()))
				if ok {
					manifestToLaunch = pods.PodManifest{}
					working = false
				}
			}
		}
	}
}

func installAndLaunchPod(newManifest *pods.PodManifest, pod Pod) bool {
	fmt.Printf("Launching %s\n", newManifest.ID())

	err := pod.Install(newManifest)
	if err != nil {
		// abort and retry
		return false
	}

	// get currently running pod to compare with the new pod
	currentManifest, err := pod.CurrentManifest()
	if err == pods.NoCurrentManifest {
		ok, err := pod.Launch(newManifest)
		if err != nil || !ok {
			// abort and retry
			return false
		}
		return true
	} else {
		currentSHA, _ := currentManifest.SHA()
		newSHA, _ := newManifest.SHA()
		if currentSHA != newSHA {
			fmt.Printf("Halting %s of %s to launch %s\n", currentSHA, newManifest.ID(), newSHA)
			ok, err := pod.Halt()
			if err != nil || !ok {
				// Abort so we retry
				return false
			}
			ok, err = pod.Launch(newManifest)
			if err != nil || !ok {
				return false
			}
		}

	}
	return true
}
