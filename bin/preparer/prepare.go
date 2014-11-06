package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/square/p2/pkg/intent"
	"github.com/square/p2/pkg/pods"
)

func watchForPodManifestsForNode(nodeName string, consulAddress string, logFile io.Writer) {
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
		case <-errChan:
			// do something, probably log somewhere? alert "deployer"?
		case manifest := <-podChan:
			podId := manifest.Id

			if podChanMap[podId] == nil {
				// No goroutine is servicing this app currently, let's start one
				podChanMap[podId] = make(chan pods.PodManifest)
				quitChanMap[podId] = make(chan struct{})
				go handlePods(podChanMap[podId], quitChanMap[podId])
			}

			podChanMap[podId] <- manifest
		}
	}
}

// no return value, no output channels. This should do everything it needs to do
// without outside intervention (other than being signalled to quit)
func handlePods(podChan <-chan pods.PodManifest, quit <-chan struct{}) {
	// install new launchables
	var manifestToLaunch pods.PodManifest
	for {
		select {
		case <-quit:
			return
		case manifestToLaunch = <-podChan:
		default:
			if !manifestToLaunch.IsEmpty() {
				ok := installAndLaunchPod(&manifestToLaunch)
				if ok {
					manifestToLaunch = pods.PodManifest{}
				} else {
					// we're about to retry, sleep a little first
					time.Sleep(1 * time.Second)
				}
			}
		}
	}
}

func installAndLaunchPod(podManifest *pods.PodManifest) bool {
	newPod := pods.PodFromPodManifest(podManifest)
	err := newPod.Install()
	if err != nil {
		// abort and retry
		return false
	}

	// get currently running pod to compare with the new pod
	currentPod, err := pods.CurrentPodFromManifestId(podManifest.Id)
	if err != nil {
		if os.IsNotExist(err) {
			// we can ignore this, just means it's a first time deploy
		} else {

			// Abort so we retry
			return false
		}
	} else {
		currentSHA, _ := currentPod.ManifestSHA()
		newSHA, _ := newPod.ManifestSHA()
		if currentSHA != newSHA {
			ok, err := currentPod.Halt()
			if err != nil || !ok {
				// Abort so we retry
				return false
			}
		}

	}
	ok, err := newPod.Launch()
	if err != nil || !ok {
		// abort and retry
		return false
	}
	return true

}
