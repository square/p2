package main

import (
	"fmt"
	"os"

	"github.com/square/p2/pkg/intent"
	"github.com/square/p2/pkg/pods"
)

func watchForPodManifestsForNode(nodeName string, consulAddress string) {
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
	podChanMap := make(map[string]chan *pods.PodManifest)
	quitChanMap := make(map[string]chan struct{})

	for {
		select {
		case err := <-errChan:
			// do something, probably log somewhere? alert "deployer"?
			fmt.Println(err)
		case manifest := <-podChan:
			podId := manifest.Id

			if podChanMap[podId] == nil {
				fmt.Println(fmt.Sprintf("starting a new goroutine for %s because one doesn't exist", podId))
				// No goroutine is servicing this app currently, let's start one
				podChanMap[podId] = make(chan *pods.PodManifest)
				quitChanMap[podId] = make(chan struct{})
				go installAndLaunchPod(podChanMap[podId], quitChanMap[podId])
			}

			podChanMap[podId] <- &manifest
		}
	}
}

// no return value, no output channels. This should do everything it needs to do
// without outside intervention (other than being signalled to quit)
func installAndLaunchPod(podChan <-chan *pods.PodManifest, quit <-chan struct{}) {
	// install new launchables
	var manifestToLaunch *pods.PodManifest
	manifestToLaunch = nil
	for {
		select {
		case <-quit:
			return
		case manifestToLaunch = <-podChan:
		default:
			if manifestToLaunch != nil {
				newPod := pods.PodFromPodManifest(manifestToLaunch)
				err := newPod.Install()
				if err != nil {
					fmt.Println(err)
					// log this
					// probably retry
					// should this be in its own go routine that we kill if a new manifest is pushed?
					break
				}

				// get currently running pod to compare with the new pod
				currentPod, err := pods.CurrentPodFromManifestId(manifestToLaunch.Id)
				if err != nil {
					if os.IsNotExist(err) {
						// we can ignore this, just means it's a first time deploy
					} else {
						fmt.Println(err)
						// log this
					}
				} else {
					if currentPod != newPod {
						err = currentPod.Halt()
						if err != nil {
							fmt.Println(err)
							// log this
						}
					}

				}
				err = newPod.Launch()
				if err != nil {
					fmt.Println(err)
					// log this
				}
				manifestToLaunch = nil

			}
		}
	}
}
