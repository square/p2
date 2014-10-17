package pods

import (
	"fmt"
	"os"
	"path"

	"github.com/nareix/curl"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
)

type Pod struct {
	podManifest *PodManifest
}

func PodFromManifestPath(path string) (*Pod, error) {
	podManifest, err := PodManifestFromPath(path)
	if err != nil {
		return nil, err
	}

	return &Pod{podManifest}, nil
}

func (pod *Pod) Halt() error {
	launchables, err := getLaunchablesFromPodManifest(pod.podManifest)
	if err != nil {
		return err
	}

	for _, launchable := range launchables {
		err = launchable.Halt(runit.DefaultBuilder, runit.DefaultSV) // TODO: make these configurable
		if err != nil {
			return err
		}
	}
	return nil
}

func (pod *Pod) Launch() error {
	launchables, err := getLaunchablesFromPodManifest(pod.podManifest)
	if err != nil {
		return err
	}

	for _, launchable := range launchables {
		err = launchable.Launch(runit.DefaultBuilder, runit.DefaultSV) // TODO: make these configurable
		if err != nil {
			return err
		}
	}
	return nil
}

func getLaunchablesFromPodManifest(podManifest *PodManifest) ([]HoistLaunchable, error) {
	launchableStanzas := podManifest.LaunchableStanzas
	if len(launchableStanzas) == 0 {
		return nil, util.Errorf("Pod must provide at least one launchable, none found")
	}

	launchables := make([]HoistLaunchable, len(launchableStanzas))
	var i int = 0
	for _, launchableStanza := range launchableStanzas {

		launchable, err := getLaunchable(launchableStanza, podManifest.Id)
		if err != nil {
			return nil, err
		}
		launchables[i] = *launchable
		i++
	}

	return launchables, nil

}

func PodHomeDir(podId string) string {
	return path.Join("/data", "pods", podId)
}

// This assumes all launchables are Hoist artifacts, we will generalize this at a later point
func (pod *Pod) Install() error {
	// if we don't want this to run as root, need another way to create pods directory
	podsHome := path.Join("/data", "pods")
	os.MkdirAll(podsHome, 0755)

	podHome := path.Join(podsHome, pod.podManifest.Id)
	os.Mkdir(podHome, 0755) // this dir needs to be owned by different user at some point

	launchables, err := getLaunchablesFromPodManifest(pod.podManifest)
	if err != nil {
		return err
	}

	for _, launchable := range launchables {
		err := launchable.Install()
		if err != nil {
			return err
		}
	}

	return nil
}

func getLaunchable(launchableStanza LaunchableStanza, podId string) (*HoistLaunchable, error) {
	if launchableStanza.LaunchableType == "hoist" {
		launchableRootDir := path.Join(PodHomeDir(podId), launchableStanza.LaunchableId)
		return &HoistLaunchable{launchableStanza.Location, launchableStanza.LaunchableId, podId, curl.File, launchableRootDir}, nil
	} else {
		return nil, fmt.Errorf("%s is not supported yet", launchableStanza.LaunchableType)
	}
}
