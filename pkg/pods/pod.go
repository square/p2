package pods

import (
	"errors"
	"fmt"
	"path"

	curl "github.com/andelf/go-curl"
)

func (podManifest *PodManifest) Launch() error {
	launchables, err := getLaunchablesFromPodManifest(podManifest)
	if err != nil {
		return err
	}

	for _, launchable := range launchables {
		err = launchable.Launch()
		if err != nil {
			return err
		}
	}
	return nil
}

func getLaunchablesFromPodManifest(podManifest *PodManifest) ([]HoistLaunchable, error) {
	launchableStanzas := podManifest.LaunchableStanzas
	if len(launchableStanzas) == 0 {
		return nil, errors.New("Pod must provide at least one launchable, none found")
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

func HoistLaunchableHomeDir(podId string) string {
	return path.Join("/data", "app", podId)
}

// This assumes all launchables are Hoist artifacts, we will generalize this at a later point
func (podManifest *PodManifest) Install() error {

	launchables, err := getLaunchablesFromPodManifest(podManifest)
	if err != nil {
		return err
	}

	for _, launchable := range launchables {
		err := launchable.Install(HoistLaunchableHomeDir(podManifest.Id))
		if err != nil {
			return err
		}
	}

	return nil
}

func getLaunchable(launchableStanza LaunchableStanza, podId string) (*HoistLaunchable, error) {
	if launchableStanza.LaunchableType == "hoist" {
		return &HoistLaunchable{launchableStanza.Location, launchableStanza.LaunchableId, podId, curl.EasyInit()}, nil
	} else {
		return nil, fmt.Errorf("%s is not supported yet", launchableStanza.LaunchableType)
	}
}
