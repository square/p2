package pods

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
)

type Pod struct {
	podManifest *PodManifest
}

func CurrentPodFromManifestId(manifestId string) (*Pod, error) {
	podManifest, err := PodManifestFromPath(CurrentPodManifestPath(manifestId))
	if err != nil {
		return nil, err
	}

	return &Pod{podManifest}, nil
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

	f, err := os.OpenFile(CurrentPodManifestPath(pod.podManifest.Id), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	err = pod.podManifest.Write(f)
	if err != nil {
		return err
	}
	return nil
}

func PodHomeDir(podId string) string {
	return path.Join("/data", "pods", podId)
}

func CurrentPodManifestPath(manifestId string) string {
	return path.Join(PodHomeDir(manifestId), "current_manifest.yaml")
}

func ConfigDir(podId string) string {
	return path.Join(PodHomeDir(podId), "config")
}

func EnvDir(podId string) string {
	return path.Join(PodHomeDir(podId), "env")
}

// This assumes all launchables are Hoist artifacts, we will generalize this at a later point
func (pod *Pod) Install() error {
	// if we don't want this to run as root, need another way to create pods directory
	podsHome := path.Join("/data", "pods")
	err := os.MkdirAll(podsHome, 0755)
	if err != nil {
		return err
	}

	podHome := path.Join(podsHome, pod.podManifest.Id)
	os.MkdirAll(podHome, 0755) // this dir needs to be owned by different user at some point

	// we may need to write config files to a unique directory per pod version, depending on restart semantics. Need
	// to think about this more.
	err = setupConfig(EnvDir(pod.podManifest.Id), ConfigDir(pod.podManifest.Id), pod.podManifest)
	if err != nil {
		return util.Errorf("Could not setup config: %s", err)
	}

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

// setupConfig creates two directories in the pod's home directory, called "env" and "config."
// the "config" directory contains the pod's config file, named with pod's ID and the
// SHA of its manifest's content. The "env" directory contains environment files
// (as described in http://smarden.org/runit/chpst.8.html, with the -e option) and includes a
// single file called CONFIG_PATH, which points at the file written in the "config" directory.
func setupConfig(envDir string, configDir string, podManifest *PodManifest) error {
	err := os.MkdirAll(configDir, 0755)
	if err != nil {
		return util.Errorf("Could not create config directory for pod %s: %s", podManifest.Id, err)
	}
	sha, err := podManifest.SHA()
	if err != nil {
		return err
	}
	configPath := path.Join(configDir, podManifest.Id+"_"+sha+".yml")

	file, err := os.OpenFile(configPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer file.Close()
	if err != nil {
		return util.Errorf("Could not open config file for pod %s for writing: %s", podManifest.Id, err)
	}
	err = podManifest.WriteConfig(file)
	if err != nil {
		return err
	}

	err = os.MkdirAll(envDir, 0755)
	if err != nil {
		return util.Errorf("Could not create the environment dir for pod %s: %s", podManifest.Id, err)
	}
	err = writeEnvFile(envDir, "CONFIG_PATH", configPath)
	if err != nil {
		return err
	}

	return nil
}

// writeEnvFile takes an environment directory (as described in http://smarden.org/runit/chpst.8.html, with the -e option)
// and writes a new file with the given value.
func writeEnvFile(envDir, name, value string) error {
	fpath := path.Join(envDir, name)

	buf := bytes.NewBufferString(value)

	err := ioutil.WriteFile(fpath, buf.Bytes(), 0644)
	if err != nil {
		return util.Errorf("Could not write environment config file at %s: %s", fpath, err)
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

func getLaunchable(launchableStanza LaunchableStanza, podId string) (*HoistLaunchable, error) {
	if launchableStanza.LaunchableType == "hoist" {
		launchableRootDir := path.Join(PodHomeDir(podId), launchableStanza.LaunchableId)
		launchableId := strings.Join([]string{podId, "__", launchableStanza.LaunchableId}, "")
		return &HoistLaunchable{launchableStanza.Location, launchableId, launchableId, EnvDir(podId), DefaultFetcher(), launchableRootDir}, nil
	} else {
		return nil, fmt.Errorf("launchable type '%s' is not supported yet", launchableStanza.LaunchableType)
	}
}
