package pods

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Logger = logrus.New()

func SetLogger(logger *logrus.Logger) {
	log = logger
}

func SetLogOut(out io.Writer) {
	log.Formatter = new(logrus.JSONFormatter)
	log.Out = out
}

type Pod struct {
	podManifest *PodManifest
}

func NewPod(manifest *PodManifest) *Pod {
	return &Pod{manifest}
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

func (pod *Pod) Halt() (bool, error) {
	launchables, err := getLaunchablesFromPodManifest(pod.podManifest)
	if err != nil {
		return false, err
	}

	success := true
	for _, launchable := range launchables {
		err = launchable.Halt(runit.DefaultBuilder, runit.DefaultSV) // TODO: make these configurable
		if err != nil {
			// Log the failure but continue
			logLaunchableError(pod.podManifest.Id, launchable.Id, err, "Unable to halt launchable")
			success = false
		}
	}
	if success {
		logPodInfo(pod.podManifest.Id, "Successfully halted")
	} else {
		logPodInfo(pod.podManifest.Id, "Attempted halt, but one or more services did not stop successfully")
	}
	return success, nil
}

func (pod *Pod) Launch() (bool, error) {
	launchables, err := getLaunchablesFromPodManifest(pod.podManifest)
	if err != nil {
		return false, err
	}

	success := true
	for _, launchable := range launchables {
		err = launchable.Launch(runit.DefaultBuilder, runit.DefaultSV) // TODO: make these configurable
		if err != nil {
			// Log the failure but continue
			logLaunchableError(pod.podManifest.Id, launchable.Id, err, "Unable to launch launchable")
			success = false
		}
	}

	f, err := os.OpenFile(CurrentPodManifestPath(pod.podManifest.Id), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		logPodError(pod.podManifest.Id, err, "Unable to open current manifest file")
		return false, err
	}

	err = pod.podManifest.Write(f)
	if err != nil {
		logPodError(pod.podManifest.Id, err, "Unable to write current manifest file")
		return false, err
	}

	if success {
		logPodInfo(pod.podManifest.Id, "Successfully launched")
	} else {
		logPodInfo(pod.podManifest.Id, "Launched pod but one or more services failed to start")
	}
	return success, nil
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

func (pod *Pod) Uninstall() error {
	launchables, err := getLaunchablesFromPodManifest(pod.podManifest)
	if err != nil {
		return err
	}

	// halt launchables
	for _, launchable := range launchables {
		err = launchable.Halt(runit.DefaultBuilder, runit.DefaultSV) // TODO: make these configurable
		if err != nil {
			// log and continue
		}
	}

	// uninstall launchables
	for _, launchable := range launchables {
		err := launchable.Uninstall(runit.DefaultBuilder)
		if err != nil {
			// log and continue
		}
	}

	// remove pod home dir
	os.RemoveAll(PodHomeDir(pod.podManifest.Id))

	return nil
}

// This assumes all launchables are Hoist artifacts, we will generalize this at a later point
func (pod *Pod) Install() error {
	// if we don't want this to run as root, need another way to create pods directory
	podsHome := path.Join("/data", "pods")
	err := os.MkdirAll(podsHome, 0755)
	if err != nil {
		logPodError(pod.podManifest.Id, err, "Unable to create /data/pods directory")
		return err
	}

	podHome := path.Join(podsHome, pod.podManifest.Id)
	err = os.MkdirAll(podHome, 0755) // this dir needs to be owned by different user at some point
	if err != nil {
		return util.Errorf("Could not create pod home: %s", err)
	}
	_, err = user.CreateUser(pod.podManifest.Id, podHome)
	if err != nil && err != user.AlreadyExists {
		return err
	}

	// we may need to write config files to a unique directory per pod version, depending on restart semantics. Need
	// to think about this more.
	err = setupConfig(EnvDir(pod.podManifest.Id), ConfigDir(pod.podManifest.Id), pod.podManifest)
	if err != nil {
		logPodError(pod.podManifest.Id, err, "Could not setup config")
		return util.Errorf("Could not setup config: %s", err)
	}

	launchables, err := getLaunchablesFromPodManifest(pod.podManifest)
	if err != nil {
		return err
	}

	for _, launchable := range launchables {
		err := launchable.Install()
		if err != nil {
			logLaunchableError(pod.podManifest.Id, launchable.Id, err, "Unable to install launchable")
			return err
		}
	}

	logPodInfo(pod.podManifest.Id, "Successfully installed")

	return nil
}

func (pod *Pod) ManifestSHA() (string, error) {
	return pod.podManifest.SHA()
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
		var runAs string
		// proposition: rename all reserved users to p2_*. p2_* users all run as root until
		// precise roles can be assigned to each.
		if podId == "intent" || podId == "preparer" {
			runAs = "root"
		} else {
			runAs = strings.Join([]string{podId, podId}, ":")
		}
		return &HoistLaunchable{launchableStanza.Location, launchableId, runAs, EnvDir(podId), DefaultFetcher(), launchableRootDir}, nil
	} else {
		err := fmt.Errorf("launchable type '%s' is not supported yet", launchableStanza.LaunchableType)
		logLaunchableError(podId, launchableStanza.LaunchableId, err, "Unknown launchable type")
		return nil, err
	}
}

func logPodError(podId string, err error, message string) {
	log.WithFields(logrus.Fields{
		"pod":   podId,
		"error": err,
	}).Error(message)
}

func logLaunchableError(podId string, launchableId string, err error, message string) {
	log.WithFields(logrus.Fields{
		"pod":        podId,
		"launchable": launchableId,
		"error":      err,
	}).Error(message)
}

func logPodInfo(podId string, message string) {
	log.WithFields(logrus.Fields{
		"pod": podId,
	}).Info(message)
}
