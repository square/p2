package pods

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
)

var Log logging.Logger

func init() {
	Log = logging.NewLogger(logrus.Fields{})
}

func PodPath(manifestId string) string {
	return path.Join("/data/pods", manifestId)
}

type Pod struct {
	path           string
	logger         logging.Logger
	SV             *runit.SV
	ServiceBuilder *runit.ServiceBuilder
}

func NewPod(path string) *Pod {
	return &Pod{path, Log.SubLogger(logrus.Fields{"pod": path}), runit.DefaultSV, runit.DefaultBuilder}
}

func PodFromManifestId(manifestId string) *Pod {
	return NewPod(PodPath(manifestId))
}

var NoCurrentManifest error = fmt.Errorf("No current manifest for this pod")

func (pod *Pod) CurrentManifest() (*PodManifest, error) {
	currentManPath := pod.CurrentPodManifestPath()
	if _, err := os.Stat(currentManPath); os.IsNotExist(err) {
		return nil, NoCurrentManifest
	}
	return PodManifestFromPath(currentManPath)
}

func (pod *Pod) Halt() (bool, error) {
	currentManifest, err := pod.CurrentManifest()
	if err != nil {
		return false, util.Errorf("Could not get current manifest: %s", err)
	}

	launchables, err := pod.getLaunchablesFromPodManifest(currentManifest)
	if err != nil {
		return false, err
	}

	success := true
	for _, launchable := range launchables {
		err = launchable.Halt(runit.DefaultBuilder, runit.DefaultSV) // TODO: make these configurable
		if err != nil {
			// Log the failure but continue
			pod.logLaunchableError(launchable.Id, err, "Unable to halt launchable")
			success = false
		}
	}
	if success {
		pod.logInfo("Successfully halted")
	} else {
		pod.logInfo("Attempted halt, but one or more services did not stop successfully")
	}
	return success, nil
}

// Launch will attempt to start every launchable listed in the pod manifest. Errors encountered
// during the launch process will be logged, but will not stop attempts to launch other launchables
// in the same pod. If any services fail to start, the first return bool will be false. If an error
// occurs when writing the current manifest to the pod directory, an error will be returned.
func (pod *Pod) Launch(manifest *PodManifest) (bool, error) {
	launchables, err := pod.getLaunchablesFromPodManifest(manifest)
	if err != nil {
		return false, err
	}

	oldManifestTemp, err := pod.writeCurrentManifest(manifest)
	defer os.RemoveAll(oldManifestTemp)

	if err != nil {
		return false, err
	}

	success := true
	for _, launchable := range launchables {
		err = launchable.Launch(pod.ServiceBuilder, pod.SV) // TODO: make these configurable
		if err != nil {
			// Log the failure but continue
			pod.logLaunchableError(launchable.Id, err, "Unable to launch launchable")
			success = false
		}
	}

	if success {
		pod.logInfo("Successfully launched")
	} else {
		pod.logInfo("Launched pod but one or more services failed to start")
	}

	return success, nil
}

func (pod *Pod) writeCurrentManifest(manifest *PodManifest) (string, error) {
	// write the old manifest to a temporary location in case a launch fails.
	tmpDir, err := ioutil.TempDir("", "manifests")
	if err != nil {
		return "", util.Errorf("could not create a tempdir to write old manifest: %s", err)
	}
	lastManifest := path.Join(tmpDir, "last_manifest.yaml")

	if _, err := os.Stat(pod.CurrentPodManifestPath()); err == nil {
		err = os.Rename(pod.CurrentPodManifestPath(), lastManifest)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}

	f, err := os.OpenFile(pod.CurrentPodManifestPath(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		pod.logError(err, "Unable to open current manifest file")
		err = pod.revertCurrentManifest(lastManifest)
		if err != nil {
			pod.logError(err, "Couldn't replace old manifest as current")
		}
		return "", err
	}

	err = manifest.Write(f)
	if err != nil {
		pod.logError(err, "Unable to write current manifest file")
		err = pod.revertCurrentManifest(lastManifest)
		if err != nil {
			pod.logError(err, "Couldn't replace old manifest as current")
		}
		return "", err
	}
	return lastManifest, nil
}

func (pod *Pod) revertCurrentManifest(lastPath string) error {
	if _, err := os.Stat(lastPath); err == nil {
		return os.Rename(lastPath, pod.CurrentPodManifestPath())
	} else {
		return err
	}
}

func (pod *Pod) CurrentPodManifestPath() string {
	return path.Join(pod.path, "current_manifest.yaml")
}

func (pod *Pod) ConfigDir() string {
	return path.Join(pod.path, "config")
}

func (pod *Pod) EnvDir() string {
	return path.Join(pod.path, "env")
}

func (pod *Pod) Uninstall() error {
	currentManifest, err := pod.CurrentManifest()
	if err != nil {
		return err
	}
	launchables, err := pod.getLaunchablesFromPodManifest(currentManifest)
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
	os.RemoveAll(pod.path)

	return nil
}

// Install will ensure that executables for all required services are present on the host
// machine and are set up to run. In the case of Hoist artifacts (which is the only format
// supported currently, this will set up runit services.).
func (pod *Pod) Install(manifest *PodManifest) error {
	// if we don't want this to run as root, need another way to create pods directory
	podHome := pod.path
	err := os.MkdirAll(podHome, 0755) // this dir needs to be owned by different user at some point
	if err != nil {
		return util.Errorf("Could not create pod home: %s", err)
	}
	_, err = user.CreateUser(manifest.ID(), podHome)
	if err != nil && err != user.AlreadyExists {
		return err
	}

	// we may need to write config files to a unique directory per pod version, depending on restart semantics. Need
	// to think about this more.
	err = setupConfig(pod.EnvDir(), pod.ConfigDir(), manifest)
	if err != nil {
		pod.logError(err, "Could not setup config")
		return util.Errorf("Could not setup config: %s", err)
	}

	launchables, err := pod.getLaunchablesFromPodManifest(manifest)
	if err != nil {
		return err
	}

	for _, launchable := range launchables {
		err := launchable.Install()
		if err != nil {
			pod.logLaunchableError(launchable.Id, err, "Unable to install launchable")
			return err
		}
	}

	pod.logInfo("Successfully installed")

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
		return util.Errorf("Could not create config directory for pod %s: %s", podManifest.ID(), err)
	}
	configFileName, err := podManifest.ConfigFileName()
	if err != nil {
		return err
	}
	configPath := path.Join(configDir, configFileName)

	file, err := os.OpenFile(configPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer file.Close()
	if err != nil {
		return util.Errorf("Could not open config file for pod %s for writing: %s", podManifest.ID(), err)
	}
	err = podManifest.WriteConfig(file)
	if err != nil {
		return err
	}

	err = os.MkdirAll(envDir, 0755)
	if err != nil {
		return util.Errorf("Could not create the environment dir for pod %s: %s", podManifest.ID(), err)
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

func (pod *Pod) getLaunchablesFromPodManifest(podManifest *PodManifest) ([]HoistLaunchable, error) {
	launchableStanzas := podManifest.LaunchableStanzas
	if len(launchableStanzas) == 0 {
		return nil, util.Errorf("Pod must provide at least one launchable, none found")
	}

	launchables := make([]HoistLaunchable, len(launchableStanzas))
	var i int = 0
	for _, launchableStanza := range launchableStanzas {

		launchable, err := pod.getLaunchable(launchableStanza, podManifest)
		if err != nil {
			return nil, err
		}
		launchables[i] = *launchable
		i++
	}

	return launchables, nil
}

func (pod *Pod) getLaunchable(launchableStanza LaunchableStanza, manifest *PodManifest) (*HoistLaunchable, error) {
	if launchableStanza.LaunchableType == "hoist" {
		launchableRootDir := path.Join(pod.path, launchableStanza.LaunchableId)
		launchableId := strings.Join([]string{manifest.ID(), "__", launchableStanza.LaunchableId}, "")
		var runAs string
		// proposition: rename all reserved users to p2_*. p2_* users all run as root until
		// precise roles can be assigned to each.
		if manifest.ID() == "intent" || manifest.ID() == "preparer" {
			runAs = "root"
		} else {
			runAs = strings.Join([]string{manifest.ID(), manifest.ID()}, ":")
		}
		return &HoistLaunchable{launchableStanza.Location, launchableId, runAs, pod.EnvDir(), DefaultFetcher(), launchableRootDir}, nil
	} else {
		err := fmt.Errorf("launchable type '%s' is not supported yet", launchableStanza.LaunchableType)
		pod.logLaunchableError(launchableStanza.LaunchableId, err, "Unknown launchable type")
		return nil, err
	}
}

func (p *Pod) logError(err error, message string) {
	p.logger.WithFields(logrus.Fields{
		"err": err,
	}).Error(message)
}

func (p *Pod) logLaunchableError(launchableId string, err error, message string) {
	p.logger.WithFields(logrus.Fields{
		"launchable": launchableId,
		"err":        err,
	}).Error(message)
}

func (p *Pod) logInfo(message string) {
	p.logger.WithFields(logrus.Fields{}).Info(message)
}
