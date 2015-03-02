package pods

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/square/p2/pkg/hoist"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
)

var (
	Log logging.Logger
)

const DEFAULT_PATH = "/data/pods"

func init() {
	Log = logging.NewLogger(logrus.Fields{})
}

func PodPath(root, manifestId string) string {
	return path.Join(root, manifestId)
}

type Pod struct {
	Id             string
	path           string
	RunAs          string
	logger         logging.Logger
	SV             *runit.SV
	ServiceBuilder *runit.ServiceBuilder
	Chpst          string
	Contain        string
}

func NewPod(id string, path string) *Pod {
	return &Pod{
		Id:             id,
		path:           path,
		RunAs:          id,
		logger:         Log.SubLogger(logrus.Fields{"pod": id}),
		SV:             runit.DefaultSV,
		ServiceBuilder: runit.DefaultBuilder,
		Chpst:          runit.DefaultChpst,
		Contain:        runit.DefaultContain,
	}
}

func ExistingPod(path string) (*Pod, error) {
	pod := NewPod("temp", path)
	manifest, err := pod.CurrentManifest()
	if err == NoCurrentManifest {
		return nil, util.Errorf("No current manifest set, this is not an extant pod directory")
	} else if err != nil {
		return nil, err
	}
	pod.Id = manifest.ID()
	return pod, nil
}

func PodFromManifestId(manifestId string) *Pod {
	return NewPod(manifestId, PodPath(DEFAULT_PATH, manifestId))
}

var NoCurrentManifest error = fmt.Errorf("No current manifest for this pod")

func (pod *Pod) Path() string {
	return pod.path
}

func (pod *Pod) CurrentManifest() (*PodManifest, error) {
	currentManPath := pod.CurrentPodManifestPath()
	if _, err := os.Stat(currentManPath); os.IsNotExist(err) {
		return nil, NoCurrentManifest
	}
	return PodManifestFromPath(currentManPath)
}

func (pod *Pod) Disable(manifest *PodManifest) (bool, error) {
	launchables, err := pod.GetLaunchables(manifest)
	if err != nil {
		return false, err
	}

	success := true
	for _, launchable := range launchables {
		_, err = launchable.Disable()
		if err != nil {
			pod.logLaunchableError(launchable.Id, err, "Unable to disable launchable")
			success = false
		}
	}
	if success {
		pod.logInfo("Successfully disabled")
	} else {
		pod.logInfo("Attempted disable, but one or more services did not disable successfully")
	}
	return success, nil
}

func (pod *Pod) Halt() (bool, error) {
	currentManifest, err := pod.CurrentManifest()
	if err != nil {
		return false, util.Errorf("Could not get current manifest: %s", err)
	}

	launchables, err := pod.GetLaunchables(currentManifest)
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
	launchables, err := pod.GetLaunchables(manifest)
	if err != nil {
		return false, err
	}

	oldManifestTemp, err := pod.WriteCurrentManifest(manifest)
	defer os.RemoveAll(oldManifestTemp)

	if err != nil {
		return false, err
	}

	var successes []bool
	for _, launchable := range launchables {
		err := launchable.MakeCurrent()
		if err != nil {
			// being unable to flip a symlink is a catastrophic error
			return false, err
		}

		out, err := launchable.PostActivate()
		if err != nil {
			// if a launchable's post-activate fails, we probably can't
			// launch it, but this does not break the entire pod
			pod.logLaunchableError(launchable.Id, err, out)
			successes = append(successes, false)
		} else {
			pod.logInfo(out)
			successes = append(successes, true)
		}
	}

	err = pod.BuildRunitServices(launchables)

	success := true
	for i, launchable := range launchables {
		if !successes[i] {
			continue
		}
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

// Write servicebuilder *.yaml file and run servicebuilder, which will register runit services for this
// pod.
func (pod *Pod) BuildRunitServices(launchables []hoist.HoistLaunchable) error {
	// if the service is new, building the runit services also starts them, making the sv start superfluous but harmless
	sbTemplate := runit.NewSBTemplate(pod.Id)
	for _, launchable := range launchables {
		executables, err := launchable.Executables(pod.ServiceBuilder)
		if err != nil {
			return err
		}
		for _, executable := range executables {
			sbTemplate.AddEntry(executable.Service.Name, executable.SBEntry())
		}
		if err != nil {
			// Log the failure but continue
			pod.logLaunchableError(launchable.Id, err, "Unable to launch launchable")
		}
	}
	_, err := pod.ServiceBuilder.Write(sbTemplate)
	if err != nil {
		return err
	}

	_, err = pod.ServiceBuilder.Rebuild()
	if err != nil {
		return err
	}
	return nil
}

func (pod *Pod) WriteCurrentManifest(manifest *PodManifest) (string, error) {
	// write the old manifest to a temporary location in case a launch fails.
	tmpDir, err := ioutil.TempDir("", "manifests")
	if err != nil {
		return "", util.Errorf("could not create a tempdir to write old manifest: %s", err)
	}
	lastManifest := path.Join(tmpDir, "last_manifest.yaml")

	if _, err := os.Stat(pod.CurrentPodManifestPath()); err == nil {
		err = uri.URICopy(pod.CurrentPodManifestPath(), lastManifest)
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
	defer f.Close()

	err = manifest.Write(f)
	if err != nil {
		pod.logError(err, "Unable to write current manifest file")
		err = pod.revertCurrentManifest(lastManifest)
		if err != nil {
			pod.logError(err, "Couldn't replace old manifest as current")
		}
		return "", err
	}

	uid, gid, err := user.IDs(pod.RunAs)
	if err != nil {
		pod.logError(err, "Unable to find pod UID/GID")
		// the write was still successful so we are not going to revert
		return "", err
	}
	err = f.Chown(uid, gid)
	if err != nil {
		pod.logError(err, "Unable to chown current manifest")
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
	launchables, err := pod.GetLaunchables(currentManifest)
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

	// remove runit services
	sbTemplate := runit.NewSBTemplate(pod.Id)
	err = pod.ServiceBuilder.Remove(sbTemplate)
	if err != nil {
		return err
	}

	_, err = pod.ServiceBuilder.Rebuild()
	if err != nil {
		return err
	}

	// remove pod home dir
	os.RemoveAll(pod.path)

	return nil
}

// Install will ensure that executables for all required services are present on the host
// machine and are set up to run. In the case of Hoist artifacts (which is the only format
// supported currently, this will set up runit services.).
func (pod *Pod) Install(manifest *PodManifest) error {
	podHome := pod.path
	uid, gid, err := user.IDs(pod.RunAs)
	if err != nil {
		return util.Errorf("Could not determine pod UID/GID: %s", err)
	}

	err = util.MkdirChownAll(podHome, uid, gid, 0755)
	if err != nil {
		return util.Errorf("Could not create pod home: %s", err)
	}

	// we may need to write config files to a unique directory per pod version, depending on restart semantics. Need
	// to think about this more.
	err = pod.setupConfig(manifest)
	if err != nil {
		pod.logError(err, "Could not setup config")
		return util.Errorf("Could not setup config: %s", err)
	}

	launchables, err := pod.GetLaunchables(manifest)
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
func (pod *Pod) setupConfig(podManifest *PodManifest) error {
	uid, gid, err := user.IDs(pod.RunAs)
	if err != nil {
		return util.Errorf("Could not determine pod UID/GID: %s", err)
	}

	err = util.MkdirChownAll(pod.ConfigDir(), uid, gid, 0755)
	if err != nil {
		return util.Errorf("Could not create config directory for pod %s: %s", podManifest.ID(), err)
	}
	configFileName, err := podManifest.ConfigFileName()
	if err != nil {
		return err
	}
	configPath := path.Join(pod.ConfigDir(), configFileName)

	file, err := os.OpenFile(configPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer file.Close()
	if err != nil {
		return util.Errorf("Could not open config file for pod %s for writing: %s", podManifest.ID(), err)
	}
	err = podManifest.WriteConfig(file)
	if err != nil {
		return err
	}
	err = file.Chown(uid, gid)
	if err != nil {
		return err
	}

	err = util.MkdirChownAll(pod.EnvDir(), uid, gid, 0755)
	if err != nil {
		return util.Errorf("Could not create the environment dir for pod %s: %s", podManifest.ID(), err)
	}
	err = writeEnvFile(pod.EnvDir(), "CONFIG_PATH", configPath, uid, gid)
	if err != nil {
		return err
	}
	err = writeEnvFile(pod.EnvDir(), "POD_HOME", pod.Path(), uid, gid)
	if err != nil {
		return err
	}

	return nil
}

// writeEnvFile takes an environment directory (as described in http://smarden.org/runit/chpst.8.html, with the -e option)
// and writes a new file with the given value.
func writeEnvFile(envDir, name, value string, uid, gid int) error {
	fpath := path.Join(envDir, name)

	buf := bytes.NewBufferString(value)

	err := ioutil.WriteFile(fpath, buf.Bytes(), 0644)
	if err != nil {
		return util.Errorf("Could not write environment config file at %s: %s", fpath, err)
	}

	err = os.Chown(fpath, uid, gid)
	if err != nil {
		return util.Errorf("Could not chown environment config file at %s: %s", fpath, err)
	}
	return nil
}

func (pod *Pod) GetLaunchables(podManifest *PodManifest) ([]hoist.HoistLaunchable, error) {
	launchableStanzas := podManifest.LaunchableStanzas
	if len(launchableStanzas) == 0 {
		return nil, util.Errorf("Pod must provide at least one launchable, none found")
	}

	launchables := make([]hoist.HoistLaunchable, len(launchableStanzas))
	var i int = 0
	for _, launchableStanza := range launchableStanzas {

		launchable, err := pod.getLaunchable(launchableStanza)
		if err != nil {
			return nil, err
		}
		launchables[i] = *launchable
		i++
	}

	return launchables, nil
}

func (pod *Pod) getLaunchable(launchableStanza LaunchableStanza) (*hoist.HoistLaunchable, error) {
	if launchableStanza.LaunchableType == "hoist" {
		launchableRootDir := path.Join(pod.path, launchableStanza.LaunchableId)
		launchableId := strings.Join([]string{pod.Id, "__", launchableStanza.LaunchableId}, "")
		return &hoist.HoistLaunchable{
			Location:    launchableStanza.Location,
			Id:          launchableId,
			RunAs:       pod.RunAs,
			ConfigDir:   pod.EnvDir(),
			FetchToFile: hoist.DefaultFetcher(),
			RootDir:     launchableRootDir,
			Chpst:       pod.Chpst,
			Contain:     pod.Contain,
		}, nil
	} else {
		err := fmt.Errorf("launchable type '%s' is not supported yet", launchableStanza.LaunchableType)
		pod.logLaunchableError(launchableStanza.LaunchableId, err, "Unknown launchable type")
		return nil, err
	}
}

func (p *Pod) logError(err error, message string) {
	p.logger.WithFields(logrus.Fields{
		"err": err.Error(),
	}).Error(message)
}

func (p *Pod) logLaunchableError(launchableId string, err error, message string) {
	p.logger.WithFields(logrus.Fields{
		"launchable": launchableId,
		"err":        err.Error(),
	}).Error(message)
}

func (p *Pod) logInfo(message string) {
	p.logger.WithFields(logrus.Fields{}).Info(message)
}
