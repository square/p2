package pods

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/digest"
	"github.com/square/p2/pkg/hoist"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/opencontainer"
	"github.com/square/p2/pkg/p2exec"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/param"
	"github.com/square/p2/pkg/util/size"

	"github.com/Sirupsen/logrus"
)

var (
	// ExperimentalOpencontainer permits the use of the experimental "opencontainer"
	// launchable type.
	ExperimentalOpencontainer = param.Bool("experimental_opencontainer", false)

	// NestedCgroups causes the p2-preparer to use a hierarchical cgroup naming scheme when
	// creating new launchables.
	NestedCgroups = param.Bool("nested_cgroups", false)
)

const (
	ConfigPathEnvVar         = "CONFIG_PATH"
	LaunchableIDEnvVar       = "LAUNCHABLE_ID"
	LaunchableRootEnvVar     = "LAUNCHABLE_ROOT"
	PodIDEnvVar              = "POD_ID"
	PodHomeEnvVar            = "POD_HOME"
	PodUniqueKeyEnvVar       = "POD_UNIQUE_KEY"
	PlatformConfigPathEnvVar = "PLATFORM_CONFIG_PATH"
)

type Pod struct {
	// ID of the pod, i.e. result of ID() called on the manifest defining the pod
	Id   types.PodID
	node types.NodeName

	// An (optional) unique identifier for the pod. May be empty for "legacy" pods, will be a uuid
	// otherwise
	uniqueKey types.PodUniqueKey

	// The home directory for the pod. Typically some global root (e.g.
	// /data/pods) followed by the pod's UniqueName() (e.g.
	// /data/pods/<pod_id> or /data/pods/<pod_id>-<uuid>
	home           string
	logger         logging.Logger
	SV             runit.SV
	ServiceBuilder *runit.ServiceBuilder
	P2Exec         string
	DefaultTimeout time.Duration // this is the default timeout for stopping and restarting services in this pod
	LogExec        runit.Exec
	FinishExec     runit.Exec
	Fetcher        uri.Fetcher
}

var NoCurrentManifest error = fmt.Errorf("No current manifest for this pod")

func (pod *Pod) Node() types.NodeName {
	return pod.node
}

func (pod *Pod) Home() string {
	return pod.home
}

// A unique name for a pod instance, useful for avoiding filename conflicts.
// Typically <id>-<uuid> if the pod has a uuid, and simply <id> if it does not
// have a uuid
//
// This is exported because being able to generate a unique deterministic
// string for pods is useful in hooks for example.
func (pod *Pod) UniqueName() string {
	return computeUniqueName(pod.Id, pod.uniqueKey)
}

func (pod *Pod) UniqueKey() types.PodUniqueKey {
	return pod.uniqueKey
}

func (pod *Pod) CurrentManifest() (manifest.Manifest, error) {
	currentManPath := pod.currentPodManifestPath()
	if _, err := os.Stat(currentManPath); os.IsNotExist(err) {
		return nil, NoCurrentManifest
	}
	return manifest.FromPath(currentManPath)
}

func (pod *Pod) Halt(manifest manifest.Manifest) (bool, error) {
	launchables, err := pod.Launchables(manifest)
	if err != nil {
		return false, err
	}

	success := true
	for _, launchable := range launchables {
		var err error
		disableFunc := func() {
			err = launchable.Disable()
		}
		pod.withTimeWarnings("disable", launchable.ServiceID(), disableFunc)
		if err != nil {
			// do not set success to false on a disable error
			pod.logLaunchableWarning(launchable.ServiceID(), err, "Could not disable launchable")
		}
	}
	for _, launchable := range launchables {
		err = launchable.Stop(runit.DefaultBuilder, runit.DefaultSV) // TODO: make these configurable
		if err != nil {
			pod.logLaunchableError(launchable.ServiceID(), err, "Could not stop launchable")
			success = false
		}
	}

	if success {
		pod.logInfo("Successfully stopped")
	} else {
		pod.logInfo("Attempted halt, but one or more services did not stop successfully")
	}
	return success, nil
}

// Launch will attempt to start every launchable listed in the pod manifest. Errors encountered
// during the launch process will be logged, but will not stop attempts to launch other launchables
// in the same pod. If any services fail to start, the first return bool will be false. If an error
// occurs when writing the current manifest to the pod directory, an error will be returned.
func (pod *Pod) Launch(manifest manifest.Manifest) (bool, error) {
	launchables, err := pod.Launchables(manifest)
	if err != nil {
		return false, err
	}

	oldManifestTemp, err := pod.WriteCurrentManifest(manifest)
	defer os.RemoveAll(oldManifestTemp)

	if err != nil {
		return false, err
	}

	for _, launchable := range launchables {
		err := launchable.MakeCurrent()
		if err != nil {
			// being unable to flip a symlink is a catastrophic error
			return false, err
		}

		var out string
		postActivateFunc := func() {
			out, err = launchable.PostActivate()
		}
		pod.withTimeWarnings("post-activate", launchable.ServiceID(), postActivateFunc)
		if err != nil {
			// if a launchable's post-activate fails, we probably can't
			// launch it, but this does not break the entire pod
			pod.logLaunchableError(launchable.ServiceID(), err, out)
		} else {
			if out != "" {
				pod.logger.WithField("output", out).Infoln("Successfully post-activated")
			}
		}
	}

	err = pod.buildRunitServices(launchables, manifest)

	success := true
	for _, launchable := range launchables {
		err = launchable.Launch(pod.ServiceBuilder, pod.SV) // TODO: make these configurable
		switch err.(type) {
		case nil:
			// noop
		case launch.EnableError:
			// do not set success to false on an enable error
			pod.logLaunchableWarning(launchable.ServiceID(), err, "Could not enable launchable")
		default:
			// this case intentionally includes launch.StartError
			pod.logLaunchableError(launchable.ServiceID(), err, "Could not launch launchable")
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

func (pod *Pod) Prune(max size.ByteCount, manifest manifest.Manifest) {
	launchables, err := pod.Launchables(manifest)
	if err != nil {
		return
	}
	for _, l := range launchables {
		err := l.Prune(max)
		if err != nil {
			pod.logLaunchableError(l.ServiceID(), err, "Could not prune directory")
			// Don't return here. We want to prune other launchables if possible.
		}
	}
}

func (pod *Pod) Services(manifest manifest.Manifest) ([]runit.Service, error) {
	allServices := []runit.Service{}
	launchables, err := pod.Launchables(manifest)
	if err != nil {
		return nil, err
	}
	for _, l := range launchables {
		es, err := l.Executables(pod.ServiceBuilder)
		if err != nil {
			return nil, err
		}
		if es != nil {
			for _, e := range es {
				allServices = append(allServices, e.Service)
			}
		}
	}
	return allServices, nil
}

// Write servicebuilder *.yaml file and run servicebuilder, which will register runit services for this
// pod.
func (pod *Pod) buildRunitServices(launchables []launch.Launchable, newManifest manifest.Manifest) error {
	// if the service is new, building the runit services also starts them
	sbTemplate := make(map[string]runit.ServiceTemplate)
	for _, launchable := range launchables {
		executables, err := launchable.Executables(pod.ServiceBuilder)
		if err != nil {
			pod.logLaunchableError(launchable.ServiceID(), err, "Unable to list executables")
			continue
		}
		for _, executable := range executables {
			if _, ok := sbTemplate[executable.Service.Name]; ok {
				return util.Errorf("Duplicate executable %q for launchable %q", executable.Service.Name, launchable.ServiceID())
			}
			sbTemplate[executable.Service.Name] = runit.ServiceTemplate{
				Log:           pod.LogExec,
				Run:           executable.Exec,
				Finish:        pod.FinishExecForExecutable(launchable, executable),
				RestartPolicy: launchable.RestartPolicy(),
			}
		}
	}
	err := pod.ServiceBuilder.Activate(pod.UniqueName(), sbTemplate)
	if err != nil {
		return err
	}

	// as with the original servicebuilder, prune after creating
	// new services
	return pod.ServiceBuilder.Prune()
}

func (pod *Pod) WriteCurrentManifest(manifest manifest.Manifest) (string, error) {
	// write the old manifest to a temporary location in case a launch fails.
	tmpDir, err := ioutil.TempDir("", "manifests")
	if err != nil {
		return "", util.Errorf("could not create a tempdir to write old manifest: %s", err)
	}
	lastManifest := filepath.Join(tmpDir, "last_manifest.yaml")

	if _, err := os.Stat(pod.currentPodManifestPath()); err == nil {
		podManifestURL, err := url.Parse(pod.currentPodManifestPath())
		if err != nil {
			return "", util.Errorf("Couldn't parse manifest path '%s' as URL: %s", pod.currentPodManifestPath(), err)
		}

		err = uri.URICopy(podManifestURL, lastManifest)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}

	f, err := os.OpenFile(pod.currentPodManifestPath(), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
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

	uid, gid, err := user.IDs(manifest.RunAsUser())
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
		return os.Rename(lastPath, pod.currentPodManifestPath())
	} else {
		return err
	}
}

func (pod *Pod) currentPodManifestPath() string {
	return filepath.Join(pod.home, "current_manifest.yaml")
}

func (pod *Pod) ConfigDir() string {
	return filepath.Join(pod.home, "config")
}

func (pod *Pod) EnvDir() string {
	return filepath.Join(pod.home, "env")
}

func (pod *Pod) Uninstall() error {
	currentManifest, err := pod.CurrentManifest()
	switch {
	case err == nil:
		// If we found the manifest, gracefully halt all the launchables.
		err = pod.disableAndHaltLaunchables(currentManifest)
		if err != nil {
			return err
		}
	case err == NoCurrentManifest:
		// this is fine, we'll proceed with the uninstall
	case err != nil:
		return err
	}

	// remove services for this pod, then prune the old
	// service dirs away
	err = os.Remove(filepath.Join(pod.ServiceBuilder.ConfigRoot, pod.UniqueName()+".yaml"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	err = pod.ServiceBuilder.Prune()
	if err != nil {
		return err
	}

	// remove pod home dir
	err = os.RemoveAll(pod.home)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// Install will ensure that executables for all required services are present on the host
// machine and are set up to run. In the case of Hoist artifacts (which is the only format
// supported currently, this will set up runit services.).
func (pod *Pod) Install(manifest manifest.Manifest, verifier auth.ArtifactVerifier, artifactRegistry artifact.Registry) error {
	podHome := pod.home
	uid, gid, err := user.IDs(manifest.RunAsUser())
	if err != nil {
		return util.Errorf("Could not determine pod UID/GID for %s: %s", manifest.RunAsUser(), err)
	}

	err = util.MkdirChownAll(podHome, uid, gid, 0755)
	if err != nil {
		return util.Errorf("Could not create pod home: %s", err)
	}

	launchables, err := pod.Launchables(manifest)
	if err != nil {
		return err
	}

	downloader := artifact.NewLocationDownloader(pod.Fetcher, verifier)
	for launchableID, stanza := range manifest.GetLaunchableStanzas() {
		// TODO: investigate passing in necessary fields to InstallDir()
		launchable, err := pod.getLaunchable(launchableID, stanza, manifest.RunAsUser())
		if err != nil {
			pod.logLaunchableError(launchable.ServiceID(), err, "Unable to install launchable")
			return err
		}

		if launchable.Installed() {
			continue
		}

		launchableURL, verificationData, err := artifactRegistry.LocationDataForLaunchable(launchableID, stanza)
		if err != nil {
			pod.logLaunchableError(launchable.ServiceID(), err, "Unable to install launchable")
			return err
		}

		err = downloader.Download(launchableURL, verificationData, launchable.InstallDir(), manifest.RunAsUser())
		if err != nil {
			pod.logLaunchableError(launchable.ServiceID(), err, "Unable to install launchable")
			_ = os.Remove(launchable.InstallDir())
			return err
		}

		err = launchable.PostInstall()
		if err != nil {
			pod.logLaunchableError(launchable.ServiceID(), err, "Unable to install launchable")
			_ = os.Remove(launchable.InstallDir())
			return err
		}
	}

	// we may need to write config files to a unique directory per pod version, depending on restart semantics. Need
	// to think about this more.
	err = pod.setupConfig(manifest, launchables)
	if err != nil {
		pod.logError(err, "Could not setup config")
		return util.Errorf("Could not setup config: %s", err)
	}

	pod.logInfo("Successfully installed")

	return nil
}

func (pod *Pod) Verify(manifest manifest.Manifest, authPolicy auth.Policy) error {
	for launchableID, stanza := range manifest.GetLaunchableStanzas() {
		if stanza.DigestLocation == "" {
			continue
		}
		launchable, err := pod.getLaunchable(launchableID, stanza, manifest.RunAsUser())
		if err != nil {
			return err
		}

		digestLocationURL, err := url.Parse(stanza.DigestLocation)
		if err != nil {
			return util.Errorf("Couldn't parse digest location '%s' as a url: %s", stanza.DigestLocation, err)
		}

		digestSignatureLocationURL, err := url.Parse(stanza.DigestSignatureLocation)
		if err != nil {
			return util.Errorf("Couldn't parse digest signature location '%s' as a url: %s", stanza.DigestSignatureLocation, err)
		}

		// Retrieve the digest data
		launchableDigest, err := digest.ParseUris(
			uri.DefaultFetcher,
			digestLocationURL,
			digestSignatureLocationURL,
		)
		if err != nil {
			return err
		}

		// Check that the digest is certified
		err = authPolicy.CheckDigest(launchableDigest)
		if err != nil {
			return err
		}

		// Check that the installed files match the digest
		err = launchableDigest.VerifyDir(launchable.InstallDir())
		if err != nil {
			return err
		}
	}
	return nil
}

// setupConfig does the following:
//
// 1) creates a directory in the pod's home directory called "config" which
// contains YAML configuration files (named with pod's ID and the SHA of its
// manifest's content) the path to which will be exported to a pods launchables
// via the CONFIG_PATH environment variable
//
// 2) writes an "env" directory in the pod's home directory called "env" which
// contains environment variables written as files that will be exported to all
// processes started by all launchables (as described in
// http://smarden.org/runit/chpst.8.html, with the -e option), including
// CONFIG_PATH
//
// 3) writes an "env" directory for each launchable. The "env" directory
// contains environment files specific to a launchable (such as
// LAUNCHABLE_ROOT)
//
// We may wish to provide a "config" directory per launchable at some point as
// well, so that launchables can have different config namespaces
func (pod *Pod) setupConfig(manifest manifest.Manifest, launchables []launch.Launchable) error {
	uid, gid, err := user.IDs(manifest.RunAsUser())
	if err != nil {
		return util.Errorf("Could not determine pod UID/GID: %s", err)
	}
	var configData bytes.Buffer
	err = manifest.WriteConfig(&configData)
	if err != nil {
		return err
	}
	var platConfigData bytes.Buffer
	err = manifest.WritePlatformConfig(&platConfigData)
	if err != nil {
		return err
	}

	err = util.MkdirChownAll(pod.ConfigDir(), uid, gid, 0755)
	if err != nil {
		return util.Errorf("Could not create config directory for pod %s: %s", manifest.ID(), err)
	}
	configFileName, err := manifest.ConfigFileName()
	if err != nil {
		return err
	}
	configPath := filepath.Join(pod.ConfigDir(), configFileName)
	err = writeFileChown(configPath, configData.Bytes(), uid, gid)
	if err != nil {
		return util.Errorf("Error writing config file for pod %s: %s", manifest.ID(), err)
	}
	platConfigFileName, err := manifest.PlatformConfigFileName()
	if err != nil {
		return err
	}
	platConfigPath := filepath.Join(pod.ConfigDir(), platConfigFileName)
	err = writeFileChown(platConfigPath, platConfigData.Bytes(), uid, gid)
	if err != nil {
		return util.Errorf("Error writing platform config file for pod %s: %s", manifest.ID(), err)
	}

	err = util.MkdirChownAll(pod.EnvDir(), uid, gid, 0755)
	if err != nil {
		return util.Errorf("Could not create the environment dir for pod %s: %s", manifest.ID(), err)
	}
	err = writeEnvFile(pod.EnvDir(), ConfigPathEnvVar, configPath, uid, gid)
	if err != nil {
		return err
	}
	err = writeEnvFile(pod.EnvDir(), PlatformConfigPathEnvVar, platConfigPath, uid, gid)
	if err != nil {
		return err
	}
	err = writeEnvFile(pod.EnvDir(), PodHomeEnvVar, pod.Home(), uid, gid)
	if err != nil {
		return err
	}
	err = writeEnvFile(pod.EnvDir(), PodIDEnvVar, pod.Id.String(), uid, gid)
	if err != nil {
		return err
	}
	err = writeEnvFile(pod.EnvDir(), PodUniqueKeyEnvVar, pod.uniqueKey.String(), uid, gid)
	if err != nil {
		return err
	}

	for _, launchable := range launchables {
		// we need to remove any unset env vars from a previous pod
		err = os.RemoveAll(launchable.EnvDir())
		if err != nil {
			return err
		}

		err = util.MkdirChownAll(launchable.EnvDir(), uid, gid, 0755)
		if err != nil {
			return util.Errorf("Could not create the environment dir for pod %s launchable %s: %s", manifest.ID(), launchable.ServiceID(), err)
		}
		err = writeEnvFile(launchable.EnvDir(), LaunchableIDEnvVar, launchable.ID().String(), uid, gid)
		if err != nil {
			return err
		}
		err = writeEnvFile(launchable.EnvDir(), "LAUNCHABLE_ROOT", launchable.InstallDir(), uid, gid)
		if err != nil {
			return err
		}
		// last, write the user-supplied env variables to ensure priority of user-supplied values
		for envName, value := range launchable.EnvVars() {
			err = writeEnvFile(launchable.EnvDir(), envName, fmt.Sprint(value), uid, gid)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// writeEnvFile takes an environment directory (as described in http://smarden.org/runit/chpst.8.html, with the -e option)
// and writes a new file with the given value.
func writeEnvFile(envDir, name, value string, uid, gid int) error {
	return writeFileChown(filepath.Join(envDir, name), []byte(value), uid, gid)
}

// writeFileChown writes data to a file and sets its owner.
func writeFileChown(filename string, data []byte, uid, gid int) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	if err != nil {
		_ = file.Close()
		return err
	}
	err = file.Chown(uid, gid)
	if err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func (pod *Pod) Launchables(manifest manifest.Manifest) ([]launch.Launchable, error) {
	launchableStanzas := manifest.GetLaunchableStanzas()
	launchables := make([]launch.Launchable, 0, len(launchableStanzas))

	for launchableID, launchableStanza := range launchableStanzas {
		launchable, err := pod.getLaunchable(launchableID, launchableStanza, manifest.RunAsUser())
		if err != nil {
			return nil, err
		}
		launchables = append(launchables, launchable)
	}

	return launchables, nil
}

func (pod *Pod) SetFinishExec(finishExec []string) {
	pod.FinishExec = finishExec
}

func (pod *Pod) FinishExecForExecutable(launchable launch.Launchable, executable launch.Executable) runit.Exec {
	p2ExecArgs := p2exec.P2ExecArgs{
		Command:  pod.FinishExec,
		User:     "nobody",
		EnvDirs:  []string{pod.EnvDir(), launchable.EnvDir()},
		ExtraEnv: map[string]string{launch.EntryPointEnvVar: executable.ServiceName},
	}

	return append([]string{pod.P2Exec}, p2ExecArgs.CommandLine()...)
}

func (pod *Pod) SetLogBridgeExec(logExec []string) {
	p2ExecArgs := p2exec.P2ExecArgs{
		Command: logExec,
		User:    "nobody",
		EnvDirs: []string{pod.EnvDir()},
	}

	pod.LogExec = append([]string{pod.P2Exec}, p2ExecArgs.CommandLine()...)
}

func (pod *Pod) getLaunchable(launchableID launch.LaunchableID, launchableStanza launch.LaunchableStanza, runAsUser string) (launch.Launchable, error) {
	launchableRootDir := filepath.Join(pod.home, launchableID.String())
	serviceId := strings.Join(
		[]string{
			pod.UniqueName(),
			"__",
			launchableID.String(),
		}, "")

	restartTimeout := pod.DefaultTimeout

	if launchableStanza.RestartTimeout != "" {
		possibleTimeout, err := time.ParseDuration(launchableStanza.RestartTimeout)
		if err != nil {
			pod.logger.WithError(err).Errorf("%v is not a valid restart timeout - must be parseable by time.ParseDuration(). Using default time %v", launchableStanza.RestartTimeout, restartTimeout)
		} else {
			restartTimeout = possibleTimeout
		}
	}

	version, err := launchableStanza.LaunchableVersion()
	if err != nil {
		pod.logger.WithError(err).Warnf("Could not parse version from launchable %s.", launchableID)
	}

	if launchableStanza.LaunchableType == "hoist" {
		entryPoints := launchableStanza.EntryPoints
		if len(entryPoints) == 0 {
			entryPoints = append(entryPoints, path.Join("bin", "launch"))
		}
		cgroupName := serviceId
		if *NestedCgroups {
			cgroupName = filepath.Join(
				"p2",
				pod.node.String(),
				pod.UniqueName(),
				launchableID.String(),
			)
		}

		ret := &hoist.Launchable{
			Version:          version,
			Id:               launchableID,
			ServiceId:        serviceId,
			RunAs:            runAsUser,
			PodEnvDir:        pod.EnvDir(),
			RootDir:          launchableRootDir,
			P2Exec:           pod.P2Exec,
			ExecNoLimit:      true,
			RestartTimeout:   restartTimeout,
			RestartPolicy_:   launchableStanza.RestartPolicy(),
			CgroupConfig:     launchableStanza.CgroupConfig,
			CgroupConfigName: launchableID.String(),
			CgroupName:       cgroupName,
			SuppliedEnvVars:  launchableStanza.Env,
			EntryPoints:      entryPoints,
		}
		ret.CgroupConfig.Name = ret.ServiceId
		return ret.If(), nil
	} else if *ExperimentalOpencontainer && launchableStanza.LaunchableType == "opencontainer" {
		ret := &opencontainer.Launchable{
			ID_:             launchableID,
			ServiceID_:      serviceId,
			RunAs:           runAsUser,
			RootDir:         launchableRootDir,
			P2Exec:          pod.P2Exec,
			RestartTimeout:  restartTimeout,
			RestartPolicy_:  launchableStanza.RestartPolicy(),
			CgroupConfig:    launchableStanza.CgroupConfig,
			SuppliedEnvVars: launchableStanza.Env,
		}
		ret.CgroupConfig.Name = serviceId
		return ret, nil
	} else {
		err := fmt.Errorf("launchable type '%s' is not supported", launchableStanza.LaunchableType)
		pod.logLaunchableError(launchableID.String(), err, "Unknown launchable type")
		return nil, err
	}
}

func (pod *Pod) disableAndHaltLaunchables(currentManifest manifest.Manifest) error {
	launchables, err := pod.Launchables(currentManifest)
	if err != nil {
		return err
	}

	// halt launchables
	for _, launchable := range launchables {
		disableFunc := func() {
			err = launchable.Disable()
		}
		pod.withTimeWarnings("disable", launchable.ServiceID(), disableFunc)
		if err != nil {
			pod.logLaunchableWarning(launchable.ServiceID(), err, "Could not disable launchable during uninstallation")
		}
	}

	// halt launchables
	for _, launchable := range launchables {
		err = launchable.Stop(runit.DefaultBuilder, runit.DefaultSV) // TODO: make these configurable
		if err != nil {
			pod.logLaunchableWarning(launchable.ServiceID(), err, "Could not stop launchable during uninstallation")
		}
	}

	return nil
}

func (p *Pod) logError(err error, message string) {
	p.logger.WithError(err).
		Error(message)
}

func (p *Pod) logLaunchableError(serviceID string, err error, message string) {
	p.logger.WithErrorAndFields(err, logrus.Fields{
		"launchable": serviceID}).Error(message)
}

func (p *Pod) logLaunchableWarning(serviceID string, err error, message string) {
	p.logger.WithErrorAndFields(err, logrus.Fields{
		"launchable": serviceID}).Warn(message)
}

func (p *Pod) logInfo(message string) {
	p.logger.WithFields(logrus.Fields{}).Info(message)
}

// Runs function f and emits warnings if it hasn't completed within 1 minute, 2
// minutes, 5 minutes, 10 minutes, and each 5 minute increment afterward. This
// is useful for identifying the cause of a very long install time, for
// instance a disable or post-activate script hanging forever.
func (p *Pod) withTimeWarnings(scriptType string, serviceID string, f func()) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		warningTimes := []time.Duration{
			1 * time.Minute,
			1 * time.Minute,
			3 * time.Minute,
			5 * time.Minute,
		}
		iteration := 0
		totalTime := time.Duration(0)
		var warnAfter time.Duration
		for {
			warnAfter = 5 * time.Minute
			if iteration < len(warningTimes) {
				warnAfter = warningTimes[iteration]
			}

			totalTime += warnAfter

			select {
			case <-doneCh:
				return
			case <-time.After(warnAfter):
				p.logger.WithFields(logrus.Fields{
					"launchable": serviceID,
				}).Warnf("The %s script for %s has been running for %s, it may be hanging", scriptType, serviceID, totalTime)
			}
		}
	}()

	f()
}
