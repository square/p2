package hoist

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/p2exec"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

type EntryPoints struct {
	// Paths contains the relative paths to each entry point from the root
	// of the launchable
	Paths []string
	// Implicit means that the entry points were not explicitly specified
	// in the pod manifest and instead a default was used. This is useful
	// information since we want to treat a missing entry point as an error
	// if and only if it was listed explicitly
	Implicit bool
}

// A HoistLaunchable represents a particular install of a hoist artifact.
type Launchable struct {
	Id               launch.LaunchableID        // A (pod-wise) unique identifier for this launchable, used to distinguish it from other launchables in the pod
	Version          launch.LaunchableVersionID // A version identifier
	PodID            types.PodID                // A (possibly-null) PodID denoting which launchable this belongs to
	ServiceId        string                     // A (host-wise) unique identifier for this launchable, used when creating runit services
	RunAs            string                     // The user to assume when launching the executable
	OwnAs            string                     // The user that owns all the launcable's artifacts
	PodEnvDir        string                     // The value for chpst -e. See http://smarden.org/runit/chpst.8.html
	RootDir          string                     // The root directory of the launchable, containing N:N>=1 installs.
	P2Exec           string                     // Struct that can be used to build a p2-exec invocation with appropriate flags
	ExecNoLimit      bool                       // If set, execute with the -n (--no-limit) argument to p2-exec
	PodCgroupConfig  cgroups.Config             // PodCgroupConfig
	CgroupConfig     cgroups.Config             // Cgroup parameters to use with p2-exec
	CgroupConfigName string                     // The string in PLATFORM_CONFIG to pass to p2-exec
	CgroupName       string                     // The name of the cgroup to run this launchable in
	RequireFile      string                     // Do not run this launchable until this file exists
	RestartTimeout   time.Duration              // How long to wait when restarting the services in this launchable.
	RestartPolicy_   runit.RestartPolicy        // Dictates whether the launchable should be automatically restarted upon exit.
	NoHaltOnUpdate_  bool                       // If set, the launchable's process(es) should not be stopped if the pod is being updated (it will arrange for its own signaling)
	SuppliedEnvVars  map[string]string          // A map of user-supplied environment variables to be exported for this launchable
	Location         *url.URL                   // URL to download the artifact from
	VerificationData auth.VerificationData      // Paths to files used to verify the artifact
	EntryPoints      EntryPoints                // paths to entry points to launch under runit

	// IsUUIDPod indicates whether the launchable is part of a "uuid pod"
	// vs a "legacy pod". Currently this information is used for determining the name of the runit service directories to use
	// for each of a pod's processes. UUID pods use the "new" naming scheme and legacy pods use the "old" naming scheme.
	//
	// Example old naming scheme:
	// /var/service/some-pod__some-launchable__launch/
	//
	// Example new naming scheme (includes full relative path to entry point with
	// slashes exchanged for double underscores):
	// /var/service/some-pod-<uuid>__some-launchable__bin__launch/
	IsUUIDPod bool
}

// LaunchAdapter adapts a hoist.Launchable to the launch.Launchable interface.
type LaunchAdapter struct {
	*Launchable
}

func (a LaunchAdapter) ID() launch.LaunchableID {
	return a.Launchable.Id
}

func (a LaunchAdapter) ServiceID() string {
	return a.Launchable.ServiceId
}

var _ launch.Launchable = &LaunchAdapter{}

// If adapts the hoist Launchable to the launch.Launchable interface.
func (hl *Launchable) If() launch.Launchable {
	return LaunchAdapter{Launchable: hl}
}

func (hl *Launchable) IsOneoff() bool {
	return hl.IsUUIDPod
}

func (hl *Launchable) Disable() error {
	if hl.IsOneoff() {
		// oneoff pods have nothing to disable/enable, they only run once and there's
		// no server component
		return nil
	}

	// the error return from os/exec.Run is almost always meaningless
	// ("exit status 1")
	// since the output is more useful to the user, that's what we'll preserve
	out, err := hl.disable()
	if err != nil {
		return launch.DisableError{Inner: util.Errorf("%s", out)}
	}

	return nil
}

func (hl *Launchable) Stop(serviceBuilder *runit.ServiceBuilder, sv runit.SV, force bool) error {
	if hl.NoHaltOnUpdate_ && !force {
		return nil
	}

	stopErr := hl.stop(serviceBuilder, sv)
	// We still want to update the "last" symlink even if there was an
	// error during stop()
	makeLastErr := hl.makeLast()
	if stopErr != nil {
		// if there was a stop error AND a makeLast() error, we want to report the stop error
		return launch.StopError{Inner: stopErr}
	}
	if makeLastErr != nil {
		return makeLastErr
	}

	return nil
}

func (hl *Launchable) Launch(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	startErr := hl.start(serviceBuilder, sv)
	if startErr != nil && !IsMissingEntryPoints(startErr) {
		return launch.StartError{Inner: startErr}
	}

	// if we were just missing entry points, we still want to enable.
	//
	// backstory: the hoist launchable specification defines that
	// bin/launch can be a file or a directory that will be turned into
	// runit services. We then added the explicit entry_points field to the
	// launchable stanza, introducing a failure mode where the entry points
	// specified may not actually exist. However some p2 apps intentionally
	// do not have a bin/launch but expect bin/enable to be run, so we have
	// to continue onward if the error was due to missing entry points
	out, enableErr := hl.enable()
	// if we had a missing entry point error, bubble that one up
	if startErr != nil {
		return launch.StartError{Inner: startErr}
	}
	if enableErr != nil {
		return launch.EnableError{Inner: util.Errorf("%s", out)}
	}
	return nil
}

func (hl *Launchable) PostActivate() (string, error) {
	// TODO: unexport this method (requires integrating BuildRunitServices into this API)
	output, err := hl.InvokeBinScript("post-activate")

	// providing a post-activate script is optional, ignore those errors
	if err != nil && !os.IsNotExist(err) {
		return output, err
	}

	return output, nil
}

func (hl *Launchable) disable() (string, error) {
	output, err := hl.InvokeBinScript("disable")

	// providing a disable script is optional, ignore those errors
	if err != nil && !os.IsNotExist(err) {
		return output, util.Errorf("Could not disable %s: %s", hl.ServiceId, err)
	}

	return output, nil
}

func (hl *Launchable) enable() (string, error) {
	if hl.IsOneoff() {
		// For oneoff pods, there is nothing to enable. It's just an entry
		// point that runs once.
		return "", nil
	}

	output, err := hl.InvokeBinScript("enable")

	// providing an enable script is optional, ignore those errors
	if err != nil && !os.IsNotExist(err) {
		return output, util.Errorf("Could not enable %s: %s", hl.ServiceId, err)
	}

	return output, nil
}

func (hl *Launchable) InvokeBinScript(script string) (string, error) {
	cmdPath := filepath.Join(hl.InstallDir(), "bin", script)
	_, err := os.Stat(cmdPath)
	if err != nil {
		return "", err
	}

	cgroupName := hl.CgroupName
	if hl.CgroupConfigName == "" {
		cgroupName = ""
	}
	p2ExecArgs := p2exec.P2ExecArgs{
		Command:          []string{cmdPath},
		User:             hl.RunAs,
		EnvDirs:          []string{hl.PodEnvDir, hl.EnvDir()},
		NoLimits:         hl.ExecNoLimit,
		CgroupConfigName: hl.CgroupConfigName,
		CgroupName:       cgroupName,
		RequireFile:      hl.RequireFile,
		ClearEnv:         true,
	}
	cmd := exec.Command(hl.P2Exec, p2ExecArgs.CommandLine()...)
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	cmd.Stderr = &buffer
	err = cmd.Run()
	if err != nil {
		return buffer.String(), err
	}

	return buffer.String(), nil
}

func (hl *Launchable) stop(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	executables, err := hl.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		_, err := sv.Stop(&executable.Service, hl.RestartTimeout)
		if err != nil && err != runit.Killed {
			// TODO: FAILURE SCENARIO (what should we do here?)
			// 1) does `sv stop` ever exit nonzero?
			// 2) should we keep stopping them all anyway?
			return err
		}
	}
	return nil
}

// Start will take a launchable and start every runit service associated with the launchable.
// All services will attempt to be started.
func (hl *Launchable) start(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	executables, err := hl.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		var err error
		if hl.RestartPolicy_ == runit.RestartPolicyAlways {
			_, err = sv.Restart(&executable.Service, hl.RestartTimeout)
		} else {
			_, err = sv.Once(&executable.Service)
		}
		if err != nil && err != runit.SuperviseOkMissing && err != runit.Killed {
			return err
		}

		if _, err = sv.Restart(&executable.LogAgent, runit.DefaultTimeout); err != nil && err != runit.Killed {
			return err
		}

	}

	return nil
}

type MissingEntryPoints struct {
	message string
}

func (m MissingEntryPoints) Error() string {
	return m.message
}

var _ error = MissingEntryPoints{}

func IsMissingEntryPoints(err error) bool {
	_, ok := err.(MissingEntryPoints)
	return ok
}

// Executables() returns a list of the executables present in the launchable.
// The podUniqueKey argument is a bit of a hack: If it is empty then the "old"
// naming scheme for runit service directories will be used. Otherwise the
// newer naming scheme will be used.
//
// Example old naming scheme:
// /var/service/some-pod__some-launchable__launch/
//
// Example new naming scheme (includes full relative path to entry point with
// slashes exchanged for double underscores):
// /var/service/some-pod-<uuid>__some-launchable__bin__launch/
func (hl *Launchable) Executables(
	serviceBuilder *runit.ServiceBuilder,
) ([]launch.Executable, error) {
	if !hl.Installed() {
		return []launch.Executable{}, util.Errorf("%s is not installed", hl.ServiceId)
	}

	// Maps service name to a launch.Executable to guarantee that no two services can share
	// a name.
	executableMap := make(map[string]launch.Executable)

	for _, relativeEntryPoint := range hl.EntryPoints.Paths {
		absEntryPointPath := filepath.Join(hl.InstallDir(), relativeEntryPoint)

		entryPointInfo, err := os.Stat(absEntryPointPath)
		if err != nil {
			if hl.EntryPoints.Implicit && os.IsNotExist(err) {
				// We assume the deployer knows what they're doing
				// and there aren't supposed to be any entry points
				return nil, nil
			}

			return nil, MissingEntryPoints{
				message: util.Errorf("missing entry point %s: %s", absEntryPointPath, err).Error(),
			}
		}

		// an entry point can be a file or a directory. If it's a file, simply
		// add it to our services map. Otherwise, add each file under it.
		var relativeExecutablePaths []string
		if entryPointInfo.IsDir() {
			services, err := ioutil.ReadDir(absEntryPointPath)
			if err != nil {
				return nil, err
			}

			for _, service := range services {
				relativeExecutablePaths = append(relativeExecutablePaths, filepath.Join(relativeEntryPoint, service.Name()))
			}
		} else {
			relativeExecutablePaths = append(relativeExecutablePaths, relativeEntryPoint)
		}

		for _, relativePath := range relativeExecutablePaths {
			var entryPointName string

			// This is a hack to preserve the runit service
			// directory layout for legacy pods. From a theoretical
			// standpoint we would like to include all parts of the
			// path in the service name so that there could be
			// multiple files started with the same basename but
			// different paths. UUID pods are new so we can adopt
			// the scheme we want
			if hl.IsUUIDPod {
				entryPointName = strings.Replace(relativePath, "/", "__", -1)
			} else {
				entryPointName = filepath.Base(relativePath)
			}
			serviceName := fmt.Sprintf("%s__%s", hl.ServiceId, entryPointName)

			// Make sure we don't have two services with the same name
			// (which is possible because) multiple entry points may be
			// specified

			if _, ok := executableMap[serviceName]; ok {
				return nil, util.Errorf("Multiple services found with name %s", serviceName)
			}

			p2ExecArgs := p2exec.P2ExecArgs{
				Command:          []string{filepath.Join(hl.InstallDir(), relativePath)},
				User:             hl.RunAs,
				EnvDirs:          []string{hl.PodEnvDir, hl.EnvDir()},
				ExtraEnv:         map[string]string{launch.EntryPointEnvVar: relativePath},
				NoLimits:         hl.ExecNoLimit,
				CgroupConfigName: hl.CgroupConfigName,
				PodID:            &hl.PodID,
				CgroupName:       hl.CgroupName,
				RequireFile:      hl.RequireFile,
			}
			execCmd := append([]string{hl.P2Exec}, p2ExecArgs.CommandLine()...)

			executableMap[serviceName] = launch.Executable{
				ServiceName:  entryPointName,
				RelativePath: relativePath,
				Service: runit.Service{
					Path: filepath.Join(serviceBuilder.RunitRoot, serviceName),
					Name: serviceName,
				},
				LogAgent: runit.Service{
					Path: filepath.Join(serviceBuilder.RunitRoot, serviceName, "log"),
					Name: serviceName + " logAgent",
				},
				Exec: execCmd,
			}
		}
	}

	var executables []launch.Executable
	for _, executable := range executableMap {
		executables = append(executables, executable)
	}

	return executables, nil
}

func (hl *Launchable) Installed() bool {
	installDir := hl.InstallDir()
	_, err := os.Stat(installDir)
	return err == nil
}

func (hl *Launchable) PostInstall() error {
	return nil
}

// The version of the artifact is determined from the artifact location. If the
// version tag is set in the location's query, that is returned. Otherwise, the
// version is derived from the location, using the naming scheme
// <the-app>_<unique-version-string>.tar.gz
func (hl *Launchable) Name() string {
	name := hl.Id.String()
	if hl.Version != "" {
		name = name + "_" + hl.Version.String()
	}
	return name
}

func (*Launchable) Type() string {
	return "hoist"
}

func (hl *Launchable) AppManifest() (*artifact.AppManifest, error) {
	if !hl.Installed() {
		return nil, util.Errorf("%s has not been installed yet", hl.ServiceId)
	}
	manPath := filepath.Join(hl.InstallDir(), "app-manifest.yaml")
	if _, err := os.Stat(manPath); os.IsNotExist(err) {
		manPath = filepath.Join(hl.InstallDir(), "app-manifest.yml")
		if _, err = os.Stat(manPath); os.IsNotExist(err) {
			return nil, util.Errorf("No app manifest was found in the Hoist launchable %s", hl.ServiceId)
		}
	}
	return artifact.ManifestFromPath(manPath)
}

func (hl *Launchable) CurrentDir() string {
	return filepath.Join(hl.RootDir, "current")
}

func (hl *Launchable) MakeCurrent() error {
	// TODO: unexport this method (requires integrating BuildRunitServices into this API)
	return hl.flipSymlink(hl.CurrentDir())
}

func (hl *Launchable) LastDir() string {
	return filepath.Join(hl.RootDir, "last")
}

func (hl *Launchable) makeLast() error {
	return hl.flipSymlink(hl.LastDir())
}

func (hl *Launchable) flipSymlink(newLinkPath string) error {
	dir, err := ioutil.TempDir(hl.RootDir, hl.ServiceId)
	if err != nil {
		return util.Errorf("Couldn't create temporary directory for symlink: %s", err)
	}
	defer os.RemoveAll(dir)
	tempLinkPath := filepath.Join(dir, hl.ServiceId)
	err = os.Symlink(hl.InstallDir(), tempLinkPath)
	if err != nil {
		return util.Errorf("Couldn't create symlink for hoist launchable %s: %s", hl.ServiceId, err)
	}

	uid, gid, err := user.IDs(hl.GetOwnAs())
	if err != nil {
		return util.Errorf("Couldn't retrieve UID/GID for hoist launchable %s user %s: %s", hl.ServiceId, hl.GetOwnAs(), err)
	}
	err = os.Lchown(tempLinkPath, uid, gid)
	if err != nil {
		return util.Errorf("Couldn't lchown symlink for hoist launchable %s: %s", hl.ServiceId, err)
	}

	return os.Rename(tempLinkPath, newLinkPath)
}

func (hl *Launchable) GetOwnAs() string {
	if hl.OwnAs != "" {
		return hl.OwnAs
	}

	return hl.RunAs
}

func (hl *Launchable) EnvDir() string {
	return filepath.Join(hl.RootDir, "env")
}

func (hl *Launchable) AllInstallsDir() string {
	return filepath.Join(hl.RootDir, "installs")
}

func (hl *Launchable) InstallDir() string {
	launchableName := hl.Name()
	return filepath.Join(hl.AllInstallsDir(), launchableName)
}

func (hl *Launchable) EnvVars() map[string]string {
	return hl.SuppliedEnvVars
}

func (hl *Launchable) RestartPolicy() runit.RestartPolicy {
	return hl.RestartPolicy_
}

func (hl *Launchable) GetRestartTimeout() time.Duration {
	return hl.RestartTimeout
}
