// The "opencontainer" package implements support for launching services packaged in an
// OpenContainer image. Containers can be used by specifying "type: opencontainer" in a
// launchable's configuration in a pod manifest.
//
// P2 support for OpenContainer images is EXPERIMENTAL, even beyond the fact that the
// OpenContainer spec has not released a stable version and is in constant flux.
package opencontainer

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/env"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/p2exec"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/param"
	"github.com/square/p2/pkg/util/size"
)

// The name of the OpenContainer spec file in the container's root.
const (
	SpecFilename                           = "config.json"
	OpenContainerLaunchableType            = "opencontainer"
	DefaultContainerConfigTemplateFilename = "config.json.template"
)

// RuncPath is the full path of the "runc" binary.
var RuncPath = param.String("runc_path", "/usr/local/bin/runc")

// Launchable represents an installation of a container.
type Launchable struct {
	ID_               launch.LaunchableID        // A (pod-wise) unique identifier for this launchable, used to distinguish it from other launchables in the pod
	ServiceID_        string                     // A (host-wise) unique identifier for this launchable, used when creating runit services
	RunAs             string                     // The user to assume when launching the executable
	RootDir           string                     // The root directory of the launchable, containing N:N>=1 installs.
	P2Exec            string                     // The path to p2-exec
	RestartTimeout    time.Duration              // How long to wait when restarting the services in this launchable.
	RestartPolicy_    runit.RestartPolicy        // Dictates whether the container should be automatically restarted upon exit.
	CgroupConfig      cgroups.Config             // Cgroup parameters to use with p2-exec
	Version_          launch.LaunchableVersionID // Version of the specified launchable
	SuppliedEnvVars   map[string]string          // User-supplied env variables
	OSVersionDetector osversion.Detector
	ExecNoLimit       bool // If set, execute with the -n (--no-limit) argument to p2-exec

	// These fields are only used to invoke bin/post-install for
	// opencontainer launchables which is only necessary to allow
	// launchables to customize their config.json at runtime. This
	// functoinality could possibly be provided by P2 and these fields
	// could be removed.
	CgroupName       string // The name of the cgroup to run this launchable in
	CgroupConfigName string // The string in PLATFORM_CONFIG to pass to p2-exec
	PodEnvDir        string // The value for chpst -e. See http://smarden.org/runit/chpst.8.html
	RequireFile      string // Do not run this launchable until this file exists

	spec *Spec // The container's "config.json"

	// ContainerBindMountPaths specify environment variables whose values contain paths that should
	// be bind mounted into the container. Typically these environment variables will be configured by
	// a hook. For example, a hook that makes secrets available to a pod might export a SECRETS_PATH
	// environment variable which contains the path to a directory in which the secrets are available.
	// We might see "SECRETS_PATH" as an entry in ContainerBindMountPathEnvVars which will cause the
	// container to have the path at SECRETS_PATH bind mounted into the container
	ContainerBindMountPathEnvVars []string
}

var _ launch.Launchable = &Launchable{}

func (l *Launchable) getSpec() (*Spec, error) {
	if l.spec != nil {
		return l.spec, nil
	}
	data, err := ioutil.ReadFile(filepath.Join(l.InstallDir(), SpecFilename))
	if err != nil {
		return nil, err
	}
	var spec Spec
	err = json.Unmarshal(data, &spec)
	if err != nil {
		return nil, err
	}
	l.spec = &spec
	return l.spec, nil
}

// ID implements the launch.Launchable interface. It returns the name of this launchable.
func (l *Launchable) ID() launch.LaunchableID {
	return l.ID_
}

func (l *Launchable) ServiceID() string {
	return l.ServiceID_
}

func (l *Launchable) EnvVars() map[string]string {
	return l.SuppliedEnvVars
}

// The version of the artifact is currently derived from the location, using
// the naming scheme <the-app>_<unique-version-string>.tar.gz
func (hl *Launchable) Version() string {
	return hl.Version_.String()
}

func (*Launchable) Type() string {
	return "opencontainer"
}

// Fetcher returns a uri.Fetcher that is capable of fetching the launchable's files.
func (l *Launchable) Fetcher() uri.Fetcher {
	return uri.DefaultFetcher
}

func (l *Launchable) EnvDir() string {
	return filepath.Join(l.RootDir, "env")
}

// InstallDir is the directory where this launchable should be installed.
func (l *Launchable) InstallDir() string {
	launchableName := l.Version()
	return filepath.Join(l.RootDir, "installs", launchableName)
}

// Executables gets a list of the runit services that will be built for this launchable.
func (l *Launchable) Executables(serviceBuilder *runit.ServiceBuilder) ([]launch.Executable, error) {
	if !l.Installed() {
		return []launch.Executable{}, util.Errorf("%s is not installed", l.ServiceID_)
	}

	lspec, err := l.getSpec()
	switch {
	case os.IsNotExist(err):
		// if there's no config.json yet that's fine, it might appear as a
		// part of pre-launch at which point we will validate again
	case err != nil:
		return nil, err
	default:
		if err != nil {
			return nil, util.Errorf("%s: loading container specification: %s", l.ServiceID_, err)
		}

		uid, gid, err := user.IDs(l.RunAs)
		if err != nil {
			return nil, util.Errorf("%s: unknown runas user: %s", l.ServiceID_, l.RunAs)
		}

		err = ValidateSpec(lspec, uid, gid)
		if err != nil {
			return nil, err
		}
	}

	runcConfig, err := GetConfig(l.OSVersionDetector)
	if err != nil {
		return nil, err
	}

	runcArgs := []string{*RuncPath}
	if runcConfig.Root != "" {
		runcArgs = append(runcArgs, "--root", runcConfig.Root)
	}
	runcArgs = append(runcArgs, "run")
	if runcConfig.NoNewKeyring {
		runcArgs = append(runcArgs, "--no-new-keyring")
	}
	serviceName := l.ServiceID_ + "__container"
	runcArgs = append(runcArgs, serviceName)

	bindMountPaths, err := l.bindMountPaths()
	if err != nil {
		return nil, util.Errorf(err.Error())
	}

	// TODO: also support adding P2-provided environment variables to the
	// config.json file so that containerized processes can make use of
	// them.
	// The EnvDirs field is set in the P2ExecArgs and used by
	// p2-exec itself, but runc will reset the env before execing the
	// containerized process
	return []launch.Executable{{
		Service: runit.Service{
			Path: filepath.Join(serviceBuilder.RunitRoot, serviceName),
			Name: serviceName,
		},
		Exec: append(
			[]string{l.P2Exec},
			p2exec.P2ExecArgs{
				NoLimits:                        l.ExecNoLimit,
				WorkDir:                         l.InstallDir(),
				EnvDirs:                         []string{l.PodEnvDir, l.EnvDir()},
				Command:                         runcArgs,
				CgroupConfigName:                l.CgroupConfigName,
				CgroupName:                      l.CgroupName,
				LaunchableType:                  OpenContainerLaunchableType,
				ContainerBindMountPaths:         bindMountPaths,
				ContainerConfigTemplateFilename: DefaultContainerConfigTemplateFilename, // TODO: support customization
				User: l.RunAs,
			}.CommandLine()...,
		),
	}}, nil
}

// Installed returns true if this launchable is already installed.
func (l *Launchable) Installed() bool {
	installDir := l.InstallDir()
	_, err := os.Stat(installDir)
	return err == nil
}

func (l *Launchable) PostInstall() (string, error) {
	return "", nil
}

// preLaunch() is a useful feature for opencontainers that need to modify their
// config.json file at runtime, for instance to change the uid/gid based on the
// currently running user
func (l *Launchable) preLaunch() (string, error) {
	output, err := l.InvokeBinScript("pre-launch")

	// providing a pre-launch script is optional, ignore those errors
	if err != nil && !os.IsNotExist(err) {
		return output, err
	}

	return output, nil
}

// InvokeBinScript is shamelessly copied from the hoist launchable
// implementation. It just runs a bin/%s script in the opencontainer
// launchable. This is only necessary so launchables can configure their
// config.json at runtime, for instance when hostname parameters or uid/gid
// parameters should be determined based on running context
func (l *Launchable) InvokeBinScript(script string) (string, error) {
	cmdPath := filepath.Join(l.InstallDir(), "bin", script)
	_, err := os.Stat(cmdPath)
	if err != nil {
		return "", err
	}

	cgroupName := l.CgroupName
	if l.CgroupConfigName == "" {
		cgroupName = ""
	}
	p2ExecArgs := p2exec.P2ExecArgs{
		Command:          []string{cmdPath},
		User:             l.RunAs,
		EnvDirs:          []string{l.PodEnvDir, l.EnvDir()},
		NoLimits:         l.ExecNoLimit,
		CgroupConfigName: l.CgroupConfigName,
		CgroupName:       cgroupName,
		RequireFile:      l.RequireFile,
		ClearEnv:         true,
	}
	cmd := exec.Command(l.P2Exec, p2ExecArgs.CommandLine()...)
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	cmd.Stderr = &buffer
	err = cmd.Run()
	if err != nil {
		return buffer.String(), err
	}

	return buffer.String(), nil
}

// PostActive runs a Hoist-specific "post-activate" script in the launchable.
func (l *Launchable) PostActivate() (string, error) {
	// Not supported in OpenContainer
	return "", nil
}

func (l *Launchable) flipSymlink(newLinkPath string) error {
	dir, err := ioutil.TempDir(l.RootDir, l.ServiceID_)
	if err != nil {
		return util.Errorf("Couldn't create temporary directory for symlink: %s", err)
	}
	defer os.RemoveAll(dir)
	tempLinkPath := filepath.Join(dir, l.ServiceID_)
	err = os.Symlink(l.InstallDir(), tempLinkPath)
	if err != nil {
		return util.Errorf("Couldn't create symlink for OpenContainer launchable %s: %s", l.ServiceID_, err)
	}

	uid, gid, err := user.IDs(l.RunAs)
	if err != nil {
		return util.Errorf("Couldn't retrieve UID/GID for OpenContainer launchable %s user %s: %s", l.ServiceID_, l.RunAs, err)
	}
	err = os.Lchown(tempLinkPath, uid, gid)
	if err != nil {
		return util.Errorf("Couldn't lchown symlink for OpenContainer launchable %s: %s", l.ServiceID_, err)
	}

	return os.Rename(tempLinkPath, newLinkPath)
}

// MakeCurrent adjusts a "current" symlink for this launchable name to point to this
// launchable's version.
func (l *Launchable) MakeCurrent() error {
	return l.flipSymlink(filepath.Join(l.RootDir, "current"))
}

func (l *Launchable) makeLast() error {
	return l.flipSymlink(filepath.Join(l.RootDir, "last"))
}

// Launch allows the launchable to begin execution.
func (l *Launchable) Launch(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	output, err := l.preLaunch()
	if err != nil {
		return util.Errorf("error running pre-launch script: %s\n%s", err, output)
	}

	// we did this when we built the runit services, but for good measure
	// validate the config.json again
	lspec, err := l.getSpec()
	if err != nil {
		return util.Errorf("could not fetch config.json to perform validation before starting the container: %s", err)
	}

	uid, gid, err := user.IDs(l.RunAs)
	if err != nil {
		return util.Errorf("%s: unknown runas user: %s", l.ServiceID_, l.RunAs)
	}

	err = ValidateSpec(lspec, uid, gid)
	if err != nil {
		return err
	}

	err = l.start(serviceBuilder, sv)
	if err != nil {
		return launch.StartError{Inner: err}
	}
	// No "enable" for OpenContainers
	return nil
}

func (l *Launchable) start(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	executables, err := l.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		var err error
		if l.RestartPolicy_ == runit.RestartPolicyAlways {
			_, err = sv.Restart(&executable.Service, l.RestartTimeout)
		} else {
			_, err = sv.Once(&executable.Service)
		}
		if err != nil && err != runit.SuperviseOkMissing {
			return err
		}
	}

	return nil
}

func (l *Launchable) stop(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	executables, err := l.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		_, err := sv.Stop(&executable.Service, l.RestartTimeout)
		if err != nil {
			cmd := exec.Command(
				l.P2Exec,
				p2exec.P2ExecArgs{
					WorkDir: l.InstallDir(),
					Command: []string{*RuncPath, "kill", "SIGKILL"},
				}.CommandLine()...,
			)
			err = cmd.Run()
			if err != nil {
				return util.Errorf("%s: error stopping container: %s", l.ServiceID_, err)
			}
		}
	}
	return nil
}

func (l *Launchable) Disable() error {
	// "disable" script not supported for containers
	return nil
}

// Halt causes the launchable to halt execution if it is running.
func (l *Launchable) Stop(serviceBuilder *runit.ServiceBuilder, sv runit.SV, _ bool) error {
	err := l.stop(serviceBuilder, sv)
	if err != nil {
		return launch.StopError{Inner: err}
	}

	err = l.makeLast()
	if err != nil {
		return err
	}
	return nil
}

func (l *Launchable) Prune(max size.ByteCount) error {
	// No-op for now
	return nil
}

func (l *Launchable) RestartPolicy() runit.RestartPolicy {
	return l.RestartPolicy_
}

func (l *Launchable) GetRestartTimeout() time.Duration {
	return l.RestartTimeout
}

// bindMountPaths() loops over the configured ContainerBindMountPathEnvVars and
// verifies that each environment variable has a path and that that path refers
// to an existing directory that is in the pod's home directory.
func (l *Launchable) bindMountPaths() ([]string, error) {
	podHomePath := os.Getenv(env.PodHomeEnvVar)
	if podHomePath == "" {
		return nil, util.Errorf("%s is not set so it can't be used to validate bind mount paths", env.PodHomeEnvVar)
	}

	var ret []string
	for _, envVar := range l.ContainerBindMountPathEnvVars {
		path := os.Getenv(envVar)
		if path == "" {
			return nil, util.Errorf("container bind mount environment variable %s was not set", envVar)
		}

		isSubdirectory, err := isSubdirectory(podHomePath, path)
		if err != nil {
			return nil, err
		}

		if !isSubdirectory {
			return nil, util.Errorf("environment variable %s contains path %s which is not under %s=%s", envVar, path, env.PodHomeEnvVar, podHomePath)
		}

		_, err = os.Stat(path)
		switch {
		case os.IsNotExist(err):
			return nil, util.Errorf("environment variable %s had value %s, but that directory doesn't exist and couldn't be configured as a bind mount for the container", envVar, path)
		case err != nil:
			return nil, util.Errorf("could not stat %s to check suitability for bind mount (via env var %s): %s", path, envVar, err)
		}

		ret = append(ret, path)
	}

	return ret, nil
}

// isSubdirectory returns true if path is underneath podHome.
func isSubdirectory(podHome string, path string) (bool, error) {
	rel, err := filepath.Rel(podHome, path)
	if err != nil {
		return false, util.Errorf("could not compute relative path from %s to %s", podHome, path)
	}

	parts := strings.Split(rel, string(os.PathSeparator))
	if parts[0] == ".." {
		return false, nil
	}

	return true, nil
}
