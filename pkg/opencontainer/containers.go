// The "opencontainer" package implements support for launching services packaged in an
// OpenContainer image. Containers can be used by specifying "type: opencontainer" in a
// launchable's configuration in a pod manifest.
//
// P2 support for OpenContainer images is EXPERIMENTAL, even beyond the fact that the
// OpenContainer spec has not released a stable version and is in constant flux.
package opencontainer

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/gzip"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/p2exec"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/param"
	"github.com/square/p2/pkg/util/size"
)

// The name of the OpenContainer spec file in the container's root.
const SpecFilename = "config.json"

// The name of the OpenContainer runtime spec.
const RuntimeSpecFilename = "runtime.json"

// RuncPath is the full path of the "runc" binary.
var RuncPath = param.String("runc_path", "/usr/local/bin/runc")

// Launchable represents an installation of a container.
type Launchable struct {
	Location        string              // A URL where we can download the artifact from.
	ID_             string              // A (pod-wise) unique identifier for this launchable, used to distinguish it from other launchables in the pod
	ServiceID_      string              // A (host-wise) unique identifier for this launchable, used when creating runit services
	RunAs           string              // The user to assume when launching the executable
	RootDir         string              // The root directory of the launchable, containing N:N>=1 installs.
	P2Exec          string              // The path to p2-exec
	RestartTimeout  time.Duration       // How long to wait when restarting the services in this launchable.
	RestartPolicy   runit.RestartPolicy // Dictates whether the container should be automatically restarted upon exit.
	CgroupConfig    cgroups.Config      // Cgroup parameters to use with p2-exec
	SuppliedEnvVars map[string]string   // User-supplied env variables

	spec *LinuxSpec // The container's "config.json"
}

var _ launch.Launchable = &Launchable{}

func (l *Launchable) getSpec() (*LinuxSpec, error) {
	if l.spec != nil {
		return l.spec, nil
	}
	data, err := ioutil.ReadFile(filepath.Join(l.InstallDir(), SpecFilename))
	if err != nil {
		return nil, err
	}
	var spec LinuxSpec
	err = json.Unmarshal(data, &spec)
	if err != nil {
		return nil, err
	}
	l.spec = &spec
	return l.spec, nil
}

// ID implements the launch.Launchable interface. It returns the name of this launchable.
func (l *Launchable) ID() string {
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
	fileName := filepath.Base(hl.Location)
	return fileName[:len(fileName)-len(".tar.gz")]
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

	uid, gid, err := user.IDs(l.RunAs)
	if err != nil {
		return nil, util.Errorf("%s: unknown runas user: %s", l.ServiceID_, l.RunAs)
	}
	lspec, err := l.getSpec()
	if err != nil {
		return nil, util.Errorf("%s: loading container specification: %s", l.ServiceID_, err)
	}
	expectedPlatform := Platform{
		OS:   "linux",
		Arch: "amd64",
	}
	if lspec.Platform != expectedPlatform {
		return nil, util.Errorf(
			"%s: unsupported container platform: %#v expected %#v",
			l.ServiceID_,
			lspec.Platform,
			expectedPlatform,
		)
	}
	if filepath.Base(lspec.Root.Path) != lspec.Root.Path {
		return nil, util.Errorf("%s: invalid container root: %s", l.ServiceID_, lspec.Root.Path)
	}
	luser := lspec.Process.User
	if uid != int(luser.UID) || gid != int(luser.GID) {
		return nil, util.Errorf("%s: cannot execute as %s(%d:%d): container expects %d:%d",
			l.ServiceID_, l.RunAs, uid, gid, luser.UID, luser.GID)
	}

	serviceName := l.ServiceID_ + "__container"
	return []launch.Executable{{
		Service: runit.Service{
			Path: filepath.Join(serviceBuilder.RunitRoot, serviceName),
			Name: serviceName,
		},
		Exec: append(
			[]string{l.P2Exec},
			p2exec.P2ExecArgs{ // TODO: support environment variables
				NoLimits: true,
				WorkDir:  l.InstallDir(),
				Command:  []string{*RuncPath, "start"},
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

// Install ...
func (l *Launchable) Install(_ auth.ArtifactVerifier) (returnedError error) {
	if l.Installed() {
		return nil
	}

	data, err := uri.DefaultFetcher.Open(l.Location)
	if err != nil {
		return err
	}
	defer data.Close()
	defer func() {
		if returnedError != nil {
			_ = os.RemoveAll(l.InstallDir())
		}
	}()
	err = gzip.ExtractTarGz("", data, l.InstallDir())
	if err != nil {
		return util.Errorf("extracting %s: %s", l.Version(), err)
	}
	if _, err = l.getSpec(); err != nil {
		return err
	}

	// Construct the host-specific configuration... This is probably the wrong place for this
	// code because the container cgroup settings depend on the manifest.
	runSpecPath := filepath.Join(l.InstallDir(), RuntimeSpecFilename)
	runSpec := DefaultRuntimeSpec
	runSpecData, err := json.Marshal(runSpec)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(runSpecPath, runSpecData, 0444)
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
	err := l.start(serviceBuilder, sv)
	if err != nil {
		return launch.StartError{err}
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
		if l.RestartPolicy == runit.RestartPolicyAlways {
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

// Halt causes the launchable to halt execution if it is running.
func (l *Launchable) Halt(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	// "disable" script not supported for containers
	err := l.stop(serviceBuilder, sv)
	if err != nil {
		return launch.StopError{err}
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
