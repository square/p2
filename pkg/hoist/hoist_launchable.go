package hoist

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/gzip"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/p2exec"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

// A HoistLaunchable represents a particular install of a hoist artifact.
type Launchable struct {
	Location         string              // A URL where we can download the artifact from.
	Id               string              // A (pod-wise) unique identifier for this launchable, used to distinguish it from other launchables in the pod
	ServiceId        string              // A (host-wise) unique identifier for this launchable, used when creating runit services
	RunAs            string              // The user to assume when launching the executable
	PodEnvDir        string              // The value for chpst -e. See http://smarden.org/runit/chpst.8.html
	Fetcher          uri.Fetcher         // Callback that downloads the file from the remote location.
	RootDir          string              // The root directory of the launchable, containing N:N>=1 installs.
	P2Exec           string              // Struct that can be used to build a p2-exec invocation with appropriate flags
	ExecNoLimit      bool                // If set, execute with the -n (--no-limit) argument to p2-exec
	CgroupConfig     cgroups.Config      // Cgroup parameters to use with p2-exec
	CgroupConfigName string              // The string in PLATFORM_CONFIG to pass to p2-exec
	RestartTimeout   time.Duration       // How long to wait when restarting the services in this launchable.
	RestartPolicy    runit.RestartPolicy // Dictates whether the launchable should be automatically restarted upon exit.
	SuppliedEnvVars  map[string]string   // A map of user-supplied environment variables to be exported for this launchable
}

// LaunchAdapter adapts a hoist.Launchable to the launch.Launchable interface.
type LaunchAdapter struct {
	*Launchable
}

func (a LaunchAdapter) ID() string {
	return a.Launchable.Id
}

func (a LaunchAdapter) ServiceID() string {
	return a.Launchable.ServiceId
}

func (a LaunchAdapter) Fetcher() uri.Fetcher {
	return a.Launchable.Fetcher
}

var _ launch.Launchable = &LaunchAdapter{}

// If adapts the hoist Launchable to the launch.Launchable interface.
func (hl *Launchable) If() launch.Launchable {
	return LaunchAdapter{Launchable: hl}
}

func (hl *Launchable) Halt(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	// the error return from os/exec.Run is almost always meaningless
	// ("exit status 1")
	// since the output is more useful to the user, that's what we'll preserve
	out, err := hl.disable()
	if err != nil {
		return launch.DisableError{util.Errorf("%s", out)}
	}

	// probably want to do something with output at some point
	err = hl.stop(serviceBuilder, sv)
	if err != nil {
		return launch.StopError{err}
	}

	err = hl.makeLast()
	if err != nil {
		return err
	}

	return nil
}

func (hl *Launchable) Launch(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error {
	err := hl.start(serviceBuilder, sv)
	if err != nil {
		return launch.StartError{err}
	}

	// same as disable
	out, err := hl.enable()
	if err != nil {
		return launch.EnableError{util.Errorf("%s", out)}
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

	cgroupName := hl.ServiceId
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
		if hl.RestartPolicy == runit.RestartPolicyAlways {
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

func (hl *Launchable) Executables(
	serviceBuilder *runit.ServiceBuilder,
) ([]launch.Executable, error) {
	if !hl.Installed() {
		return []launch.Executable{}, util.Errorf("%s is not installed", hl.ServiceId)
	}

	binLaunchPath := filepath.Join(hl.InstallDir(), "bin", "launch")

	binLaunchInfo, err := os.Stat(binLaunchPath)
	if os.IsNotExist(err) {
		return []launch.Executable{}, nil
	} else if err != nil {
		return nil, util.Errorf("%s", err)
	}

	// we support bin/launch being a file, or a directory, so we check here.
	services := []os.FileInfo{binLaunchInfo}
	serviceDir := filepath.Dir(binLaunchPath)
	if binLaunchInfo.IsDir() {
		serviceDir = binLaunchPath
		services, err = ioutil.ReadDir(binLaunchPath)
		if err != nil {
			return nil, err
		}
	}

	var executables []launch.Executable
	for _, service := range services {
		serviceName := fmt.Sprintf("%s__%s", hl.ServiceId, service.Name())
		p2ExecArgs := p2exec.P2ExecArgs{
			Command:          []string{filepath.Join(serviceDir, service.Name())},
			User:             hl.RunAs,
			EnvDirs:          []string{hl.PodEnvDir, hl.EnvDir()},
			NoLimits:         hl.ExecNoLimit,
			CgroupConfigName: hl.CgroupConfigName,
			CgroupName:       hl.ServiceId,
		}
		execCmd := append([]string{hl.P2Exec}, p2ExecArgs.CommandLine()...)

		executables = append(executables, launch.Executable{
			Service: runit.Service{
				Path: filepath.Join(serviceBuilder.RunitRoot, serviceName),
				Name: serviceName,
			},
			LogAgent: runit.Service{
				Path: filepath.Join(serviceBuilder.RunitRoot, serviceName, "log"),
				Name: serviceName + " logAgent",
			},
			Exec: execCmd,
		})
	}
	return executables, nil
}

func (hl *Launchable) Installed() bool {
	installDir := hl.InstallDir()
	_, err := os.Stat(installDir)
	return err == nil
}

func (hl *Launchable) Install() error {
	if hl.Installed() {
		// install is idempotent, no-op if already installed
		return nil
	}

	// Write to a temporary file for easy cleanup if the network transfer fails
	artifactFile, err := ioutil.TempFile("", path.Base(hl.Location))
	if err != nil {
		return err
	}
	defer os.Remove(artifactFile.Name())
	defer artifactFile.Close()
	remoteData, err := hl.Fetcher.Open(hl.Location)
	if err != nil {
		return err
	}
	defer remoteData.Close()
	_, err = io.Copy(artifactFile, remoteData)
	if err != nil {
		return err
	}
	_, err = artifactFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	err = gzip.ExtractTarGz(hl.RunAs, artifactFile, hl.InstallDir())
	if err != nil {
		_ = os.RemoveAll(hl.InstallDir())
		return util.Errorf("error while extracting %s: %s", hl.Version(), err)
	}
	return err
}

// The version of the artifact is currently derived from the location, using
// the naming scheme <the-app>_<unique-version-string>.tar.gz
func (hl *Launchable) Version() string {
	fileName := filepath.Base(hl.Location)
	return fileName[:len(fileName)-len(".tar.gz")]
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

	uid, gid, err := user.IDs(hl.RunAs)
	if err != nil {
		return util.Errorf("Couldn't retrieve UID/GID for hoist launchable %s user %s: %s", hl.ServiceId, hl.RunAs, err)
	}
	err = os.Lchown(tempLinkPath, uid, gid)
	if err != nil {
		return util.Errorf("Couldn't lchown symlink for hoist launchable %s: %s", hl.ServiceId, err)
	}

	return os.Rename(tempLinkPath, newLinkPath)
}

func (hl *Launchable) EnvDir() string {
	return filepath.Join(hl.RootDir, "env")
}

func (hl *Launchable) AllInstallsDir() string {
	return filepath.Join(hl.RootDir, "installs")
}

func (hl *Launchable) InstallDir() string {
	launchableName := hl.Version()
	return filepath.Join(hl.AllInstallsDir(), launchableName)
}

func (hl *Launchable) EnvVars() map[string]string {
	return hl.SuppliedEnvVars
}
