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
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

// A HoistLaunchable represents a particular install of a hoist artifact.
type Launchable struct {
	location         string         // A URL where we can download the artifact from.
	id               string         // A unique identifier for this launchable, used when creating runit services
	runAs            string         // The user to assume when launching the executable
	configDir        string         // The value for chpst -e. See http://smarden.org/runit/chpst.8.html
	fetcher          uri.Fetcher    // Callback that downloads the file from the remote location.
	rootDir          string         // The root directory of the launchable, containing N:N>=1 installs.
	p2exec           string         // The path to p2-exec
	cgroupConfig     cgroups.Config // Cgroup parameters to use with p2-exec
	cgroupConfigName string         // The string in PLATFORM_CONFIG to pass to p2-exec
	restartTimeout   time.Duration  // How long to wait when restarting the services in this launchable.
}

func NewLaunchable(
	location string,
	id string,
	runAs string,
	configDir string,
	fetcher uri.Fetcher,
	rootDir string,
	p2exec string,
	cgroupConfig cgroups.Config,
	cgroupConfigName string,
	restartTimeout time.Duration,
) launch.Launchable {
	return &Launchable{
		location:         location,
		id:               id,
		runAs:            runAs,
		configDir:        configDir,
		fetcher:          fetcher,
		rootDir:          rootDir,
		p2exec:           p2exec,
		cgroupConfig:     cgroupConfig,
		cgroupConfigName: cgroupConfigName,
		restartTimeout:   restartTimeout,
	}
}

func (l *Launchable) ID() string {
	return l.id
}

func (l *Launchable) Fetcher() uri.Fetcher {
	return l.fetcher
}

func (hl *Launchable) Halt(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {
	// the error return from os/exec.Run is almost always meaningless
	// ("exit status 1")
	// since the output is more useful to the user, that's what we'll preserve
	out, err := hl.disable()
	if err != nil {
		return launch.DisableError(util.Errorf("%s", out))
	}

	// probably want to do something with output at some point
	err = hl.stop(serviceBuilder, sv)
	if err != nil {
		return launch.StopError(err)
	}

	err = hl.makeLast()
	if err != nil {
		return err
	}

	return nil
}

func (hl *Launchable) Launch(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {
	err := hl.start(serviceBuilder, sv)
	if err != nil {
		return launch.StartError(err)
	}

	// same as disable
	out, err := hl.enable()
	if err != nil {
		return launch.EnableError(util.Errorf("%s", out))
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
		return output, util.Errorf("Could not disable %s: %s", hl.id, err)
	}

	return output, nil
}

func (hl *Launchable) enable() (string, error) {
	output, err := hl.InvokeBinScript("enable")

	// providing an enable script is optional, ignore those errors
	if err != nil && !os.IsNotExist(err) {
		return output, util.Errorf("Could not enable %s: %s", hl.id, err)
	}

	return output, nil
}

func (hl *Launchable) InvokeBinScript(script string) (string, error) {
	cmdPath := filepath.Join(hl.InstallDir(), "bin", script)
	_, err := os.Stat(cmdPath)
	if err != nil {
		return "", err
	}

	cmd := exec.Command(
		hl.p2exec,
		"-n",
		"-u",
		hl.runAs,
		"-e",
		hl.configDir,
		"-l",
		hl.cgroupConfigName,
		"-c",
		hl.id,
		cmdPath,
	)
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	cmd.Stderr = &buffer
	err = cmd.Run()
	if err != nil {
		return buffer.String(), err
	}

	return buffer.String(), nil
}

func (hl *Launchable) stop(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {
	executables, err := hl.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		_, err := sv.Stop(&executable.Service, hl.restartTimeout)
		if err != nil {
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
func (hl *Launchable) start(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {
	executables, err := hl.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		_, err := sv.Restart(&executable.Service, hl.restartTimeout)
		if err != nil && err != runit.SuperviseOkMissing {
			return err
		}
	}

	return nil
}

func (hl *Launchable) Executables(
	serviceBuilder *runit.ServiceBuilder,
) ([]launch.Executable, error) {
	if !hl.Installed() {
		return []launch.Executable{}, util.Errorf("%s is not installed", hl.id)
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
		serviceName := fmt.Sprintf("%s__%s", hl.id, service.Name())
		execCmd := []string{
			hl.p2exec,
			"-n",
			"-u",
			hl.runAs,
			"-e",
			hl.configDir,
			"-l",
			hl.cgroupConfigName,
			"-c",
			hl.id,
			filepath.Join(serviceDir, service.Name()),
		}

		executables = append(executables, launch.Executable{
			Service: runit.Service{
				Path: filepath.Join(serviceBuilder.RunitRoot, serviceName),
				Name: serviceName,
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
	artifactFile, err := ioutil.TempFile("", path.Base(hl.location))
	if err != nil {
		return err
	}
	defer os.Remove(artifactFile.Name())
	defer artifactFile.Close()
	remoteData, err := hl.fetcher.Open(hl.location)
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

	err = gzip.ExtractTarGz(hl.runAs, artifactFile, hl.Version(), hl.InstallDir())
	if err != nil {
		os.RemoveAll(hl.InstallDir())
	}
	return err
}

// The version of the artifact is currently derived from the location, using
// the naming scheme <the-app>_<unique-version-string>.tar.gz
func (hl *Launchable) Version() string {
	fileName := filepath.Base(hl.location)
	return fileName[:len(fileName)-len(".tar.gz")]
}

func (*Launchable) Type() string {
	return "hoist"
}

func (hl *Launchable) AppManifest() (*artifact.AppManifest, error) {
	if !hl.Installed() {
		return nil, util.Errorf("%s has not been installed yet", hl.id)
	}
	manPath := filepath.Join(hl.InstallDir(), "app-manifest.yaml")
	if _, err := os.Stat(manPath); os.IsNotExist(err) {
		manPath = filepath.Join(hl.InstallDir(), "app-manifest.yml")
		if _, err = os.Stat(manPath); os.IsNotExist(err) {
			return nil, util.Errorf("No app manifest was found in the Hoist launchable %s", hl.id)
		}
	}
	return artifact.ManifestFromPath(manPath)
}

func (hl *Launchable) CurrentDir() string {
	return filepath.Join(hl.rootDir, "current")
}

func (hl *Launchable) MakeCurrent() error {
	// TODO: unexport this method (requires integrating BuildRunitServices into this API)
	return hl.flipSymlink(hl.CurrentDir())
}

func (hl *Launchable) LastDir() string {
	return filepath.Join(hl.rootDir, "last")
}

func (hl *Launchable) makeLast() error {
	return hl.flipSymlink(hl.LastDir())
}

func (hl *Launchable) flipSymlink(newLinkPath string) error {
	dir, err := ioutil.TempDir(hl.rootDir, hl.id)
	if err != nil {
		return util.Errorf("Couldn't create temporary directory for symlink: %s", err)
	}
	defer os.RemoveAll(dir)
	tempLinkPath := filepath.Join(dir, hl.id)
	err = os.Symlink(hl.InstallDir(), tempLinkPath)
	if err != nil {
		return util.Errorf("Couldn't create symlink for hoist launchable %s: %s", hl.id, err)
	}

	uid, gid, err := user.IDs(hl.runAs)
	if err != nil {
		return util.Errorf("Couldn't retrieve UID/GID for hoist launchable %s user %s: %s", hl.id, hl.runAs, err)
	}
	err = os.Lchown(tempLinkPath, uid, gid)
	if err != nil {
		return util.Errorf("Couldn't lchown symlink for hoist launchable %s: %s", hl.id, err)
	}

	return os.Rename(tempLinkPath, newLinkPath)
}

func (hl *Launchable) InstallDir() string {
	launchableName := hl.Version()
	return filepath.Join(hl.rootDir, "installs", launchableName)
}

func (hl *Launchable) Location() string {
	return hl.location
}

func (hl *Launchable) RunAs() string {
	return hl.runAs
}
