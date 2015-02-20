package pods

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

type Fetcher func(string, string) error

// A HoistLaunchable represents a particular install of a hoist artifact.
type HoistLaunchable struct {
	Location    string  // A URL where we can download the artifact from.
	Id          string  // A unique identifier for this launchable, used when creating runit services
	RunAs       string  // The user to assume when launching the executable
	ConfigDir   string  // The value for chpst -e. See http://smarden.org/runit/chpst.8.html
	FetchToFile Fetcher // Callback that downloads the file from the remote location.
	RootDir     string  // The root directory of the launchable, containing N:N>=1 installs.
	Chpst       string  // The path to chpst
}

func DefaultFetcher() Fetcher {
	return uri.URICopy
}

func (hoistLaunchable *HoistLaunchable) Halt(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {

	// probably want to do something with output at some point
	_, err := hoistLaunchable.Disable()
	if err != nil {
		return err
	}

	// probably want to do something with output at some point
	err = hoistLaunchable.Stop(serviceBuilder, sv)
	if err != nil {
		return err
	}

	return nil
}

func (hoistLaunchable *HoistLaunchable) Launch(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {
	// Should probably do something with output at some point
	// probably want to do something with output at some point
	err := hoistLaunchable.Start(serviceBuilder, sv)
	if err != nil {
		return util.Errorf("Could not launch %s: %s", hoistLaunchable.Id, err)
	}

	_, err = hoistLaunchable.Enable()
	return err
}

func (hoistLaunchable *HoistLaunchable) PostActivate() (string, error) {
	output, err := hoistLaunchable.invokeBinScript("post-activate")

	// providing a post-activate script is optional, ignore those errors
	if err != nil && !os.IsNotExist(err) {
		return output, err
	}

	return output, nil
}

func (hoistLaunchable *HoistLaunchable) Disable() (string, error) {
	output, err := hoistLaunchable.invokeBinScript("disable")

	// providing a disable script is optional, ignore those errors
	if err != nil && !os.IsNotExist(err) {
		return output, err
	}

	return output, nil
}

func (hoistLaunchable *HoistLaunchable) Enable() (string, error) {
	output, err := hoistLaunchable.invokeBinScript("enable")

	// providing an enable script is optional, ignore those errors
	if err != nil && !os.IsNotExist(err) {
		return output, err
	}

	return output, nil
}

func (hoistLaunchable *HoistLaunchable) invokeBinScript(script string) (string, error) {
	cmdPath := path.Join(hoistLaunchable.InstallDir(), "bin", script)
	_, err := os.Stat(cmdPath)
	if err != nil {
		return "", err
	}

	cmd := exec.Command(hoistLaunchable.Chpst, "-u", hoistLaunchable.RunAs, "-e", hoistLaunchable.ConfigDir, cmdPath)
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	cmd.Stderr = &buffer
	err = cmd.Run()
	if err != nil {
		return buffer.String(), err
	}

	return buffer.String(), nil
}

func (hoistLaunchable *HoistLaunchable) Stop(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {
	executables, err := hoistLaunchable.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		_, err := sv.Stop(&executable.Service)
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
func (hoistLaunchable *HoistLaunchable) Start(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {

	executables, err := hoistLaunchable.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		_, err := sv.Restart(&executable.Service)
		if err != runit.SuperviseOkMissing {
			return err
		}
	}

	return nil
}

func (h *HoistLaunchable) Executables(serviceBuilder *runit.ServiceBuilder) ([]HoistExecutable, error) {
	if !h.Installed() {
		return []HoistExecutable{}, util.Errorf("%s is not installed", h.Id)
	}

	binLaunchPath := path.Join(h.InstallDir(), "bin", "launch")

	binLaunchInfo, err := os.Stat(binLaunchPath)
	if err != nil {
		return nil, util.Errorf("%s", err)
	}

	// we support bin/launch being a file, or a directory, so we check here.
	if !(binLaunchInfo.IsDir()) {
		serviceName := strings.Join([]string{h.Id, "__", "launch"}, "")
		servicePath := path.Join(serviceBuilder.RunitRoot, serviceName)
		runitService := &runit.Service{servicePath, serviceName}
		executable := &HoistExecutable{
			Service:   *runitService,
			ExecPath:  binLaunchPath,
			Chpst:     h.Chpst,
			Nolimit:   "/usr/bin/nolimit",
			RunAs:     h.RunAs,
			ConfigDir: h.ConfigDir,
		}

		return []HoistExecutable{*executable}, nil
	} else {
		services, err := ioutil.ReadDir(binLaunchPath)
		if err != nil {
			return nil, err
		}

		executables := make([]HoistExecutable, len(services))
		for i, service := range services {
			// use the ID of the hoist launchable plus "__" plus the name of the script inside the launch/ directory
			serviceName := strings.Join([]string{h.Id, "__", service.Name()}, "")
			servicePath := path.Join(serviceBuilder.RunitRoot, serviceName)
			execPath := path.Join(binLaunchPath, service.Name())
			runitService := &runit.Service{servicePath, serviceName}
			executable := &HoistExecutable{
				Service:   *runitService,
				ExecPath:  execPath,
				Chpst:     h.Chpst,
				Nolimit:   "/usr/bin/nolimit",
				RunAs:     h.RunAs,
				ConfigDir: h.ConfigDir,
			}
			executables[i] = *executable
		}
		return executables, nil
	}
}

func (hoistLaunchable *HoistLaunchable) Installed() bool {
	installDir := hoistLaunchable.InstallDir()
	_, err := os.Stat(installDir)
	return err == nil
}

func (hoistLaunchable *HoistLaunchable) Install() error {
	if hoistLaunchable.Installed() {
		// install is idempotent, no-op if already installed
		return nil
	}

	outDir, err := ioutil.TempDir("", hoistLaunchable.Version())
	defer os.RemoveAll(outDir)
	if err != nil {
		return util.Errorf("Could not create temporary directory to install %s: %s", hoistLaunchable.Version(), err)
	}

	outPath := path.Join(outDir, hoistLaunchable.Version())

	err = hoistLaunchable.FetchToFile(hoistLaunchable.Location, outPath)
	if err != nil {
		return err
	}

	fd, err := os.Open(outPath)
	if err != nil {
		return err
	}
	defer fd.Close()

	err = hoistLaunchable.extractTarGz(fd, hoistLaunchable.InstallDir())
	if err != nil {
		return err
	}
	return nil
}

// The version of the artifact is currently derived from the location, using
// the naming scheme <the-app>_<unique-version-string>.tar.gz
func (hoistLaunchable *HoistLaunchable) Version() string {
	_, fileName := path.Split(hoistLaunchable.Location)
	return fileName[:len(fileName)-len(".tar.gz")]
}

func (*HoistLaunchable) Type() string {
	return "hoist"
}

func (hoistLaunchable *HoistLaunchable) AppManifest() (*artifact.AppManifest, error) {
	if !hoistLaunchable.Installed() {
		return nil, util.Errorf("%s has not been installed yet", hoistLaunchable.Id)
	}
	manPath := path.Join(hoistLaunchable.InstallDir(), "app-manifest.yaml")
	if _, err := os.Stat(manPath); os.IsNotExist(err) {
		manPath = path.Join(hoistLaunchable.InstallDir(), "app-manifest.yml")
		if _, err = os.Stat(manPath); os.IsNotExist(err) {
			return nil, util.Errorf("No app manifest was found in the Hoist launchable %s", hoistLaunchable.Id)
		}
	}
	return artifact.ManifestFromPath(manPath)
}

func (hoistLaunchable *HoistLaunchable) CurrentDir() string {
	return path.Join(hoistLaunchable.RootDir, "current")
}

func (hoistLaunchable *HoistLaunchable) MakeCurrent() error {
	dir, err := ioutil.TempDir(hoistLaunchable.RootDir, hoistLaunchable.Id)
	if err != nil {
		return util.Errorf("Couldn't create temporary directory for symlink: %s", err)
	}
	defer os.RemoveAll(dir)
	tempLinkPath := path.Join(dir, hoistLaunchable.Id)
	err = os.Symlink(hoistLaunchable.InstallDir(), tempLinkPath)
	if err != nil {
		return util.Errorf("Couldn't create symlink for hoist launchable %s: %s", hoistLaunchable.Id, err)
	}

	uid, gid, err := user.IDs(hoistLaunchable.RunAs)
	if err != nil {
		return util.Errorf("Couldn't retrieve UID/GID for hoist launchable %s user %s: %s", hoistLaunchable.Id, hoistLaunchable.RunAs, err)
	}
	err = os.Lchown(tempLinkPath, uid, gid)
	if err != nil {
		return util.Errorf("Couldn't lchown symlink for hoist launchable %s: %s", hoistLaunchable.Id, err)
	}

	return os.Rename(tempLinkPath, hoistLaunchable.CurrentDir())
}

func (hoistLaunchable *HoistLaunchable) InstallDir() string {
	launchableName := hoistLaunchable.Version()
	return path.Join(hoistLaunchable.RootDir, "installs", launchableName)
}

func (hoistLaunchable *HoistLaunchable) extractTarGz(fp *os.File, dest string) (err error) {
	fz, err := gzip.NewReader(fp)
	if err != nil {
		return util.Errorf("Unable to create gzip reader: %s", err)
	}
	defer fz.Close()

	tr := tar.NewReader(fz)
	uid, gid, err := user.IDs(hoistLaunchable.RunAs)
	if err != nil {
		return err
	}

	err = util.MkdirChownAll(dest, uid, gid, 0755)
	if err != nil {
		return util.Errorf("Unable to create root directory %s when unpacking %s: %s", dest, fp.Name(), err)
	}
	err = os.Chown(dest, uid, gid)
	if err != nil {
		return util.Errorf("Unable to chown root directory %s to %s when unpacking %s: %s", dest, hoistLaunchable.RunAs, fp.Name(), err)
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.Errorf("Unable to read %s: %s", fp.Name(), err)
		}
		fpath := path.Join(dest, hdr.Name)

		if hdr.FileInfo().IsDir() {
			err = os.Mkdir(fpath, hdr.FileInfo().Mode())
			if err != nil && !os.IsExist(err) {
				return util.Errorf("Unable to create destination directory %s when unpacking %s: %s", fpath, fp.Name(), err)
			}

			err = os.Chown(fpath, uid, gid)
			if err != nil {
				return util.Errorf("Unable to chown destination directory %s to %s when unpacking %s: %s", fpath, hoistLaunchable.RunAs, fp.Name(), err)
			}
		} else {
			f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, hdr.FileInfo().Mode())
			if err != nil {
				return util.Errorf("Unable to open destination file %s when unpacking %s: %s", fpath, fp.Name(), err)
			}
			defer f.Close()

			err = f.Chown(uid, gid) // this operation may cause tar unpacking to become significantly slower. Refactor as necessary.
			if err != nil {
				return util.Errorf("Unable to chown destination file %s to %s when unpacking %s: %s", fpath, hoistLaunchable.RunAs, fp.Name(), err)
			}

			_, err = io.Copy(f, tr)
			if err != nil {
				return util.Errorf("Unable to copy into destination file %s when unpacking %s: %s", fpath, fp.Name(), err)
			}
			f.Close() // eagerly release file descriptors rather than letting them pile up
		}
	}
	return nil
}
