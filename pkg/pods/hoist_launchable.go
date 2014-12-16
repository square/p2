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
	"time"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

type Fetcher func(string, string, ...interface{}) error

// A HoistLaunchable represents a particular install of a hoist artifact.
type HoistLaunchable struct {
	Location    string  // A URL where we can download the artifact from.
	Id          string  // A unique identifier for this launchable, used when creating runit services
	RunAs       string  // The user to assume when launching the executable
	ConfigDir   string  // The value for chpst -e. See http://smarden.org/runit/chpst.8.html
	FetchToFile Fetcher // Callback that downloads the file from the remote location.
	RootDir     string  // The root directory of the launchable, containing N:N>=1 installs.
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
	err := hoistLaunchable.MakeCurrent()
	if err != nil {
		return util.Errorf("Could not make %s current: %s", err)
	}

	// Should probably do something with output at some point
	// probably want to do something with output at some point
	err = hoistLaunchable.Start(serviceBuilder, sv)
	if err != nil {
		return util.Errorf("Could not launch %s: %s", hoistLaunchable.Id, err)
	}

	_, err = hoistLaunchable.Enable()
	return err
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

	cmd := exec.Command(cmdPath)
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
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

	// if the service is new, building the runit services also starts them, making the sv start superfluous but harmless
	err := hoistLaunchable.BuildRunitServices(serviceBuilder)
	if err != nil {
		return err
	}

	executables, err := hoistLaunchable.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		_, err := sv.Restart(&executable.Service)
		if err != runit.SuperviseOkMissing {
			return err
		}
		maxRetries := 6
		for i := 0; i < maxRetries; i++ {
			_, err = sv.Stat(&executable.Service)
			if err == nil {
				break
			}
			<-time.After(1 * time.Second)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// Write servicebuilder *.yaml file and run servicebuilder, which will register runit services for this
// launchable. The executables generated are located in the "current" symlink of the hoist artifact.
func (hoistLaunchable *HoistLaunchable) BuildRunitServices(serviceBuilder *runit.ServiceBuilder) error {
	sbTemplate := runit.NewSBTemplate(hoistLaunchable.Id)
	executables, err := hoistLaunchable.Executables(serviceBuilder)
	if err != nil {
		return err
	}

	for _, executable := range executables {
		sbTemplate.AddEntry(executable.Name, []string{
			"/usr/bin/nolimit",
			"/usr/bin/chpst",
			"-u",
			strings.Join([]string{hoistLaunchable.RunAs, hoistLaunchable.RunAs}, ":"),
			"-e",
			hoistLaunchable.ConfigDir,
			executable.execPath,
		})
	}
	_, err = serviceBuilder.Write(sbTemplate)
	if err != nil {
		return err
	}

	_, err = serviceBuilder.Rebuild()
	if err != nil {
		return err
	}

	return nil
}

// Remove servicebuilder *.yaml file and run servicebuilder, which will deregister service
func (hoistLaunchable *HoistLaunchable) RemoveRunitServices(serviceBuilder *runit.ServiceBuilder) error {
	sbTemplate := runit.NewSBTemplate(hoistLaunchable.Id)
	err := serviceBuilder.Remove(sbTemplate)
	if err != nil {
		return err
	}

	_, err = serviceBuilder.Rebuild()
	if err != nil {
		return err
	}

	return nil
}

func (hoistLaunchable *HoistLaunchable) Executables(serviceBuilder *runit.ServiceBuilder) ([]HoistExecutable, error) {
	binLaunchPath := path.Join(hoistLaunchable.InstallDir(), "bin", "launch")

	binLaunchInfo, err := os.Stat(binLaunchPath)
	if err != nil {
		return nil, util.Errorf("%s", err)
	}

	// we support bin/launch being a file, or a directory, so we have to check
	// ideally a launchable will have just one launch script someday (can't be
	// a dir)
	if !(binLaunchInfo.IsDir()) {
		serviceName := hoistLaunchable.Id // use the ID of the launchable as its unique Runit service name
		servicePath := path.Join(serviceBuilder.RunitRoot, serviceName)
		runitService := &runit.Service{servicePath, serviceName}
		executable := &HoistExecutable{*runitService, binLaunchPath}

		return []HoistExecutable{*executable}, nil
	} else {
		services, err := ioutil.ReadDir(binLaunchPath)
		if err != nil {
			return nil, err
		}

		executables := make([]HoistExecutable, len(services))
		for i, service := range services {
			// use the ID of the hoist launchable plus "__" plus the name of the script inside the launch/ directory
			serviceName := strings.Join([]string{hoistLaunchable.Id, "__", service.Name()}, "")
			servicePath := path.Join(serviceBuilder.RunitRoot, serviceName)
			execPath := path.Join(binLaunchPath, service.Name())
			runitService := &runit.Service{servicePath, serviceName}
			executable := &HoistExecutable{*runitService, execPath}
			executables[i] = *executable
		}
		return executables, nil
	}
}

func (hoistLaunchable *HoistLaunchable) Install() error {
	installDir := hoistLaunchable.InstallDir()
	if _, err := os.Stat(installDir); err == nil {
		// install is idempotent, no-op if already installed
		return nil
	}

	outPath := path.Join(os.TempDir(), hoistLaunchable.Version())

	err := hoistLaunchable.FetchToFile(hoistLaunchable.Location, outPath)
	if err != nil {
		return err
	}

	fd, err := os.Open(outPath)
	if err != nil {
		return err
	}
	defer fd.Close()

	err = hoistLaunchable.extractTarGz(fd, installDir)
	if err != nil {
		return err
	}
	return nil
}

func (hoistLaunchable *HoistLaunchable) Uninstall(builder *runit.ServiceBuilder) error {
	err := hoistLaunchable.RemoveRunitServices(builder)
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
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.Errorf("Encountered an error reading gunzipped tar archive: %s", err)
		}
		fpath := path.Join(dest, hdr.Name)
		if hdr.FileInfo().IsDir() {
			continue
		} else {
			dir := path.Dir(fpath)
			os.MkdirAll(dir, 0755)
			f, err := os.OpenFile(
				fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, hdr.FileInfo().Mode())

			if err != nil {
				return util.Errorf("Unable to open destination file when unpacking tar: %s", err)
			}
			defer f.Close()
			err = f.Chown(uid, gid) // this operation may cause tar unpacking to become significantly slower. Refactor as necessary.
			if err != nil {
				return util.Errorf("Unable to chown destination file to user %s: %s", hoistLaunchable.RunAs, err)
			}

			_, err = io.Copy(f, tr)
			if err != nil {
				return util.Errorf("Unable to copy file to destination when extracting tar.gz: %s", err)
			}
		}
	}
	return nil
}
