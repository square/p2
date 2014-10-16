package pods

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"

	curl "github.com/andelf/go-curl"
)

type easy interface {
	Cleanup()
	Perform() error
	Setopt(int, interface{}) error
}

// A HoistLaunchable represents a particular install of a hoist artifact.
type HoistLaunchable struct {
	location string
	id       string
	podId    string
	fetcher  easy
	rootDir  string
}

func (hoistLaunchable *HoistLaunchable) Halt(serviceBuilder *runit.ServiceBuilder) error {

	err := hoistLaunchable.Disable()
	if err != nil {
		return err
	}

	err = hoistLaunchable.Stop(serviceBuilder)
	if err != nil {
		return err
	}

	return nil
}

func (hoistLaunchable *HoistLaunchable) Launch() error {
	return util.Errorf("Not implemented")
}

func (hoistLaunchable *HoistLaunchable) Disable() error {
	cmd := exec.Command(path.Join(hoistLaunchable.InstallDir(), "bin", "disable"))
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}

func (hoistLaunchable *HoistLaunchable) Stop(serviceBuilder *runit.ServiceBuilder) error {
	return util.Errorf("Not implemented")
}

func (hoistLaunchable *HoistLaunchable) RunitServices(serviceBuilder *runit.ServiceBuilder) ([]runit.Service, error) {
	binLaunchPath := path.Join(hoistLaunchable.InstallDir(), "bin", "launch")

	binLaunchInfo, err := os.Stat(binLaunchPath)
	if err != nil {
		return nil, err
	}

	// we support bin/launch being a file, or a directory, so we have to check
	// ideally a launchable will have just one launch script someday (can't be
	// a dir)
	if !(binLaunchInfo.IsDir()) {
		serviceNameComponents := []string{hoistLaunchable.podId, "__", hoistLaunchable.id}
		serviceName := strings.Join(serviceNameComponents, "")
		servicePath := path.Join(serviceBuilder.RunitRoot, serviceName)
		runitService := &runit.Service{servicePath, serviceName}

		return []runit.Service{*runitService}, nil
	} else {
		services, err := ioutil.ReadDir(binLaunchPath)
		if err != nil {
			return nil, err
		}

		runitServices := make([]runit.Service, len(services))
		for i, service := range services {
			serviceNameComponents := []string{hoistLaunchable.podId, "__", hoistLaunchable.id, "__", service.Name()}
			serviceName := strings.Join(serviceNameComponents, "")
			servicePath := path.Join(serviceBuilder.RunitRoot, serviceName)
			runitService := &runit.Service{servicePath, serviceName}
			runitServices[i] = *runitService
		}
		return runitServices, nil
	}
}

func (hoistLaunchable *HoistLaunchable) Install() error {
	installDir := hoistLaunchable.InstallDir()
	if _, err := os.Stat(installDir); err == nil {
		return nil
	}

	easy := hoistLaunchable.fetcher
	defer easy.Cleanup()

	// follow redirects
	easy.Setopt(curl.OPT_FOLLOWLOCATION, true)

	// fail on HTTP 4xx errors
	easy.Setopt(curl.OPT_FAILONERROR, true)
	easy.Setopt(curl.OPT_URL, hoistLaunchable.location)

	easy.Setopt(curl.OPT_WRITEFUNCTION, func(ptr []byte, userdata interface{}) bool {
		file := userdata.(*os.File)
		if _, err := file.Write(ptr); err != nil {
			return false
		}
		return true
	})

	fp, err := os.Create(path.Join(os.TempDir(), hoistLaunchable.Name()))
	if err != nil {
		return err
	}
	defer fp.Close()

	easy.Setopt(curl.OPT_WRITEDATA, fp)

	if err := easy.Perform(); err != nil {
		return err
	}
	fp.Seek(0, 0)

	err = extractTarGz(fp, installDir)
	if err != nil {
		return err
	}
	return nil
}

func (hoistLaunchable *HoistLaunchable) Name() string {
	_, fileName := path.Split(hoistLaunchable.location)
	return fileName
}

func (*HoistLaunchable) Type() string {
	return "hoist"
}

func (hoistLaunchable *HoistLaunchable) InstallDir() string {
	launchableFileName := hoistLaunchable.Name()
	launchableName := launchableFileName[:len(launchableFileName)-len(".tar.gz")]
	return path.Join(hoistLaunchable.rootDir, "installs", launchableName) // need to generalize this (no /data/pods assumption)
}

func extractTarGz(fp *os.File, dest string) (err error) {
	fz, err := gzip.NewReader(fp)
	if err != nil {
		return err
	}
	defer fz.Close()

	tr := tar.NewReader(fz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
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
				return err
			}
			defer f.Close()

			_, err = io.Copy(f, tr)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
