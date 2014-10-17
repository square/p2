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

	"github.com/square/p2/pkg/runit"
)

// A HoistLaunchable represents a particular install of a hoist artifact.
type HoistLaunchable struct {
	location    string
	id          string
	podId       string
	fetchToFile func(string, string, ...interface{}) error
	rootDir     string
}

func (hoistLaunchable *HoistLaunchable) Halt(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {

	// probably want to do something with output at some point
	_, err := hoistLaunchable.Disable()
	if err != nil {
		return err
	}

	// probably want to do something with output at some point
	_, err = hoistLaunchable.Stop(serviceBuilder, sv)
	if err != nil {
		return err
	}

	return nil
}

func (hoistLaunchable *HoistLaunchable) Launch(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) error {
	// Should probably do something with output at some point
	// probably want to do something with output at some point
	_, err := hoistLaunchable.Start(serviceBuilder, sv)
	if err != nil {
		return err
	}

	_, err = hoistLaunchable.Enable()
	if err != nil {
		return err
	}

	return nil
}

func (hoistLaunchable *HoistLaunchable) Disable() (string, error) {
	output, err := hoistLaunchable.invokeBinScript("disable")
	if err != nil {
		return output, err
	}

	return output, nil
}

func (hoistLaunchable *HoistLaunchable) Enable() (string, error) {
	output, err := hoistLaunchable.invokeBinScript("enable")
	if err != nil {
		return output, err
	}

	return output, nil
}

func (hoistLaunchable *HoistLaunchable) invokeBinScript(script string) (string, error) {
	cmd := exec.Command(path.Join(hoistLaunchable.InstallDir(), "bin", script))
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	err := cmd.Run()
	if err != nil {
		return buffer.String(), err
	}

	return buffer.String(), nil
}

func (hoistLaunchable *HoistLaunchable) Stop(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) ([]string, error) {
	runitServices, err := hoistLaunchable.RunitServices(serviceBuilder)
	if err != nil {
		return nil, err
	}

	stopOutputs := make([]string, len(runitServices))
	for i, runitService := range runitServices {
		stopOutput, err := sv.Stop(&runitService)
		stopOutputs[i] = stopOutput
		if err != nil {
			// TODO: FAILURE SCENARIO (what should we do here?)
			// 1) does `sv stop` ever exit nonzero?
			// 2) should we keep stopping them all anyway?
			return stopOutputs, err
		}
	}
	return stopOutputs, nil
}

func (hoistLaunchable *HoistLaunchable) Start(serviceBuilder *runit.ServiceBuilder, sv *runit.SV) ([]string, error) {
	runitServices, err := hoistLaunchable.RunitServices(serviceBuilder)
	if err != nil {
		return nil, err
	}

	startOutputs := make([]string, len(runitServices))
	for i, runitService := range runitServices {
		startOutput, err := sv.Start(&runitService)
		startOutputs[i] = startOutput
		if err != nil {
			return startOutputs, err
		}
	}

	return startOutputs, nil
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

	outPath := path.Join(os.TempDir(), hoistLaunchable.Name())

	err := hoistLaunchable.fetchToFile(hoistLaunchable.location, outPath)
	if err != nil {
		return err
	}

	fd, err := os.Open(outPath)
	if err != nil {
		return err
	}
	defer fd.Close()

	err = extractTarGz(fd, installDir)
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
