package runit

import (
	"bytes"
	"errors"
	"os/exec"
	"strings"

	"github.com/square/p2/pkg/util"
)

type SV struct {
	Bin string
}

var DefaultSV = &SV{"/usr/bin/sv"}

type Service struct {
	Path string
	Name string
}

type StatError error

var (
	NotRunning         StatError = errors.New("RunSV is not running and must be started")
	SuperviseOkMissing           = errors.New("The supervise/ok file is missing")
)

func (sv *SV) execOnService(service *Service, toRun string) (string, error) {
	cmd := exec.Command(sv.Bin, toRun, service.Path)
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	cmd.Stderr = &buffer
	err := cmd.Run()
	if err != nil {
		return buffer.String(), util.Errorf("Could not %s service %s: %s. Output: %s", toRun, service.Name, err, buffer.String())
	}
	return buffer.String(), nil
}

func (sv *SV) Start(service *Service) (string, error) {
	return sv.execOnService(service, "start")
}

func (sv *SV) Stop(service *Service) (string, error) {
	return sv.execOnService(service, "stop")
}

func (sv *SV) Restart(service *Service) (string, error) {
	out, err := sv.execOnService(service, "restart")
	return convertToErr(out, err)
}

func (sv *SV) Stat(service *Service) (string, error) {
	return convertToErr(sv.execOnService(service, "stat"))
}

func convertToErr(msg string, original error) (string, error) {
	if original == nil {
		return msg, nil
	} else if strings.Contains(msg, "runsv not running") {
		return msg, NotRunning
	} else if strings.Contains(msg, "supervise/ok") {
		return msg, SuperviseOkMissing
	} else {
		return msg, original
	}
}
