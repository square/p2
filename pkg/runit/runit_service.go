package runit

import (
	"bytes"
	"os/exec"

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

func (sv *SV) execOnService(service *Service, toRun string) (string, error) {
	cmd := exec.Command(sv.Bin, toRun, service.Path)
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	err := cmd.Run()
	if err != nil {
		return buffer.String(), util.Errorf("Could not %s service %s: %s", toRun, service.Name, err)
	}
	return buffer.String(), nil
}

func (sv *SV) Start(service *Service) (string, error) {
	return sv.execOnService(service, "start")
}

func (sv *SV) Stop(service *Service) (string, error) {
	return sv.execOnService(service, "stop")
}
