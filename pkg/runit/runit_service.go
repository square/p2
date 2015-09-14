package runit

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/square/p2/pkg/util"
)

type SV struct {
	Bin string
}

var DefaultSV = &SV{"/usr/bin/sv"}

var statOutput = regexp.MustCompile(`run: ([/\w\_\-\.]+): \(pid ([\d]+)\) (\d+s); run: log: \(pid ([\d]+)\) (\d+s)`)

type Service struct {
	Path string
	Name string
}

type StatResult struct {
	ChildPID    uint64
	ChildUptime time.Duration
	LogPID      uint64
	LogUptime   time.Duration
}

type StatError error

var (
	NotRunning         StatError = errors.New("RunSV is not running and must be started")
	SuperviseOkMissing           = errors.New("The supervise/ok file is missing")
)

func (sv *SV) waitForSupervision(service *Service) error {
	maxWait := time.After(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(service.Path, "supervise")); !os.IsNotExist(err) {
			return nil
		}
		select {
		case <-maxWait:
			return NotRunning
		case <-time.After(150 * time.Millisecond):
			// no op
		}
	}
}

func (sv *SV) execOnService(service *Service, toRun string) (string, error) {
	err := sv.waitForSupervision(service)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(sv.Bin, toRun, service.Path)
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	cmd.Stderr = &buffer
	err = cmd.Run()
	if err != nil {
		return buffer.String(), util.Errorf("Could not %s service %s: %s. Output: %s", toRun, service.Name, err, buffer.String())
	}
	return buffer.String(), nil
}

func (sv *SV) Start(service *Service) (string, error) {
	return convertToErr(sv.execOnService(service, "start"))
}

func (sv *SV) Stop(service *Service) (string, error) {
	return convertToErr(sv.execOnService(service, "stop"))
}

func (sv *SV) Restart(service *Service) (string, error) {
	return convertToErr(sv.execOnService(service, "restart"))
}

func (sv *SV) Stat(service *Service) (*StatResult, error) {
	out, err := convertToErr(sv.execOnService(service, "stat"))
	if err != nil {
		return nil, err
	}
	return outToStatResult(out)
}

func outToStatResult(out string) (*StatResult, error) {
	matches := statOutput.FindStringSubmatch(out)
	if matches == nil || len(matches) < 6 {
		return nil, util.Errorf("Could not find matching run output for service: %q", matches)
	}
	childPID, err := strconv.ParseUint(matches[2], 0, 32)
	if err != nil {
		return nil, util.Errorf("Could not parse child PID from %s: %v", matches[2], err)
	}
	childUptime, err := time.ParseDuration(matches[3])
	if err != nil {
		return nil, util.Errorf("Could not parse child uptime from %s: %v", matches[3], err)
	}
	logPID, err := strconv.ParseUint(matches[4], 0, 32)
	if err != nil {
		return nil, util.Errorf("Could not parse log PID from %s: %v", matches[4], err)
	}
	logUptime, err := time.ParseDuration(matches[5])
	if err != nil {
		return nil, util.Errorf("Could not parse log uptime from %s: %v", matches[5], err)
	}
	return &StatResult{childPID, childUptime, logPID, logUptime}, nil
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
