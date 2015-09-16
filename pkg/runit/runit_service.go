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

func (sv *SV) execCmdOnService(service *Service, cmd *exec.Cmd) (string, error) {
	err := sv.waitForSupervision(service)
	if err != nil {
		return "", err
	}
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	cmd.Stderr = &buffer
	err = cmd.Run()
	if err != nil {
		return buffer.String(), util.Errorf("Could not run %v - Error: %s, Output: %s", cmd.Args, err, buffer.String())
	}
	return buffer.String(), nil
}

func (sv *SV) execOnService(service *Service, svVerb string) (string, error) {
	cmd := exec.Command(sv.Bin, svVerb, service.Path)
	return sv.execCmdOnService(service, cmd)
}

func (sv *SV) Start(service *Service) (string, error) {
	return convertToErr(sv.execOnService(service, "start"))
}

func (sv *SV) Stop(service *Service) (string, error) {
	return convertToErr(sv.execOnService(service, "stop"))
}

func (sv *SV) Stat(service *Service) (*StatResult, error) {
	out, err := convertToErr(sv.execOnService(service, "stat"))
	if err != nil {
		return nil, err
	}
	return outToStatResult(out)
}

// If timeout is passed, will use the force-restart command to send a kill. If no timeout
// is provided, will just send a TERM.
func (sv *SV) Restart(service *Service, timeout time.Duration) (string, error) {
	var cmd *exec.Cmd
	if timeout > 0 {
		cmd = exec.Command(sv.Bin, "-w", strconv.FormatInt(int64(timeout.Seconds()), 10), "force-restart", service.Path)
	} else {
		cmd = exec.Command(sv.Bin, "restart", service.Path)
	}
	return convertToErr(sv.execCmdOnService(service, cmd))
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
