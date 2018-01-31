package cgroups

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ProcSubsystemer returns cgroup mountpoints based on the /proc filesystem
type ProcSubsystemer struct {
	CachedSubsystems *Subsystems
}

var DefaultSubsystemer = &ProcSubsystemer{}

func (ps *ProcSubsystemer) Find() (Subsystems, error) {
	if ps.CachedSubsystems != nil {
		return *ps.CachedSubsystems, nil
	}
	// For details about how this file is structured, refer to `man proc` or
	// https://www.kernel.org/doc/Documentation/filesystems/proc.txt section 3.5
	mountInfo, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return Subsystems{}, err
	}
	defer mountInfo.Close()

	var ret Subsystems
	scanner := bufio.NewScanner(mountInfo)
	for scanner.Scan() {
		lineSegs := strings.Fields(scanner.Text())
		nSegs := len(lineSegs)
		if nSegs < 10 || lineSegs[nSegs-4] != "-" {
			return Subsystems{}, fmt.Errorf("mountinfo: unrecognized format")
		}
		mountPoint := lineSegs[4]
		fsType := lineSegs[nSegs-3]
		superOptions := strings.Split(lineSegs[nSegs-1], ",")

		if fsType != "cgroup" {
			// filesystem type is not "cgroup", skip
			continue
		}

		for _, opt := range superOptions {
			switch opt {
			case "cpu":
				ret.CPU = mountPoint
			case "memory":
				ret.Memory = mountPoint
			}
		}
	}
	ps.CachedSubsystems = &ret

	return ret, nil
}
