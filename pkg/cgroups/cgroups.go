package cgroups

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// maps cgroup subsystems to their respective paths
type Cgroups struct {
	CPU    string
	Memory string
}

type UnsupportedError string

func (err UnsupportedError) Error() string {
	return fmt.Sprintf("subsystem %q is not available on this system", err)
}

// Find retrieves the mount points for all cgroup subsystems on the host. The
// result of this operation should be cached if possible.
func Find() (Cgroups, error) {
	// refer to `man proc` or
	// https://www.kernel.org/doc/Documentation/filesystems/proc.txt section 3.5
	mountInfo, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return Cgroups{}, err
	}
	defer mountInfo.Close()

	var ret Cgroups
	scanner := bufio.NewScanner(mountInfo)
	for scanner.Scan() {
		lineSegs := strings.Fields(scanner.Text())
		if lineSegs[len(lineSegs)-3] != "cgroup" {
			// filesystem type is not "cgroup", skip
			continue
		}
		mountPoint := lineSegs[4]

		cgroupSuperOpts := strings.Split(lineSegs[len(lineSegs)-1], ",")
		for _, opt := range cgroupSuperOpts {
			switch opt {
			case "cpu":
				ret.CPU = mountPoint
			case "memory":
				ret.Memory = mountPoint
			}
		}
	}

	return ret, nil
}

var Default Cgroups = Cgroups{
	CPU:    "/cgroup/cpu",
	Memory: "/cgroup/memory",
}

// set the number of logical CPUs in a given cgroup, 0 to unrestrict
// https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
func (cg Cgroups) SetCPU(name string, cpus int) error {
	if cg.CPU == "" {
		return UnsupportedError("cpu")
	}

	period := 1000000 // one million microseconds
	quota := cpus * period
	if cpus == 0 {
		// one hundred thousand microseconds is the default, -1 will return EINVAL
		period = 100000
		// setting -1 here will unrestrict the cgroup, so the period won't matter
		quota = -1
	}

	err := os.Mkdir(filepath.Join(cg.CPU, name), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	_, err = writeIfChanged(filepath.Join(cg.CPU, name, "cpu.cfs_period_us"), []byte(strconv.Itoa(period)+"\n"), 0)
	if err != nil {
		return err
	}

	_, err = writeIfChanged(filepath.Join(cg.CPU, name, "cpu.cfs_quota_us"), []byte(strconv.Itoa(quota)+"\n"), 0)
	if err != nil {
		return err
	}

	return nil
}

// set the memory limit on a cgroup, 0 to unrestrict
// https://www.kernel.org/doc/Documentation/cgroups/memory.txt
func (cg Cgroups) SetMemory(name string, bytes int) error {
	if cg.Memory == "" {
		return UnsupportedError("memory")
	}

	softLimit := bytes
	hardLimit := bytes * 11 / 10
	if bytes == 0 {
		softLimit = -1
		hardLimit = -1
	}

	err := os.Mkdir(filepath.Join(cg.Memory, name), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	// the hard memory limit must be set BEFORE the mem+swap limit
	// so we must clear the swap limit at the start
	err = ioutil.WriteFile(filepath.Join(cg.Memory, name, "memory.memsw.limit_in_bytes"), []byte("-1\n"), 0)
	if err != nil {
		return err
	}

	_, err = writeIfChanged(filepath.Join(cg.Memory, name, "memory.soft_limit_in_bytes"), []byte(strconv.Itoa(softLimit)+"\n"), 0)
	if err != nil {
		return err
	}

	_, err = writeIfChanged(filepath.Join(cg.Memory, name, "memory.limit_in_bytes"), []byte(strconv.Itoa(hardLimit)+"\n"), 0)
	if err != nil {
		return err
	}

	_, err = writeIfChanged(filepath.Join(cg.Memory, name, "memory.memsw.limit_in_bytes"), []byte(strconv.Itoa(hardLimit)+"\n"), 0)
	if err != nil {
		return err
	}

	return nil
}

func writeIfChanged(filename string, data []byte, perm os.FileMode) (bool, error) {
	content, err := ioutil.ReadFile(filename)

	if !os.IsNotExist(err) && err != nil {
		return false, err
	}
	if !os.IsNotExist(err) && bytes.Compare(content, data) == 0 {
		return false, nil
	}

	err = ioutil.WriteFile(filename, data, perm)
	if err != nil {
		return true, err
	}

	if perm != 0 {
		err = os.Chmod(filename, perm)
	}
	return true, err
}
