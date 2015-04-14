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

var DefaultCgexec = "/bin/cgexec"

// maps cgroup subsystems to their respective paths
type Subsystems struct {
	CPU    string
	Memory string
}

var Default Subsystems = Subsystems{
	CPU:    "/cgroup/cpu",
	Memory: "/cgroup/memory",
}

type UnsupportedError string

func (err UnsupportedError) Error() string {
	return fmt.Sprintf("subsystem %q is not available on this system", err)
}

// Find retrieves the mount points for all cgroup subsystems on the host. The
// result of this operation should be cached if possible.
func Find() (Subsystems, error) {
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

	return ret, nil
}

// set the number of logical CPUs in a given cgroup, 0 to unrestrict
// https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
func (subsys Subsystems) SetCPU(name string, cpus int) error {
	if subsys.CPU == "" {
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

	err := os.Mkdir(filepath.Join(subsys.CPU, name), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	_, err = writeIfChanged(
		filepath.Join(subsys.CPU, name, "cpu.cfs_period_us"),
		[]byte(strconv.Itoa(period)+"\n"),
		0,
	)
	if err != nil {
		return err
	}

	_, err = writeIfChanged(
		filepath.Join(subsys.CPU, name, "cpu.cfs_quota_us"),
		[]byte(strconv.Itoa(quota)+"\n"),
		0,
	)
	if err != nil {
		return err
	}

	return nil
}

// set the memory limit on a cgroup, 0 to unrestrict
// https://www.kernel.org/doc/Documentation/cgroups/memory.txt
func (subsys Subsystems) SetMemory(name string, bytes int) error {
	if subsys.Memory == "" {
		return UnsupportedError("memory")
	}

	softLimit := bytes
	hardLimit := bytes + bytes/10
	if hardLimit < softLimit {
		// Deal with overflow
		hardLimit = softLimit
	}
	if bytes == 0 {
		softLimit = -1
		hardLimit = -1
	}

	err := os.Mkdir(filepath.Join(subsys.Memory, name), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	// the hard memory limit must be set BEFORE the mem+swap limit
	// so we must clear the swap limit at the start
	err = ioutil.WriteFile(filepath.Join(subsys.Memory, name, "memory.memsw.limit_in_bytes"), []byte("-1\n"), 0)
	if err != nil {
		return err
	}

	_, err = writeIfChanged(filepath.Join(subsys.Memory, name, "memory.soft_limit_in_bytes"), []byte(strconv.Itoa(softLimit)+"\n"), 0)
	if err != nil {
		return err
	}

	_, err = writeIfChanged(filepath.Join(subsys.Memory, name, "memory.limit_in_bytes"), []byte(strconv.Itoa(hardLimit)+"\n"), 0)
	if err != nil {
		return err
	}

	_, err = writeIfChanged(filepath.Join(subsys.Memory, name, "memory.memsw.limit_in_bytes"), []byte(strconv.Itoa(hardLimit)+"\n"), 0)
	if err != nil {
		return err
	}

	return nil
}

func (subsys Subsystems) Write(config Config) error {
	err := subsys.SetCPU(config.Name, config.CPUs)
	if err != nil {
		return err
	}
	return subsys.SetMemory(config.Name, config.Memory)
}

func (subsys Subsystems) AddPID(name string, pid int) error {
	err := appendIntToFile(filepath.Join(subsys.Memory, name, "tasks"), pid)
	if err != nil {
		return err
	}
	return appendIntToFile(filepath.Join(subsys.CPU, name, "tasks"), pid)
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

func appendIntToFile(filename string, data int) error {
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()
	_, err = fd.WriteString(strconv.Itoa(data))
	return err
}
