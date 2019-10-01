package cgroups

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

const (
	CPUPeriod = 1000000 // 1 million microseconds which equals 1 second
)

// maps cgroup subsystems to their respective paths
type Subsystems struct {
	CPU    string
	Memory string
	Prefix string
}

var DefaultSubsystems = Subsystems{
	CPU:    "/cgroup/cpu",
	Memory: "/cgroup/memory",
	Prefix: "/cgroup",
}

type UnsupportedError string

func (err UnsupportedError) Error() string {
	// remember to cast to a string, otherwise %q invokes
	// the Error function again (infinite recursion)
	return fmt.Sprintf("subsystem %q is not available on this system", string(err))
}

type CgroupID string

func (c *CgroupID) String() string {
	return string(*c)
}

// SetCPU will set a number of CPU limits in the cgroup specified by the argument.
// A sentinel value of 0 will create an unrestricted CPU subsystem
// https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
func (subsys Subsystems) SetCPU(name CgroupID, cpus int) error {
	if subsys.CPU == "" {
		return UnsupportedError("cpu")
	}

	err := os.MkdirAll(filepath.Join(subsys.CPU, name.String()), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	// setting the quota to unrestricted first to avoid invalid change of cpu_period
	quota := -1
	_, err = util.WriteIfChanged(
		filepath.Join(subsys.CPU, name.String(), "cpu.cfs_quota_us"),
		[]byte(strconv.Itoa(quota)+"\n"),
		0,
	)

	if cpus != 0 {
		quota = cpus * CPUPeriod
	}

	_, err = util.WriteIfChanged(
		filepath.Join(subsys.CPU, name.String(), "cpu.cfs_period_us"),
		[]byte(strconv.Itoa(CPUPeriod)+"\n"),
		0,
	)
	if err != nil {
		return err
	}

	_, err = util.WriteIfChanged(
		filepath.Join(subsys.CPU, name.String(), "cpu.cfs_quota_us"),
		[]byte(strconv.Itoa(quota)+"\n"),
		0,
	)
	if err != nil {
		return err
	}

	return nil
}

// SetMemory will set several memory limits in the cgroup specified in the argument.
// A sentinel value of 0 will create an unrestricted memory subsystem
// https://www.kernel.org/doc/Documentation/cgroups/memory.txt
func (subsys Subsystems) SetMemory(name CgroupID, bytes int) error {
	if subsys.Memory == "" {
		return UnsupportedError("memory")
	}

	softLimit := bytes
	hardLimit := 2 * bytes
	if hardLimit < softLimit {
		// Deal with overflow
		hardLimit = softLimit
	}
	if bytes == 0 {
		softLimit = -1
		hardLimit = -1
	}

	err := os.MkdirAll(filepath.Join(subsys.Memory, name.String()), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	// the hard memory limit must be set BEFORE the mem+swap limit
	// so we must clear the swap limit at the start
	_, err = util.WriteIfChanged(filepath.Join(subsys.Memory, name.String(), "memory.memsw.limit_in_bytes"), []byte("-1\n"), 0600)
	if err != nil {
		return err
	}

	_, err = util.WriteIfChanged(filepath.Join(subsys.Memory, name.String(), "memory.soft_limit_in_bytes"), []byte(strconv.Itoa(softLimit)+"\n"), 0600)
	if err != nil {
		return err
	}

	_, err = util.WriteIfChanged(filepath.Join(subsys.Memory, name.String(), "memory.limit_in_bytes"), []byte(strconv.Itoa(hardLimit)+"\n"), 0600)
	if err != nil {
		return err
	}

	_, err = util.WriteIfChanged(filepath.Join(subsys.Memory, name.String(), "memory.memsw.limit_in_bytes"), []byte(strconv.Itoa(hardLimit)+"\n"), 0600)
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
	return subsys.SetMemory(config.Name, int(config.Memory))
}

func (subsys Subsystems) AddPID(name string, pid int) error {
	err := appendIntToFile(filepath.Join(subsys.Memory, name, "cgroup.procs"), pid)
	if err != nil {
		return err
	}
	return appendIntToFile(filepath.Join(subsys.CPU, name, "cgroup.procs"), pid)
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

// Subsystemer is anything that can find a set of Subsystems
type Subsystemer interface {
	Find() (Subsystems, error)
}

// CreatePodCgroup will set cgroup parameters for the specified pod with podID
// on hostname. In order for nested cgroups to work correctly, the path used here
// must match the path used in pkg/servicebuilder
func CreatePodCgroup(podID types.PodID, hostname types.NodeName, c Config, s Subsystemer) error {
	ss, err := s.Find()
	if err != nil {
		return err
	}

	cgroupID, err := CgroupIDForPod(s, podID, hostname)
	if err != nil {
		return err
	}
	err = ss.SetCPU(*cgroupID, c.CPUs)
	if err != nil {
		return err
	}
	err = ss.SetMemory(*cgroupID, int(c.Memory))
	if err != nil {
		return err
	}

	return nil
}

// CgroupIDForLaunchable encapsulates the cgroup path for launchable cgroups.
// launchableID is a string because I am too cowardly to break the import cycle
// caused by importing `launch` here.
func CgroupIDForLaunchable(s Subsystemer, podID types.PodID, nodeName types.NodeName, launchableID string) (*CgroupID, error) {
	ss, err := s.Find()
	if err != nil {
		return nil, err
	}
	cgID := CgroupID(filepath.Join(ss.Prefix, "p2", nodeName.String(), podID.String(), launchableID))
	return &cgID, nil
}

// CgroupIDForPod encapsulates the cgroup path for pod cgroups. This function
// should match CgroupIDForLaunchable
func CgroupIDForPod(s Subsystemer, podID types.PodID, nodeName types.NodeName) (*CgroupID, error) {
	ss, err := s.Find()
	if err != nil {
		return nil, err
	}
	cgID := CgroupID(filepath.Join(ss.Prefix, "p2", nodeName.String(), podID.String()))
	return &cgID, nil
}
