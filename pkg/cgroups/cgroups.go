package cgroups

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/square/p2/pkg/util"
	"gopkg.in/yaml.v2"
)

// Subsystems maps cgroup subsystems to their respective paths
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
	// remember to cast to a string, otherwise %q invokes
	// the Error function again (infinite recursion)
	return fmt.Sprintf("subsystem %q is not available on this system", string(err))
}

type Subsystemer interface {
	Find() (Subsystems, error)
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

func FindWithParentGroup(podID string, subsystemer Subsystemer) (Subsystems, error) {
	subsys, err := subsystemer.Find()
	if err != nil {
		return Subsystems{}, err
	}

	node, err := os.Hostname()
	if err != nil {
		return Subsystems{}, err
	}

	parentGroupName := filepath.Join("p2", node, podID)

	subsys.CPU = filepath.Join(subsys.CPU, parentGroupName)
	subsys.Memory = filepath.Join(subsys.Memory, parentGroupName)

	if stat, err := os.Stat(subsys.CPU); err == nil && stat.IsDir() {
		if stat, err := os.Stat(subsys.Memory); err == nil && stat.IsDir() {
			return subsys, nil
		}
	}
	return Subsystems{}, UnsupportedError(parentGroupName)
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

	err := os.MkdirAll(filepath.Join(subsys.CPU, name), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	_, err = util.WriteIfChanged(
		filepath.Join(subsys.CPU, name, "cpu.cfs_period_us"),
		[]byte(strconv.Itoa(period)+"\n"),
		0,
	)
	if err != nil {
		return err
	}

	_, err = util.WriteIfChanged(
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
	hardLimit := 2 * bytes
	if hardLimit < softLimit {
		// Deal with overflow
		hardLimit = softLimit
	}
	if bytes == 0 {
		softLimit = -1
		hardLimit = -1
	}

	err := os.MkdirAll(filepath.Join(subsys.Memory, name), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	// the hard memory limit must be set BEFORE the mem+swap limit
	// so we must clear the swap limit at the start
	err = ioutil.WriteFile(filepath.Join(subsys.Memory, name, "memory.memsw.limit_in_bytes"), []byte("-1\n"), 0600)
	if err != nil {
		return err
	}

	_, err = util.WriteIfChanged(filepath.Join(subsys.Memory, name, "memory.soft_limit_in_bytes"), []byte(strconv.Itoa(softLimit)+"\n"), 0600)
	if err != nil {
		return err
	}

	_, err = util.WriteIfChanged(filepath.Join(subsys.Memory, name, "memory.limit_in_bytes"), []byte(strconv.Itoa(hardLimit)+"\n"), 0600)
	if err != nil {
		return err
	}

	_, err = util.WriteIfChanged(filepath.Join(subsys.Memory, name, "memory.memsw.limit_in_bytes"), []byte(strconv.Itoa(hardLimit)+"\n"), 0600)
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

func CreatePodCgroup(cgroupName string, config Config, subsystemer Subsystemer) error {
	subsys, err := subsystemer.Find()
	if err != nil {
		return err
	}
	node, err := os.Hostname()
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Join(subsys.CPU, "p2", node), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}
	err = os.MkdirAll(filepath.Join(subsys.Memory, "p2", node), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}
	return create(cgroupName, "", config, subsystemer)
}

func CreateLaunchableCgroup(cgroupName string, config Config, subsystemer Subsystemer) error {
	return create(cgroupName, "", config, subsystemer)
}

func create(cgroupName string, podID string, config Config, subsystemer Subsystemer) error {
	config.Name = cgroupName

	var subsys Subsystems
	var err error
	if podID != "" {
		subsys, err = FindWithParentGroup(podID, subsystemer)
	} else {
		subsys, err = subsystemer.Find()
	}

	if err != nil {
		return util.Errorf("Could not find cgroupfs mount point: %s", err)
	}

	err = subsys.Write(config)
	if _, ok := err.(UnsupportedError); ok {
		return util.Errorf("Unsupported subsystem: %s", err)
	} else if err != nil {
		return util.Errorf("Could not set cgroup parameters: %s", err)
	}
	return subsys.AddPID(config.Name, 0)
}

func GetPodConfig(configPath string) (Config, error) {
	if config := os.Getenv(configPath); config != "" {
		confBuf, err := ioutil.ReadFile(config)
		if err != nil {
			return Config{}, err
		}
		cgMap := make(map[string]Config)
		err = yaml.Unmarshal(confBuf, cgMap)
		if err != nil {
			return Config{}, err
		}
		if _, ok := cgMap["cgroup"]; !ok {
			return Config{}, util.Errorf("%s does not contain cgroup parameters\n", configPath)
		}
		cgConfig := cgMap["cgroup"]
		return cgConfig, nil
	} else {
		return Config{}, util.Errorf("No %s found in environment", configPath)
	}
}

func GetLaunchableConfig(configPath string, configKey string) (Config, error) {
	if config := os.Getenv(configPath); config != "" {
		confBuf, err := ioutil.ReadFile(config)
		if err != nil {
			return Config{}, err
		}
		cgMap := make(map[string]map[string]Config)
		err = yaml.Unmarshal(confBuf, cgMap)
		if err != nil {
			return Config{}, err
		}

		if _, ok := cgMap[configKey]; !ok {
			return Config{}, util.Errorf("Key %q not found in %s", configKey, configPath)
		}
		if _, ok := cgMap[configKey]["cgroup"]; !ok {
			return Config{}, util.Errorf("Key %q in %s does not contain cgroup parameters\n", configKey, configPath)
		}
		cgConfig := cgMap[configKey]["cgroup"]
		return cgConfig, nil
	} else {
		return Config{}, util.Errorf("No %s found in environment", configPath)
	}
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
