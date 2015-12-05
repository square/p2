package cgroups

import (
	"fmt"

	"github.com/square/p2/pkg/util/size"
)

type Config struct {
	Name   string `yaml:"-"`                // The name of the cgroup in cgroupfs
	CPUs   int    `yaml:"cpus,omitempty"`   // The number of logical CPUs
	Memory string `yaml:"memory,omitempty"` // The number of bytes of memory
}

func NewCGroup(cpus int, memory size.ByteCount) Config {
	return Config{
		CPUs:   cpus,
		Memory: memory.String(),
	}
}

func (config Config) MemoryByteCount() (size.ByteCount, error) {
	if config.Memory == "" {
		return size.Byte * 0, nil
	}
	return size.Parse(config.Memory)
}

func (config Config) CgexecArgs() []string {
	return []string{
		"-g",
		fmt.Sprintf("memory:%s", config.Name),
		"-g",
		fmt.Sprintf("cpu:%s", config.Name),
		"--sticky",
	}
}
