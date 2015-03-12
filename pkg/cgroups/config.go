package cgroups

import (
	"fmt"
)

type Config struct {
	Name   string `yaml:"-"`                // The name of the cgroup in cgroupfs
	CPUs   int    `yaml:"cpus,omitempty"`   // The number of logical CPUs
	Memory int    `yaml:"memory,omitempty"` // The number of bytes of memory
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
