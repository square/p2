package cgroups

import (
	"github.com/square/p2/pkg/util/size"
)

// Config combines an ID and subsystem limits
type Config struct {
	Name   CgroupID       `yaml:"-"`                // The name of the cgroup in cgroupfs
	CPUs   int            `yaml:"cpus,omitempty"`   // The number of logical CPUs
	Memory size.ByteCount `yaml:"memory,omitempty"` // The number of bytes of memory
}
