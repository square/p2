package cgroups

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

type FakeSubsystemer struct {
	tmpdir string
}

func (fs *FakeSubsystemer) Find() (Subsystems, error) {
	var err error
	if fs.tmpdir == "" {
		fs.tmpdir, err = ioutil.TempDir("", "")
		if err != nil {
			return Subsystems{}, err
		}

		if err = os.Chmod(fs.tmpdir, os.ModePerm); err != nil {
			return Subsystems{}, err
		}
	}
	return Subsystems{CPU: filepath.Join(fs.tmpdir, "cpu"), Memory: filepath.Join(fs.tmpdir, "memory")}, nil
}

func TestCreatePodCgroup(t *testing.T) {

	// Name   string         `yaml:"-"`                // The name of the cgroup in cgroupfs
	// CPUs   int            `yaml:"cpus,omitempty"`   // The number of logical CPUs
	// Memory size.ByteCount `yaml:"memory,omitempty"` // The number of bytes of memory

	c := Config{
		"cgroup-path",
		2,
		1024,
	}
	if err := CreatePodCgroup("pod", c, &FakeSubsystemer{}); err != nil {
		t.Fatalf("err: %v", err)
	}
}
