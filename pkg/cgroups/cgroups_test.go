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
	c := Config{
		"cgroup-path",
		2,
		1024,
	}
	fs := &FakeSubsystemer{}
	if err := CreatePodCgroup("pod", c, fs); err != nil {
		t.Fatalf("err: %v", err)
	}

	expectCgroupFileToContain(t, "2048\n", filepath.Join(fs.tmpdir, "memory", "pod", "memory.limit_in_bytes"))
	expectCgroupFileToContain(t, "2048\n", filepath.Join(fs.tmpdir, "memory", "pod", "memory.memsw.limit_in_bytes"))
	expectCgroupFileToContain(t, "1024\n", filepath.Join(fs.tmpdir, "memory", "pod", "memory.soft_limit_in_bytes"))
}

func expectCgroupFileToContain(t *testing.T, s string, file string) {
	actual, err := ioutil.ReadFile(filepath.Join(file))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(actual) != s { // We set the hard limit to 2x the request
		t.Errorf("expected %s, but got: %s", s, string(actual))
	}
}
