package cgroups

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util/size"
)

type FakeSubsystemer struct {
	tmpdir string
}

func (fs *FakeSubsystemer) Find() (Subsystems, error) {
	var err error
	if fs.tmpdir == "" {
		if fs.createTmpdir(); err != nil {
			return Subsystems{}, err
		}
	}

	if err = os.Chmod(fs.tmpdir, os.ModePerm); err != nil {
		return Subsystems{}, err
	}
	return Subsystems{CPU: filepath.Join(fs.tmpdir, "cpu"), Memory: filepath.Join(fs.tmpdir, "memory")}, nil
}

// createTmpDir is useful only for tests, it creates the tmpdir
func (fs *FakeSubsystemer) createTmpdir() error {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}
	fs.tmpdir = tmpdir
	return nil
}

func (fs *FakeSubsystemer) cleanupTmpdir() error {
	return os.RemoveAll(fs.tmpdir)
}

func TestCreatePodCgroup(t *testing.T) {
	c := Config{CPUs: 2, Memory: size.ByteCount(1024)}
	podID := types.PodID("podID")
	hostname := types.NodeName("abc123.example")
	fs := &FakeSubsystemer{}
	defer fs.cleanupTmpdir()
	if err := CreatePodCgroup(podID, hostname, c, fs); err != nil {
		t.Fatalf("err: %v", err)
	}

	expectCgroupFileToContain(t, "2048\n", filepath.Join(fs.tmpdir, "memory", "p2", hostname.String(), podID.String(), "memory.limit_in_bytes"))
	expectCgroupFileToContain(t, "2048\n", filepath.Join(fs.tmpdir, "memory", "p2", hostname.String(), podID.String(), "memory.memsw.limit_in_bytes"))
	expectCgroupFileToContain(t, "1024\n", filepath.Join(fs.tmpdir, "memory", "p2", hostname.String(), podID.String(), "memory.soft_limit_in_bytes"))
}

func expectCgroupFileToContain(t *testing.T, s string, file string) {
	actual, err := ioutil.ReadFile(filepath.Join(file))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(actual) != s {
		t.Errorf("expected %s, but got: %s", s, string(actual))
	}
}
