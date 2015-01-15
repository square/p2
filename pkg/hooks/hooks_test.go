package hooks

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
)

func TestExecutableHooksAreRun(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hook")
	Assert(t).IsNil(err, "the error should have been nil")
	defer os.RemoveAll(tempDir)

	podDir, err := ioutil.TempDir("", "pod")
	defer os.RemoveAll(podDir)
	Assert(t).IsNil(err, "the error should have been nil")

	ioutil.WriteFile(path.Join(tempDir, "test1"), []byte("#!/bin/sh\necho $POD_ID > $(dirname $0)/output"), 0755)

	podId := "TestPod"
	manifest := pods.PodManifest{Id: podId}
	hooks := Hooks(os.TempDir(), &logging.DefaultLogger)
	hooks.runHooks(tempDir, pods.NewPod(podId, podDir), &manifest)

	contents, err := ioutil.ReadFile(path.Join(tempDir, "output"))
	Assert(t).IsNil(err, "the error should have been nil")

	Assert(t).AreEqual(string(contents), "TestPod\n", "hook should output pod ID into output file")
}

func TestNonExecutableHooksAreNotRun(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hook")
	Assert(t).IsNil(err, "the error should have been nil")
	defer os.RemoveAll(tempDir)

	podDir, err := ioutil.TempDir("", "pod")
	defer os.RemoveAll(podDir)
	Assert(t).IsNil(err, "the error should have been nil")

	err = ioutil.WriteFile(path.Join(tempDir, "test2"), []byte("#!/bin/sh\ntouch $(dirname $0)/failed"), 0644)
	Assert(t).IsNil(err, "the error should have been nil")

	podId := "TestPod"
	manifest := pods.PodManifest{Id: podId}
	hooks := Hooks(os.TempDir(), &logging.DefaultLogger)
	hooks.runHooks(tempDir, pods.NewPod(podId, podDir), &manifest)

	if _, err := os.Stat(path.Join(tempDir, "failed")); err == nil {
		t.Fatal("`failed` file exists; non-executable hook ran but should not have run")
	}
}

func TestDirectoriesDoNotBreakEverything(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hook")
	Assert(t).IsNil(err, "the error should have been nil")
	defer os.RemoveAll(tempDir)

	podDir, err := ioutil.TempDir("", "pod")
	defer os.RemoveAll(podDir)
	Assert(t).IsNil(err, "the error should have been nil")

	Assert(t).IsNil(os.Mkdir(path.Join(tempDir, "mydir"), 0755), "Should not have erred")

	podId := "TestPod"
	manifest := pods.PodManifest{Id: podId}
	pod := pods.NewPod(podId, podDir)
	logger := logging.TestLogger()
	hooks := Hooks(os.TempDir(), &logger)
	err = hooks.runHooks(tempDir, pod, &manifest)

	Assert(t).IsNil(err, "Got an error when running a directory inside the hooks directory")
}
