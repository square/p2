package hooks

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
)

const podId = "TestPod"

func TestExecutableHooksAreRun(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hook")
	Assert(t).IsNil(err, "the error should have been nil")
	defer os.RemoveAll(tempDir)

	podDir, err := ioutil.TempDir("", "pod")
	defer os.RemoveAll(podDir)
	Assert(t).IsNil(err, "the error should have been nil")

	ioutil.WriteFile(path.Join(tempDir, "test1"), []byte("#!/bin/sh\necho $HOOKED_POD_ID > $(dirname $0)/output"), 0755)

	hooks := Hooks(os.TempDir(), &logging.DefaultLogger)
	hooks.runHooks(tempDir, AFTER_INSTALL, pods.NewPod(podId, podDir), testManifest(), logging.DefaultLogger)

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

	hooks := Hooks(os.TempDir(), &logging.DefaultLogger)
	hooks.runHooks(tempDir, AFTER_INSTALL, pods.NewPod(podId, podDir), testManifest(), logging.DefaultLogger)

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

	pod := pods.NewPod(podId, podDir)
	logger := logging.TestLogger()
	hooks := Hooks(os.TempDir(), &logger)
	err = hooks.runHooks(tempDir, AFTER_INSTALL, pod, testManifest(), logging.DefaultLogger)

	Assert(t).IsNil(err, "Got an error when running a directory inside the hooks directory")
}

func testManifest() pods.Manifest {
	builder := pods.NewManifestBuilder()
	builder.SetID(podId)
	return builder.GetManifest()
}
