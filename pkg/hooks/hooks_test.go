package hooks

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
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

func testManifest() manifest.Manifest {
	builder := manifest.NewManifestBuilder()
	builder.SetID(podId)
	return builder.GetManifest()
}

func TestHookRunWithTimeout(t *testing.T) {
	timeout := 1 * time.Millisecond
	sleep := "1" // 1 second sleep to be executed by the script

	// build an executable file to feed to Hook
	contents := []byte("#!/bin/bash\nsleep " + sleep)

	tmpFile, err := tempFileWithContents("test-hook-run-with-timeout.", contents)
	if err != nil {
		t.Error(err.Error())
	}
	defer os.Remove(tmpFile)

	hook := Hook{tmpFile, "timeout-test-hook", timeout, []string{}, logging.TestLogger()}

	toErr := hook.RunWithTimeout()
	if _, ok := toErr.(HookTimeoutError); !ok {
		// we either had no error or a different error
		t.Errorf("timeout did not throw a HookTimeoutError: timeout: %#v / sleep: %#v / err: %#v", timeout, sleep, toErr)
	}
}

// tempFileWithContents creates a tempfile (0744), fills it with contents and returns the path to it
//
// You are expected to delete the file afterwards
func tempFileWithContents(name string, contents []byte) (string, error) {
	tmpfile, err := ioutil.TempFile(".", name)
	if err != nil {
		return "", err
	}

	os.Chmod(tmpfile.Name(), 0744)

	tmpfile.Write([]byte(contents))
	tmpfile.Close()

	path, _ := filepath.Abs(tmpfile.Name())

	return path, nil
}
