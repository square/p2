package pods

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestExecutableHooksAreRun(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hook")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	ioutil.WriteFile(path.Join(tempDir, "test1"), []byte("#!/bin/sh\necho $POD_ID > $(dirname $0)/output"), 0755)

	manifest := PodManifest{Id: "TestPod"}
	RunHooks(tempDir, &manifest)

	contents, err := ioutil.ReadFile(path.Join(tempDir, "output"))
	if err != nil {
		panic(err)
	}

	Assert(t).AreEqual(string(contents), "TestPod\n", "hook should output pod ID into output file")
}

func TestNonExecutableHooksAreNotRun(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hook")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	ioutil.WriteFile(path.Join(tempDir, "test2"), []byte("#!/bin/sh\ntouch $(dirname $0)/failed"), 0644)

	manifest := PodManifest{Id: "TestPod"}
	RunHooks(tempDir, &manifest)

	if _, err := os.Stat(path.Join(tempDir, "failed")); err == nil {
		panic("`failed` file exists; non-executable hook ran but should not have run")
	}
}

func TestDirectoriesDoNotBreakEverything(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hook")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	os.Mkdir(path.Join(tempDir, "mydir"), 0755)

	manifest := PodManifest{Id: "TestPod"}
	err = RunHooks(tempDir, &manifest)

	Assert(t).IsNil(err, "Got an error when running a directory inside the hooks directory")
}
