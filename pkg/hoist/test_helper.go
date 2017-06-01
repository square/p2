package hoist

import (
	"io/ioutil"
	"os"
	"os/user"
	"runtime"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
)

func FakeHoistLaunchableForDirUUIDPod(dirName string) (*Launchable, *runit.ServiceBuilder) {
	return fakeHoistLaunchableForDir(dirName, true)
}

func FakeHoistLaunchableForDirLegacyPod(dirName string) (*Launchable, *runit.ServiceBuilder) {
	return fakeHoistLaunchableForDir(dirName, false)
}

func fakeHoistLaunchableForDir(dirName string, isUUIDPod bool) (*Launchable, *runit.ServiceBuilder) {
	tempDir, _ := ioutil.TempDir("", "fakeenv")
	launchableInstallDir := util.From(runtime.Caller(0)).ExpandPath(dirName)

	launchable := &Launchable{
		Id:        "testLaunchable",
		ServiceId: "testPod__testLaunchable",
		RunAs:     "testPod",
		PodEnvDir: tempDir,
		RootDir:   launchableInstallDir,
		P2Exec:    util.From(runtime.Caller(0)).ExpandPath("fake_p2-exec"),
		Version:   "abc123",
		EntryPoints: EntryPoints{
			Paths:    []string{"bin/launch"},
			Implicit: true,
		},
		IsUUIDPod: isUUIDPod,
	}

	curUser, err := user.Current()
	if err == nil {
		launchable.RunAs = curUser.Username
	}

	sbTemp, _ := ioutil.TempDir("", "fakesvdir")
	sb := &runit.ServiceBuilder{
		RunitRoot: sbTemp,
	}

	executables, _ := launchable.Executables(sb)
	for _, exe := range executables {
		_ = os.MkdirAll(exe.Service.Path, 0644)
	}

	return launchable, sb
}

func CleanupFakeLaunchable(h *Launchable, s *runit.ServiceBuilder) {
	if os.TempDir() != h.PodEnvDir {
		_ = os.RemoveAll(h.PodEnvDir)
	}
	if os.TempDir() != s.RunitRoot {
		_ = os.RemoveAll(s.RunitRoot)
	}
}
