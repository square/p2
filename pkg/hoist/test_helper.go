package hoist

import (
	"io/ioutil"
	"os"
	"os/user"
	"runtime"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

func FakeHoistLaunchableForDir(dirName string) (*Launchable, *runit.ServiceBuilder) {
	tempDir, _ := ioutil.TempDir("", "fakeenv")
	launchableInstallDir := util.From(runtime.Caller(0)).ExpandPath(dirName)

	launchable := &Launchable{
		Location:  "testLaunchable.tar.gz",
		Id:        "testPod__testLaunchable",
		RunAs:     "testPod",
		ConfigDir: tempDir,
		Fetcher_:  uri.DefaultFetcher,
		RootDir:   launchableInstallDir,
		P2exec:    util.From(runtime.Caller(0)).ExpandPath("fake_p2-exec"),
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
		os.MkdirAll(exe.Service.Path, 0644)
	}

	return launchable, sb
}

func CleanupFakeLaunchable(h *Launchable, s *runit.ServiceBuilder) {
	if os.TempDir() != h.ConfigDir {
		os.RemoveAll(h.ConfigDir)
	}
	if os.TempDir() != s.RunitRoot {
		os.RemoveAll(s.RunitRoot)
	}
}
