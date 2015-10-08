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

// Used as argument to launchableWithFields to make it easy for tests to only
// specify the fields they care about when creating a hoist.Launchable
type LaunchableFields struct {
	ID    string
	RunAs string
}

func FakeHoistLaunchableForDir(dirName string, modifyFields LaunchableFields) (*Launchable, *runit.ServiceBuilder) {
	tempDir, _ := ioutil.TempDir("", "fakeenv")
	launchableInstallDir := util.From(runtime.Caller(0)).ExpandPath(dirName)

	// Prefer the passed RunAs, otherwise make one up
	runAs := modifyFields.RunAs
	if runAs == "" {
		runAs = "testPod"
	}

	launchable := &Launchable{
		location:  "testLaunchable.tar.gz",
		id:        "testPod__testLaunchable",
		runAs:     runAs,
		configDir: tempDir,
		fetcher:   uri.DefaultFetcher,
		rootDir:   launchableInstallDir,
		p2exec:    util.From(runtime.Caller(0)).ExpandPath("fake_p2-exec"),
	}

	curUser, err := user.Current()
	if err == nil {
		launchable.runAs = curUser.Username
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
	if os.TempDir() != h.configDir {
		os.RemoveAll(h.configDir)
	}
	if os.TempDir() != s.RunitRoot {
		os.RemoveAll(s.RunitRoot)
	}
}
