package hoist

import (
	"io/ioutil"
	"os"
	"os/user"
	"runtime"

	"github.com/square/p2/pkg/util"
)

type FakeCurl struct {
	url     string
	outPath string
}

func (fc *FakeCurl) File(url string, outPath string) error {
	fc.url = url
	fc.outPath = outPath

	return DefaultFetcher()(url, outPath)
}

func FakeHoistLaunchableForDir(dirName string) *Launchable {
	tempDir, _ := ioutil.TempDir("", "fakeenv")
	launchableInstallDir := util.From(runtime.Caller(0)).ExpandPath(dirName)

	launchable := &Launchable{
		Location:    "testLaunchable.tar.gz",
		Id:          "testPod__testLaunchable",
		RunAs:       "testPod",
		ConfigDir:   tempDir,
		FetchToFile: new(FakeCurl).File,
		RootDir:     launchableInstallDir,
		P2exec:      util.From(runtime.Caller(0)).ExpandPath("fake_p2-exec"),
	}

	curUser, err := user.Current()
	if err == nil {
		launchable.RunAs = curUser.Username
	}

	return launchable
}

func cleanupFakeLaunchable(h *Launchable) {
	if os.TempDir() != h.ConfigDir {
		os.RemoveAll(h.ConfigDir)
	}
}
