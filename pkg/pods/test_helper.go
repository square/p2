package pods

import (
	"os"
	"path"
	"runtime"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
)

type FakeCurl struct {
	url     string
	outPath string
}

func (fc *FakeCurl) File(url string, outPath string, args ...interface{}) error {
	fc.url = url
	fc.outPath = outPath

	return DefaultFetcher()(url, outPath, args...)
}

func FakeChpst() string {
	return util.From(runtime.Caller(0)).ExpandPath("fake_chpst")
}

func FakeHoistLaunchableForDir(dirName string) *HoistLaunchable {
	tempDir := os.TempDir()
	_, filename, _, _ := runtime.Caller(0)
	launchableInstallDir := path.Join(path.Dir(filename), dirName)

	launchable := &HoistLaunchable{
		Location:    "testLaunchable.tar.gz",
		Id:          "testPod__testLaunchable",
		RunAs:       "testPod",
		ConfigDir:   tempDir,
		FetchToFile: new(FakeCurl).File,
		RootDir:     launchableInstallDir,
		Chpst:       FakeChpst(),
	}
	return launchable
}

func FakeServiceBuilder() *runit.ServiceBuilder {
	testDir := os.TempDir()
	fakeSBBinPath := util.From(runtime.Caller(0)).ExpandPath("fake_servicebuilder")
	configRoot := path.Join(testDir, "/etc/servicebuilder.d")
	os.MkdirAll(configRoot, 0755)
	_, err := os.Stat(configRoot)
	if err != nil {
		panic("unable to create test dir")
	}
	stagingRoot := path.Join(testDir, "/var/service-stage")
	os.MkdirAll(stagingRoot, 0755)
	runitRoot := path.Join(testDir, "/var/service")
	os.MkdirAll(runitRoot, 0755)

	return &runit.ServiceBuilder{
		ConfigRoot:  configRoot,
		StagingRoot: stagingRoot,
		RunitRoot:   runitRoot,
		Bin:         fakeSBBinPath,
	}
}
