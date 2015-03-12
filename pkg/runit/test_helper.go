package runit

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/square/p2/pkg/util"
)

func FakeSV() *SV {
	return &SV{util.From(runtime.Caller(0)).ExpandPath("fake_sv")}
}

func ErringSV() *SV {
	return &SV{util.From(runtime.Caller(0)).ExpandPath("erring_sv")}
}

func FakeChpst() string {
	return util.From(runtime.Caller(0)).ExpandPath("fake_chpst")
}

func FakeServiceBuilder() *ServiceBuilder {
	testDir := os.TempDir()
	fakeSBBinPath := util.From(runtime.Caller(0)).ExpandPath("fake_servicebuilder")
	configRoot := filepath.Join(testDir, "/etc/servicebuilder.d")
	os.MkdirAll(configRoot, 0755)
	_, err := os.Stat(configRoot)
	if err != nil {
		panic("unable to create test dir")
	}
	stagingRoot := filepath.Join(testDir, "/var/service-stage")
	os.MkdirAll(stagingRoot, 0755)
	runitRoot := filepath.Join(testDir, "/var/service")
	os.MkdirAll(runitRoot, 0755)

	return &ServiceBuilder{
		ConfigRoot:  configRoot,
		StagingRoot: stagingRoot,
		RunitRoot:   runitRoot,
		Bin:         fakeSBBinPath,
	}
}
