package runit

import (
	"io/ioutil"
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

// testServiceBuilder is a ServiceBuilder for use in unit tests.
type testServiceBuilder struct {
	root string
	ServiceBuilder
}

// Cleanup removes the file system changes made by the testServiceBuilder.
func (s testServiceBuilder) Cleanup() {
	os.RemoveAll(s.root)
}

// mustMkdirAll creates the given directory or dies trying
func mustMkdirAll(path string) {
	if err := os.MkdirAll(path, 0755); err != nil {
		panic(err)
	}
}

// FakeServiceBuilder constructs a testServiceBuilder for use in unit tests. It is the
// caller's responsibility to always call Cleanup() on the return value to ensure that
// file system changes are removed when this test ends.
func FakeServiceBuilder() (s *testServiceBuilder) {
	root, err := ioutil.TempDir("", "runit_test")
	if err != nil {
		panic(err)
	}
	defer func() {
		// If the method exits abnormally, try to clean up the file system.
		if s == nil {
			os.RemoveAll(root)
		}
	}()
	config := filepath.Join(root, "config")
	mustMkdirAll(config)
	staging := filepath.Join(root, "staging")
	mustMkdirAll(staging)
	install := filepath.Join(root, "service")
	mustMkdirAll(install)

	bin := util.From(runtime.Caller(0)).ExpandPath("fake_servicebuilder")

	return &testServiceBuilder{
		root: root,
		ServiceBuilder: ServiceBuilder{
			ConfigRoot:     config,
			StagingRoot:    staging,
			RunitRoot:      install,
			Bin:            bin,
			testingNoChown: true,
		},
	}
}
