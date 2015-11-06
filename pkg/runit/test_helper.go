package runit

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/square/p2/pkg/util"
)

func FakeSV() SV {
	return &sv{util.From(runtime.Caller(0)).ExpandPath("fake_sv")}
}

func ErringSV() SV {
	return &sv{util.From(runtime.Caller(0)).ExpandPath("erring_sv")}
}

func NewRecordingSV() SV {
	return &RecordingSV{}
}

type RecordingSV struct {
	LastCommand string
}

func (r *RecordingSV) setLastCommand(command string) (string, error) {
	if r.LastCommand != "" {
		return "", util.Errorf("This recording SV has already been issued a command, it must be cleared first")
	}
	r.LastCommand = command
	return "success", nil
}

func (r *RecordingSV) Start(service *Service) (string, error) {
	return r.setLastCommand("start")
}
func (r *RecordingSV) Stop(service *Service, timeout time.Duration) (string, error) {
	return r.setLastCommand("stop")
}
func (r *RecordingSV) Stat(service *Service) (*StatResult, error) {
	_, err := r.setLastCommand("stat")
	return nil, err
}

func (r *RecordingSV) Restart(service *Service, timeout time.Duration) (string, error) {
	return r.setLastCommand("restart")
}

func (r *RecordingSV) Once(service *Service) (string, error) {
	return r.setLastCommand("once")
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
