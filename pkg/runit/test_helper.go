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
	return &RecordingSV{Commands: make([]string, 0)}
}

type RecordingSV struct {
	Commands []string
}

func (r *RecordingSV) LastCommand() string {
	return r.Commands[len(r.Commands)-1]
}

func (r *RecordingSV) recordCommand(command string) (string, error) {
	r.Commands = append(r.Commands, command)
	return "success", nil
}

func (r *RecordingSV) Start(service *Service) (string, error) {
	return r.recordCommand("start")
}
func (r *RecordingSV) Stop(service *Service, timeout time.Duration) (string, error) {
	return r.recordCommand("stop")
}
func (r *RecordingSV) Stat(service *Service) (*StatResult, error) {
	_, err := r.recordCommand("stat")
	return nil, err
}
func (r *RecordingSV) Restart(service *Service, timeout time.Duration) (string, error) {
	return r.recordCommand("restart")
}
func (r *RecordingSV) Once(service *Service) (string, error) {
	return r.recordCommand("once")
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
	_ = os.RemoveAll(s.root)
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
			_ = os.RemoveAll(root)
		}
	}()
	config := filepath.Join(root, "config")
	mustMkdirAll(config)
	staging := filepath.Join(root, "staging")
	mustMkdirAll(staging)
	install := filepath.Join(root, "service")
	mustMkdirAll(install)

	return &testServiceBuilder{
		root: root,
		ServiceBuilder: ServiceBuilder{
			ConfigRoot:     config,
			StagingRoot:    staging,
			RunitRoot:      install,
			testingNoChown: true,
		},
	}
}
