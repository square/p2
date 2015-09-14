package runit

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
)

func TestRunitServicesCanBeStarted(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "runit_service")
	os.MkdirAll(filepath.Join(tmpdir, "supervise"), 0644)

	Assert(t).IsNil(err, "test setup should have created a tmpdir")

	defer os.RemoveAll(tmpdir)

	sv := FakeSV()
	service := &Service{tmpdir, "foo"}
	out, err := sv.Start(service)
	Assert(t).IsNil(err, "There should not have been an error starting the service")
	Assert(t).AreEqual(out, fmt.Sprintf("start %s\n", service.Path), "Did not start service with correct arguments")
}

func TestErrorReturnedIfRunitServiceBails(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "runit_service")
	os.MkdirAll(filepath.Join(tmpdir, "supervise"), 0644)
	Assert(t).IsNil(err, "test setup should have created a tmpdir")

	defer os.RemoveAll(tmpdir)

	sv := ErringSV()
	service := &Service{tmpdir, "foo"}
	_, err = sv.Start(service)
	Assert(t).IsNotNil(err, "There should have been an error starting the service")
}

func TestOutToStatResultCorrectlyParsesPIDs(t *testing.T) {
	statRes, err := outToStatResult("run: /var/service/hoist__artifact-cleanup3/: (pid 22807) 2599s; run: log: (pid 1748) 8269291s")
	Assert(t).IsNil(err, "should not have failed to parse stat output")

	Assert(t).AreEqual(uint64(22807), statRes.ChildPID, "Should have found the correct child PID")
	Assert(t).AreEqual(2599*time.Second, statRes.ChildUptime, "Should have found the correct child PID uptime")
	Assert(t).AreEqual(uint64(1748), statRes.LogPID, "Should have found the correct log PID")
	Assert(t).AreEqual(8269291*time.Second, statRes.LogUptime, "Should have found the correct log PID uptime")
}
