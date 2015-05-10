package runit

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/anthonybishopric/gotcha"
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
