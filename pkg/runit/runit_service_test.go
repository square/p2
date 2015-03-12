package runit

import (
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestRunitServicesCanBeStarted(t *testing.T) {
	sv := FakeSV()
	service := &Service{"/var/service/foo", "foo"}
	out, err := sv.Start(service)
	Assert(t).IsNil(err, "There should not have been an error starting the service")
	Assert(t).AreEqual(out, "start /var/service/foo\n", "Did not start service with correct arguments")
}

func TestErrorReturnedIfRunitServiceBails(t *testing.T) {
	sv := ErringSV()
	service := &Service{"/var/service/foo", "foo"}
	_, err := sv.Start(service)
	Assert(t).IsNotNil(err, "There should have been an error starting the service")
}
