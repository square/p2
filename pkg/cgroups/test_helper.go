package cgroups

import (
	"runtime"

	"github.com/square/p2/pkg/util"
)

func FakeCgexec() string {
	return util.From(runtime.Caller(0)).ExpandPath("fake_cgexec")
}
