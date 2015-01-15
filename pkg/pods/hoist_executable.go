package pods

import (
	"github.com/square/p2/pkg/runit"
)

type HoistExecutable struct {
	runit.Service
	ExecPath string
}

func (e HoistExecutable) Execute(env []string) (string, error) {
	return "", nil
}
