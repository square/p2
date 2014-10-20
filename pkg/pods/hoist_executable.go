package pods

import (
	"github.com/square/p2/pkg/runit"
)

type HoistExecutable struct {
	runit.Service
	execPath string
}
