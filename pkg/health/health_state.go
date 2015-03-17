package health

import (
	"fmt"
)

type HealthState string

var (
	Passing  = HealthState("passing")
	Unknown  = HealthState("unknown")
	Warning  = HealthState("warning")
	Critical = HealthState("critical")

	NoStatusGiven = fmt.Errorf("No status given")
)

func ToHealthState(v string) HealthState {
	if v == "passing" {
		return Passing
	}
	if v == "unknown" {
		return Unknown
	}
	if v == "warning" {
		return Warning
	}
	if v == "critical" {
		return Critical
	}
	return Unknown
}

// Compare two HealthStates. Return 0 if equal, -1 if a<b and +1 if a>b.
// The ordering is Passing>Warning>Unknown>Critical.
func Compare(a, b HealthState) int {
	if a == b {
		return 0
	}

	if a == Passing {
		return 1
	}
	if b == Passing {
		return -1
	}

	if a == Warning {
		return 1
	}
	if b == Warning {
		return -1
	}

	if a == Unknown {
		return 1
	}
	if b == Unknown {
		return -1
	}

	return 0
}
