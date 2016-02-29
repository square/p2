package store

import (
	"time"
)

type HealthState string

// String representations of the canonical health states
var (
	Critical = HealthState("critical")
	Unknown  = HealthState("unknown")
	Warning  = HealthState("warning")
	Passing  = HealthState("passing")
)

// Integer enum representations of the canonical health states. These are not guaranteed
// to be consistent across versions. Only externalize the strings! The enum value is used
// to order HealthStates.
const (
	criticalInt = iota
	unknownInt
	warningInt
	passingInt
)

// ToHealthState converts a string to its corresponding HealthState value. Unrecognized
// values become Unknown.
func ToHealthState(str string) HealthState {
	switch s := HealthState(str); s {
	case Critical, Unknown, Warning, Passing:
		return s
	default:
		return Unknown
	}
}

// Int converts a HealthState to an enum representation suitable for comparisons.
func (s HealthState) Int() int {
	switch s {
	case Critical:
		return criticalInt
	case Unknown:
		return unknownInt
	case Warning:
		return warningInt
	case Passing:
		return passingInt
	default:
		return criticalInt
	}
}

// Returns whether the given string matches a particular health state
func (s HealthState) Is(state string) bool {
	return ToHealthState(state) == s
}

// Compare two HealthStates. Return 0 if equal, a value less than 0 if a < b and a value
// greater than 0 if a > b. The ordering is Passing > Warning > Unknown > Critical.
func Compare(a, b HealthState) int {
	return a.Int() - b.Int()
}

// HealthCheck defines the data structure used to record health checks. The non-standard
// JSON names are for legacy compatibility.
type HealthCheck struct {
	Node    string      `json:"Node"`
	Pod     PodID       `json:"Id"`
	Service PodID       `json:"Service"` // DEPRECATED: should always = Id
	Status  HealthState `json:"Status"`
	Output  string      `json:"Output"`
	Time    time.Time   `json:"Time"`
	Expires time.Time   `json:"Expires,omitempty"`
}

// SortOrder sorts the nodes in the list from least to most health.
type SortOrder struct {
	Nodes  []string
	Health map[string]*HealthCheck
}

func (s SortOrder) Len() int {
	return len(s.Nodes)
}

func (s SortOrder) Swap(i, j int) {
	s.Nodes[i], s.Nodes[j] = s.Nodes[j], s.Nodes[i]
}

func (s SortOrder) Less(i, j int) bool {
	iHealth := Unknown
	if res, ok := s.Health[s.Nodes[i]]; ok {
		iHealth = res.Status
	}

	jHealth := Unknown
	if res, ok := s.Health[s.Nodes[j]]; ok {
		jHealth = res.Status
	}

	return Compare(iHealth, jHealth) < 0
}
