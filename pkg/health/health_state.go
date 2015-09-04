package health

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

// Compare two HealthStates. Return 0 if equal, a value less than 0 if a < b and a value
// greater than 0 if a > b. The ordering is Passing > Warning > Unknown > Critical.
func Compare(a, b HealthState) int {
	return a.Int() - b.Int()
}
