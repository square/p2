package error_reporter

// Level allows the Reporter to distinguish errors and warnings.
type Level int

// Values for the Level type
const (
	LevelError Level = iota
	LevelWarning
	NumLevels int = iota
)

// Context provides additional information about an error.
type Context struct {
	Level Level

	Tags map[string]string
}

// Reporter provides an interface for reporting errors to an exception service
// such as Sentry or Airbrake. No concrete implementations are provided but
// code calling into certain libraries can pass an implementation of this
// interface to have errors collected
type Reporter interface {
	// Report an error. Context may be nil. Depth is the number of stack frames to skip
	// when reporting stack traces. An implementation should assume LevelError if
	// context is nil
	Report(err error, context *Context, depth int)
}

type nopReporter struct{}

func (nopReporter) Report(_ error, _ *Context, _ int) {}

func NewNop() Reporter {
	return &nopReporter{}
}
