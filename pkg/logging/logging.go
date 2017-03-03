// The logging package is a wrapper around github.com/Sirupsen/logrus that provides some
// convenience methods and improved error reporting.
package logging

import (
	"io"
	"os"

	"github.com/Sirupsen/logrus"

	"github.com/square/p2/pkg/util"
)

// processCounter is a singleton instance of the counter, so that all messages can be
// sequentially ordered and lost messages can be detected.
var processCounter ProcessCounter

// Logger is a thin wrapper around a logrus.Entry that lets us extend and override some of
// Entry's methods.
type Logger struct {
	*logrus.Entry
}

type OutType string

// Recognized output hook types
const (
	OutSocket = OutType("socket")
)

var DefaultLogger = NewLogger(logrus.Fields{})

func NewLogger(baseFields logrus.Fields) Logger {
	logger := logrus.New()
	logger.Formatter = new(logrus.TextFormatter)
	logger.Hooks.Add(&processCounter)
	return Logger{logrus.NewEntry(logger).WithFields(baseFields)}
}

func (l Logger) SetLogOut(out io.Writer) {
	l.Logger.Out = out
}

func (l Logger) AddHook(outType OutType, dest string) error {
	switch outType {
	case OutSocket:
		l.Logger.Hooks.Add(SocketHook{socketPath: dest})
	default:
		return util.Errorf("Unsupported log output type: %s", outType)
	}
	return nil
}

func (l Logger) SubLogger(fields logrus.Fields) Logger {
	return l.WithFields(fields)
}

func (l Logger) NoFields() Logger {
	return l
}

func (l Logger) WithError(err error) Logger {
	fields := logrus.Fields{
		"err": err.Error(),
	}
	// if err conforms to the util.CallsiteError interface, add some fields to
	// the log message that can be used for exception reporting by a buddy
	// launchable (e.g. to bugsnag or airbrake)
	if stackErr, ok := err.(util.CallsiteError); ok {
		fields["line_number"] = stackErr.LineNumber()
		fields["filename"] = stackErr.Filename()
		fields["function"] = stackErr.Function()
	}
	return l.WithFields(fields)
}

func (l Logger) WithFields(fields logrus.Fields) Logger {
	return Logger{l.Entry.WithFields(fields)}
}

func (l Logger) WithErrorAndFields(err error, fields logrus.Fields) Logger {
	return l.WithError(err).WithFields(fields)
}

func (l Logger) WithField(key string, value interface{}) Logger {
	return Logger{l.Entry.WithField(key, value)}
}

func TestLogger() Logger {
	logger := NewLogger(logrus.Fields{})
	logger.Logger.Out = os.Stdout
	logger.Logger.Formatter = &logrus.TextFormatter{}
	logger.Logger.Level = logrus.DebugLevel
	return logger
}
