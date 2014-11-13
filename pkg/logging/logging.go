package logging

import (
	"io"

	"github.com/Sirupsen/logrus"
)

// the process counter increments for every sent message over logrus. If
// messages are missed, the process counter can be inspected and missed
// messages will present gaps in the counter.

var processCounter ProcessCounter

type Logger struct {
	Logger     *logrus.Logger
	baseFields logrus.Fields
}

var DefaultLogger Logger

func init() {
	DefaultLogger = NewLogger(logrus.Fields{})
}

func NewLogger(baseFields logrus.Fields) Logger {
	logger := Logger{logrus.New(), baseFields}
	logger.Logger.Formatter = new(logrus.JSONFormatter)
	return logger
}

func (l *Logger) SetLogOut(out io.Writer) {
	l.Logger.Out = out
}

func (l *Logger) SubLogger(fields logrus.Fields) Logger {
	return Logger{l.Logger, Merge(l.baseFields, fields)}
}

func (l *Logger) WithFields(fields logrus.Fields) *logrus.Entry {
	return l.Logger.WithFields(Merge(Merge(l.baseFields, fields), processCounter.Fields()))
}

func Merge(template, additional logrus.Fields) logrus.Fields {
	combined := logrus.Fields{}
	for key, value := range template {
		combined[key] = value
	}
	for key, value := range additional {
		combined[key] = value
	}
	return combined
}
