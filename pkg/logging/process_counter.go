package logging

import (
	"os"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

var counter uint64

// ProcessCounter is a Logrus hook that appends a sequence number to all entries. This
// hook should appear before other hooks that externalize data, to ensure that they see
// the added fields.
type ProcessCounter struct{}

func (ProcessCounter) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

func (p *ProcessCounter) Fire(entry *logrus.Entry) error {
	data := make(logrus.Fields, len(entry.Data)+2)
	for k, v := range entry.Data {
		data[k] = v
	}
	data["Counter"] = atomic.AddUint64(&counter, 1) - 1
	data["PID"] = os.Getpid()
	entry.Data = data
	return nil
}
