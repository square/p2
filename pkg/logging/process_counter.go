package logging

import (
	"os"
	"sync/atomic"

	"github.com/Sirupsen/logrus"
)

// ProcessCounter is a Logrus hook that appends a sequence number to all entries. This
// hook should appear before other hooks that externalize data, to ensure that they see
// the added fields.
type ProcessCounter struct {
	counter uint64
}

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
	data["Counter"] = atomic.AddUint64(&p.counter, 1) - 1
	data["PID"] = os.Getpid()
	entry.Data = data
	return nil
}
