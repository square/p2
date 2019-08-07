package logging

import (
	"io/ioutil"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/sirupsen/logrus"
)

// RecordingLogger keeps a record of all the entries it's asked to write.
type RecordingLogger struct {
	Logger
	Entries []*logrus.Entry
}

func NewRecordingLogger(fields logrus.Fields) *RecordingLogger {
	l := NewLogger(fields)
	l.SetLogOut(ioutil.Discard)
	rl := &RecordingLogger{l, make([]*logrus.Entry, 0, 5)}
	l.Logger.Formatter = rl
	return rl
}

func (rl *RecordingLogger) Format(entry *logrus.Entry) ([]byte, error) {
	rl.Entries = append(rl.Entries, entry)
	return []byte{}, nil
}

func TestProcessCounterIncrementsEveryTime(t *testing.T) {
	logger := NewRecordingLogger(nil)
	c := counter
	Assert(t).AreEqual(c+0, counter, "unexpected counter value")

	logger.Info("First message")
	Assert(t).AreEqual(c+0, logger.Entries[0].Data["Counter"], "wrong counter value")
	Assert(t).AreEqual(c+1, counter, "unexpected counter value")

	logger.Info("Second message")
	Assert(t).AreEqual(c+1, logger.Entries[1].Data["Counter"], "wrong counter value")
	Assert(t).AreEqual(c+2, counter, "unexpected counter value")

	logger.Info("Third message")
	Assert(t).AreEqual(c+2, logger.Entries[2].Data["Counter"], "wrong counter value")
	Assert(t).AreEqual(c+3, counter, "unexpected counter value")
}
