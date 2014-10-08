// Yes there are a million libraries that do logging, but for 50 lines of code
// I get things just the way I like them.

package logger

import (
	"fmt"
	"io"
	"os"
	"time"
)

const (
	NONE = iota
	FATAL
	WARN
	INFO
	DEBUG
)

var (
	logLevel = INFO
	out      = DefaultOut()
)

func DefaultOut() io.Writer {
	return io.Writer(os.Stdout)
}

func Fatal(msg string, args ...interface{}) { LogAt(FATAL, "FATAL", msg, args...) }
func Warn(msg string, args ...interface{})  { LogAt(WARN, "WARN", msg, args...) }
func Info(msg string, args ...interface{})  { LogAt(INFO, "INFO", msg, args...) }
func Debug(msg string, args ...interface{}) { LogAt(DEBUG, "DEBUG", msg, args...) }

func SetOut(writer io.Writer) {
	out = writer
}

func SetLogLevel(level string) {
	switch level {
	case "DEBUG":
		logLevel = DEBUG
	case "INFO":
		logLevel = INFO
	case "WARN":
		logLevel = WARN
	case "FATAL":
		logLevel = FATAL
	case "NONE":
		logLevel = NONE
	}
}

func LogAt(level int, tag string, msg string, args ...interface{}) {
	if logLevel < level {
		return
	}

	Log(tag, msg, args...)
}

func Log(tag string, msg string, args ...interface{}) {
	now := time.Now().UTC()

	fmt.Fprintf(out, "%-5s [%s] %s\n", tag,
		now.Format("2006-01-02 15:04:05.999"),
		fmt.Sprintf(msg, args...))
}
