package logging

import (
	"fmt"
	"net"

	"github.com/Sirupsen/logrus"
)

type SocketHook struct {
	socketPath string
}

func (SocketHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

func (s SocketHook) Fire(entry *logrus.Entry) error {
	c, err := net.Dial("unix", s.socketPath)
	if err != nil {
		// Airbrake someday
		fmt.Println("Unable to dial socket:", err)
		return nil
	}
	defer c.Close()

	logMessage, err := entry.Logger.Formatter.Format(entry)
	if err != nil {
		// Airbrake someday
		return nil
	}
	_, err = c.Write(logMessage)
	if err != nil {
		// Airbrake someday
		fmt.Println("Unable to write to socket:", err)
	}
	return nil
}
