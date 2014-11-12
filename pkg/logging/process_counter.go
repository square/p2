package logging

import (
	"os"

	"github.com/Sirupsen/logrus"
)

type ProcessCounter struct {
	counter int
}

func (p *ProcessCounter) Fields() logrus.Fields {
	fields := logrus.Fields{
		"Counter": p.counter,
		"PID":     os.Getpid(),
	}
	p.counter++
	return fields
}
