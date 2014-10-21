package logger

import (
	"github.com/Sirupsen/logrus"
)

type P2Logger struct {
	host   string
	topic  string
	logger logrus.Logger
}
