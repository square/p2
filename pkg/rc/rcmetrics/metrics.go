package rcmetrics

import (
	"time"

	"github.com/square/p2/pkg/logging"
)

const (
	// Represents the amount of time it takes to process an RC list update
	// in the farm. Specifically, this means spinning off a goroutine for
	// each unclaimed RC received from a watch.
	RCProcessingTimeMetric = "rc_processing_time"
)

type Metrics interface {
	RecordRCProcessingTime(processingTime time.Duration)
}

func NewLoggingMetrics(logger logging.Logger) Metrics {
	return &defaultMetrics{
		Logger: logger,
	}
}

type defaultMetrics struct {
	Logger logging.Logger
}

func (m *defaultMetrics) RecordRCProcessingTime(processingTime time.Duration) {
	m.Logger.WithField(RCProcessingTimeMetric, processingTime.String()).Infoln()
}
