package rcmetrics

import (
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/rcrowley/go-metrics"

	"github.com/square/p2/pkg/logging"
)

const (
	// Represents the amount of time it takes to process an RC list update
	// in the farm. Specifically, this means spinning off a goroutine for
	// each unclaimed RC received from a watch.
	RCProcessingTimeMetric = "rc_processing_time"
)

type Metrics struct {
	Registry metrics.Registry
	Logger   logging.Logger
}

func (m *Metrics) SetRegistry(registry metrics.Registry) error {
	m.Registry = registry
	return m.Registry.Register(RCProcessingTimeMetric, metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)))
}

func (m *Metrics) RecordRCProcessingTime(processingTime time.Duration) {
	if m.Registry == nil {
		m.Logger.Infof("Not logging %s metric because no metric registry is set", RCProcessingTimeMetric)
		return
	}

	histogram, ok := m.Registry.Get(RCProcessingTimeMetric).(metrics.Histogram)
	if !ok {
		m.Logger.Errorln("Not logging %s metric because the metric of that name was not a Histogram type.", RCProcessingTimeMetric)
		return
	}

	histogram.Update(int64(processingTime))
}
