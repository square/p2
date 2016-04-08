package rcmetrics

import (
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/github.com/rcrowley/go-metrics"
)

const (
	// Represents the amount of time it takes to process an RC list update
	// in the farm. Specifically, this means spinning off a goroutine for
	// each unclaimed RC received from a watch.
	RCProcessingTimeMetric = "rc_processing_time"
)

// Subset of metrics.Registry interface
type MetricsRegistry interface {
	Get(metricName string) interface{}
	Register(metricName string, metric interface{}) error
}

// Test that default registry implements this interface
var _ MetricsRegistry = metrics.DefaultRegistry

func NewMetrics(logger logging.Logger) *Metrics {
	return &Metrics{
		Logger: logger,
	}
}

type Metrics struct {
	Logger   logging.Logger
	Registry MetricsRegistry
}

func (m *Metrics) SetMetricsRegistry(registry MetricsRegistry) error {
	m.Registry = registry
	return m.Registry.Register(RCProcessingTimeMetric, metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)))
}

func (m *Metrics) RecordRCProcessingTime(processingTime time.Duration) {
	if m.Registry == nil {
		// No registry was set, just log the metric
		m.Logger.WithField(RCProcessingTimeMetric, processingTime.String()).Infoln()
		return
	}

	metric := m.Registry.Get(RCProcessingTimeMetric)
	if metric == nil {
		err := util.Errorf("No %s metric set on metrics registry", RCProcessingTimeMetric)
		m.Logger.WithError(err).Errorln("Unable to send metric")
		return
	}

	histogram, ok := metric.(metrics.Histogram)
	if !ok {
		err := util.Errorf("%s metric was not a metrics.Histogram", RCProcessingTimeMetric)
		m.Logger.WithError(err).Errorln("Unable to send metric")
	}

	histogram.Update(int64(processingTime))
}
