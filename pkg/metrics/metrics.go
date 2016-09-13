package metrics

import (
	"net/http"
	"sync"

	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
)

// Registry is a global metrics registry
var Registry metrics.Registry

// ExpHandler is an http handler that will publish the contents of its composed registry as JSON
var ExpHandler http.Handler

var m sync.Mutex

// Those who import this package get a default metrics.Registry
func init() {
	Registry = metrics.NewRegistry()
	if ExpHandler == nil {
		ExpHandler = exp.ExpHandler(Registry)
	}
}

func SetMetricsRegistry(registry metrics.Registry) {
	m.Lock()
	defer m.Unlock()
	Registry = registry
	ExpHandler = exp.ExpHandler(Registry)
}
