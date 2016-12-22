// This package contains code used by p2-schedule that is useful
// to import elsewhere, e.g. the structure of its output.
package schedule

import (
	"github.com/square/p2/pkg/store"
)

// Defines the JSON structure of the output of p2-schedule
type Output struct {
	PodID        store.PodID        `json:"pod_id"`
	PodUniqueKey store.PodUniqueKey `json:"pod_unique_key"`
}
