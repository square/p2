package constants

import (
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util/param"
)

const (
	PreparerPodID = types.PodID("p2-preparer")
)

// Maximum allowed time for a single health check, in seconds
var HEALTHCHECK_TIMEOUT = param.Int64("healthcheck_timeout", 5)
