package store

import (
	"sync"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/types"
)

// HealthStore can answer questions about the health of a particular pod on a node
// It performs this by watching the health tree of consul and caching the result
type HealthStore interface {
	StartWatch(quitCh <-chan struct{})
	Fetch(types.PodID, string) *health.Result
}

type healthStore struct {
	cachedHealth  map[cacheKey]*health.Result
	healthChecker checker.ConsulHealthChecker
	lock          sync.RWMutex
	logger        logging.Logger
}

var _ HealthStore = &healthStore{}

func NewHealthStore(healthChecker checker.ConsulHealthChecker) HealthStore {
	return &healthStore{
		healthChecker: healthChecker,
		lock:          sync.RWMutex{},
		logger:        logging.DefaultLogger,
	}
}

func (hs *healthStore) StartWatch(quitCh <-chan struct{}) {
	healthUpdates := make(chan []*health.Result, 1)
	defer close(healthUpdates)
	errCh := make(chan error)
	defer close(errCh)

	hs.healthChecker.WatchHealth(healthUpdates, errCh, quitCh)

	select {
	case updates := <-healthUpdates:
		hs.cache(updates)
	case <-quitCh:
		hs.logger.Errorln("Quitting...")
		return
	case err := <-errCh:
		hs.logger.WithError(err).Errorln("Consul Watch error")
	}
}

func (hs *healthStore) cache(results []*health.Result) {
	tmpCache := make(map[cacheKey]*health.Result, len(results))

	// duplicate key is undefined behavior
	for _, result := range results {
		result := result
		tmpCache[newCacheKey(result.ID, types.NodeName(result.Node))] = result
	}

	hs.lock.Lock()
	defer hs.lock.Unlock()

	hs.cachedHealth = tmpCache
}

func (hs *healthStore) Fetch(podID types.PodID, node string) *health.Result {
	cacheKey := newCacheKey(podID, types.NodeName(node))

	hs.lock.RLock()
	defer hs.lock.RUnlock()

	res, ok := hs.cachedHealth[cacheKey]
	if !ok {
		hs.logger.WithFields(logrus.Fields{"cachedHealth": len(hs.cachedHealth)}).Errorln("Cache miss!!")
		return nil
	}
	return res
}

type cacheKey struct {
	pod  types.PodID
	node types.NodeName
}

func newCacheKey(podID types.PodID, node types.NodeName) cacheKey {
	return cacheKey{pod: podID, node: node}
}
