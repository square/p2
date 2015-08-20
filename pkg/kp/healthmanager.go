package kp

import (
	"fmt"
	"time"

	"github.com/square/p2/pkg/logging"
)

type consulSimpleHealthManager struct {
	store  consulStore
	node   string
	logger logging.Logger
}

type consulSimpleHealthUpdater struct {
	node       string
	pod        string
	service    string
	store      consulStore
	logger     logging.Logger
	lastCheck  time.Time
	lastStatus string
}

// newSimpleHealthManager creates a simple health manager that writes a service's health
// status to Consul every 15 seconds or whenever its status changes.
func (c consulStore) newSimpleHealthManager(
	node string,
	logger logging.Logger,
) HealthManager {
	return &consulSimpleHealthManager{
		store:  c,
		node:   node,
		logger: logger,
	}
}

// NewUpdater implements the HealthManager interface.
func (m *consulSimpleHealthManager) NewUpdater(pod, service string) HealthUpdater {
	return &consulSimpleHealthUpdater{
		node:    m.node,
		pod:     pod,
		service: service,
		store:   m.store,
		logger:  m.logger,
	}
}

// Close implements the HealthManager interface. consulSimpleHealthManager has no
// resources to release.
func (m *consulSimpleHealthManager) Close() {}

// PutHealth implements the HealthUpdater interface.
func (u *consulSimpleHealthUpdater) PutHealth(health WatchResult) error {
	if health.Node != u.node || health.Id != u.pod || health.Service != u.service {
		return fmt.Errorf(
			"this updater is bound to %s/%s/%s and cannot update %s/%s/%s",
			u.node,
			u.pod,
			u.service,
			health.Node,
			health.Id,
			health.Service,
		)
	}
	if health.Status != u.lastStatus || time.Since(u.lastCheck) > TTL/4 {
		var err error
		u.lastCheck, _, err = u.store.PutHealth(health)
		u.lastStatus = health.Status
		if err != nil {
			u.logger.WithError(err).Warning("error writing health to Consul")
		}
	}
	return nil
}

// Close implements the HealthUpdater interface. consulSimpleHealthUpdater has no
// resources to release.
func (u *consulSimpleHealthUpdater) Close() {}
