package kp

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util/param"
	"github.com/square/p2/pkg/util/stream"
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

var (
	// HealthRetryTimeSec determines how long to wait between retries when a health check
	// fails to write.
	HealthRetryTimeSec = param.Int("health_retry_time_sec", 5)

	// SessionTTLSec sets the TTL time for each session created by consulHealthManager. This
	// parameter controls how long it takes for clients to notice that health checks have
	// stopped.
	SessionTTLSec = param.Int("health_session_ttl_sec", 15)
)

// consulHealthManager maintains a Consul session for all the local node's health checks,
// renews it periodically, and refreshes all health checks if it expires.
type consulHealthManager struct {
	sessionPub *stream.StringValuePublisher // Publishes the current session
	done       chan struct{}                // Close this to stop reporting health
	client     *api.Client                  // Connection to the Consul agent
	node       string
	logger     logging.Logger // Logger for health events
}

// NewHealthManager implements the Store interface. It creates a new HealthManager that
// uses the Consul Key-Value store to hold app health statues.
func (c consulStore) newSessionHealthManager(
	node string,
	logger logging.Logger,
) HealthManager {
	// Create a stream of sessions
	sessionChan := make(chan string)
	done := make(chan struct{})
	// Current time of "Jan 2, 15:04:05" turns into "0102-150405"
	timeStr := time.Now().Format("0102-150405")
	go ConsulSessionManager(
		api.SessionEntry{
			Name:      fmt.Sprintf("health:%s:%d:%s", node, os.Getpid(), timeStr),
			LockDelay: 1 * time.Nanosecond,
			Behavior:  api.SessionBehaviorDelete,
			TTL:       fmt.Sprintf("%ds", *SessionTTLSec),
		},
		c.client,
		sessionChan,
		done,
		logger,
	)

	// Hook the session stream into a publisher
	pub := stream.NewStringValuePublisher(sessionChan, "")

	return &consulHealthManager{
		sessionPub: pub,
		done:       done,
		client:     c.client,
		node:       node,
		logger:     logger,
	}
}

// Close cleans up the HealthManager. New health reports will not be published, and
// existing reports will be removed. Implements the HealthManager interface.
func (m *consulHealthManager) Close() {
	close(m.done)
}

// consulHealthUpdater holds the state needed for the update process to track the current
// service health and which health it has published.
type consulHealthUpdater struct {
	node    string           // The node this updater is bound to
	pod     string           // The pod ID this updater is bound to
	service string           // The service this updater is bound to
	checker chan WatchResult // Stream of health updates from a checker
}

// NewUpdater creates a new HealthUpdater that can be used to update an app's health status
// on this node.
func (m *consulHealthManager) NewUpdater(pod, service string) HealthUpdater {
	checksStream := make(chan WatchResult)
	u := &consulHealthUpdater{
		node:    m.node,
		pod:     pod,
		service: service,
		checker: checksStream,
	}
	go func() {
		sub := m.sessionPub.Subscribe(nil)
		defer sub.Unsubscribe()
		processHealthUpdater(
			m.client,
			checksStream,
			sub.Chan(),
			m.logger.SubLogger(logrus.Fields{
				"service": service,
				"node":    m.node,
			}),
		)
	}()
	return u
}

func (u *consulHealthUpdater) PutHealth(health WatchResult) error {
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
	u.checker <- health
	return nil
}

func (u *consulHealthUpdater) Close() {
	close(u.checker)
}

type writeResult struct {
	Health *WatchResult // The health that was just written
	OK     bool         // Whether the write succeeded
}

// processHealthUpdater() runs in a goroutine to keep Consul in sync with the local health
// state. It is written as a non-blocking finite state machine: events arrive and update
// internal state, and after each event, the internal state is examined to see if an
// asynchronous action needs to be taken.
//
// Events come from three different sources:
//   1. App monitors send their periodic health check results here. When the service is no
//      longer being checked, the monitor must close this channel.
//   2. The session manager sends notifications whenever the current Consul session
//      expires or is renewed. When the manager exits, it must close this channel.
//   3. Writes to Consul are performed in a separate goroutine, and when each finishes, it
//      notifies the updater of what it just wrote.
//
// In response to these events, two actions can be taken:
//   A. Exit, once the app monitor has exited and the health check in Consul has been
//      removed.
//   B. Write the recent service state to Consul. At most one outstanding write will be
//      in-flight at any time.
func processHealthUpdater(
	client *api.Client,
	checksStream <-chan WatchResult,
	sessionsStream <-chan string,
	logger logging.Logger,
) {
	var localHealth *WatchResult  // Health last reported by checker
	var remoteHealth *WatchResult // Health last written to Consul
	var session string            // Current session

	var write <-chan writeResult // Future result of an in-flight write

	logger.NoFields().Debug("starting update loop")
	for {
		// Receive event notification; update internal FSM state
		select {
		case h, ok := <-checksStream:
			// The local health checker sent a new result
			if ok {
				logger.NoFields().Debug("new health status: ", h.Status)
				localHealth = &h
			} else {
				logger.NoFields().Debug("check stream closed")
				checksStream = nil
				localHealth = nil
			}
		case s, ok := <-sessionsStream:
			// The active Consul session changed
			if ok {
				logger.NoFields().Debug("new session: ", s)
			} else {
				logger.NoFields().Debug("session stream closed")
				sessionsStream = nil
			}
			session = s
			// The old health result is deleted when its session expires
			remoteHealth = nil
		case result := <-write:
			// The in-flight write completed
			logger.NoFields().Debug("write completed: ", result.OK)
			write = nil
			if result.OK {
				remoteHealth = result.Health
			}
		}

		// Exit
		if checksStream == nil && remoteHealth == nil && write == nil {
			logger.NoFields().Debug("exiting update loop")
			return
		}

		// Send update to Consul
		if !healthEquiv(localHealth, remoteHealth) && session != "" && write == nil {
			writeLogger := logger.SubLogger(logrus.Fields{
				"session": session,
			})
			w := make(chan writeResult, 1)
			if localHealth == nil {
				logger.NoFields().Debug("deleting remote health")
				key := HealthPath(remoteHealth.Service, remoteHealth.Node)
				go sendHealthUpdate(writeLogger, w, nil, func() error {
					_, err := client.KV().Delete(key, nil)
					if err != nil {
						return NewKVError("delete", key, err)
					}
					return nil
				})
			} else {
				logger.NoFields().Debug("writing remote health")
				kv, err := healthToKV(*localHealth, session)
				if err != nil {
					// Practically, this should never happen.
					logger.WithErrorAndFields(err, logrus.Fields{
						"health": *localHealth,
					}).Error("could not serialize health update")
					localHealth = nil
					continue
				}
				if remoteHealth == nil {
					go sendHealthUpdate(writeLogger, w, localHealth, func() error {
						ok, _, err := client.KV().Acquire(kv, nil)
						if err != nil {
							return NewKVError("acquire", kv.Key, err)
						}
						if !ok {
							return fmt.Errorf("write denied")
						}
						return nil
					})
				} else {
					go sendHealthUpdate(writeLogger, w, localHealth, func() error {
						_, err := client.KV().Put(kv, nil)
						if err != nil {
							return NewKVError("put", kv.Key, err)
						}
						return nil
					})
				}
			}
			write = w
		}
	}
}

// Helper to processHealthUpdater()
func healthEquiv(x *WatchResult, y *WatchResult) bool {
	return x == nil && y == nil ||
		x != nil && y != nil && x.Status == y.Status
}

// Helper to processHealthUpdater()
func healthToKV(wr WatchResult, session string) (*api.KVPair, error) {
	now := time.Now()
	wr.Time = now
	// This health check only expires when the key is removed
	wr.Expires = now.Add(100 * 365 * 24 * time.Hour)
	data, err := json.Marshal(wr)
	if err != nil {
		return nil, err
	}
	return &api.KVPair{
		Key:     HealthPath(wr.Service, wr.Node),
		Value:   data,
		Session: session,
	}, nil
}

// Helper to processHealthUpdater()
func sendHealthUpdate(
	logger logging.Logger,
	w chan<- writeResult,
	health *WatchResult,
	sender func() error,
) {
	if err := sender(); err != nil {
		logger.WithError(err).Error("error writing health")
		// Try not to overwhelm Consul
		time.Sleep(time.Duration(*HealthRetryTimeSec) * time.Second)
		w <- writeResult{nil, false}
	} else {
		w <- writeResult{health, true}
	}
}
