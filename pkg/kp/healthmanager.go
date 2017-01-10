package kp

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/util/limit"
	"github.com/square/p2/pkg/util/param"
	"github.com/square/p2/pkg/util/stream"
)

var (
	// HealthRetryTimeSec determines how long to wait between retries when a health check
	// fails to write.
	HealthRetryTimeSec = param.Int("health_retry_time_sec", 5)

	// SessionTTLSec sets the TTL time for each session created by consulHealthManager. This
	// parameter controls how long it takes for clients to notice that health checks have
	// stopped.
	SessionTTLSec = param.Int("health_session_ttl_sec", 15)

	// HealthWritesPerMinute sets the average number of writes per minute per service that
	// will be sent to Consul to update health.
	HealthWritesPerMinute = param.Int("health_writes_per_minute", 4)

	// HealthMaxBucketSize sets the maximum token bucket size per service used to
	// rate-limit Consul writes.
	HealthMaxBucketSize = param.Int64("health_max_bucket_size", 16)

	// HealthResumeLimit sets the lower bound on the number of tokens at which updates will
	// be resumed. If a service runs out of tokens, its last update will be to set the
	// health status to "unknown" with an error message, and further updates will be
	// throttled until enough tokens have been accumulated.
	HealthResumeLimit = param.Int64("health_resume_limit", 4)
)

// consulHealthManager maintains a Consul session for all the local node's health checks,
// renews it periodically, and refreshes all health checks if it expires.
type consulHealthManager struct {
	sessionPub *stream.StringValuePublisher // Publishes the current session
	done       chan<- struct{}              // Close this to stop reporting health
	client     consulutil.ConsulClient      // Connection to the Consul agent
	node       store.NodeName
	logger     logging.Logger // Logger for health events
	wg         sync.WaitGroup
}

// NewHealthManager implements the Store interface. It creates a new HealthManager that
// uses the Consul Key-Value store to hold app health statues.
func (c consulStore) newSessionHealthManager(
	node store.NodeName,
	logger logging.Logger,
) HealthManager {
	done := make(chan struct{})
	m := &consulHealthManager{
		done:   done,
		client: c.client,
		node:   node,
		logger: logger,
	}

	// Create a stream of sessions
	sessionChan := make(chan string)
	// Current time of "Jan 2, 15:04:05" turns into "0102-150405"
	timeStr := time.Now().Format("0102-150405")
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		consulutil.SessionManager(
			api.SessionEntry{
				Name:      fmt.Sprintf("health:%s:%d:%s", node, os.Getpid(), timeStr),
				LockDelay: 1 * time.Millisecond,
				Behavior:  api.SessionBehaviorDelete,
				TTL:       fmt.Sprintf("%ds", *SessionTTLSec),
			},
			c.client,
			sessionChan,
			done,
			logger,
		)
	}()

	// Hook the session stream into a publisher
	m.sessionPub = stream.NewStringValuePublisher(sessionChan, "")

	return m
}

// Close cleans up the HealthManager. New health reports will not be published, and
// existing reports will be removed. Implements the HealthManager interface.
func (m *consulHealthManager) Close() {
	close(m.done)
	m.wg.Wait()
}

// consulHealthUpdater holds the state needed for the update process to track the current
// service health and which health it has published.
type consulHealthUpdater struct {
	node    store.NodeName   // The node this updater is bound to
	pod     store.PodID      // The pod ID this updater is bound to
	service string           // The service this updater is bound to
	checker chan WatchResult // Stream of health updates from a checker
}

// NewUpdater creates a new HealthUpdater that can be used to update an app's health status
// on this node.
func (m *consulHealthManager) NewUpdater(pod store.PodID, service string) HealthUpdater {
	checksStream := make(chan WatchResult)
	u := &consulHealthUpdater{
		node:    m.node,
		pod:     pod,
		service: service,
		checker: checksStream,
	}
	// Don't increment m.wg for this goroutine: the returned HealthUpdater is closed
	// separately from the HealthManager, and we don't want closing the HealthManager to
	// be dependent on its updaters being closed first.
	go func() {
		sub := m.sessionPub.Subscribe()
		defer sub.Unsubscribe()
		processHealthUpdater(
			m.client,
			checksStream,
			sub.Chan(),
			m.logger.SubLogger(logrus.Fields{
				"service": service,
				"pod":     pod,
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
	Health   *WatchResult // The health that was just written
	OK       bool         // Whether the write succeeded
	Throttle bool         // Whether to turn on the write throttle on success
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
	client consulutil.ConsulClient,
	checksStream <-chan WatchResult,
	sessionsStream <-chan string,
	logger logging.Logger,
) {
	var localHealth *WatchResult  // Health last reported by checker
	var remoteHealth *WatchResult // Health last written to Consul
	var session string            // Current session

	var write <-chan writeResult  // Future result of an in-flight write
	var throttle <-chan time.Time // If set, writes are throttled

	// Track and limit all writes to avoid crushing Consul
	bucketRefreshRate := time.Minute / time.Duration(*HealthWritesPerMinute)
	rateLimiter, err := limit.NewTokenBucket(
		*HealthMaxBucketSize,
		*HealthMaxBucketSize,
		bucketRefreshRate,
	)
	if err != nil {
		panic("invalid token bucket parameters")
	}

	logger.NoFields().Debug("starting update loop")
	for {
		// Receive event notification; update internal FSM state
		select {
		case h, ok := <-checksStream:
			// The local health checker sent a new result
			if ok {
				logger.NoFields().Debug("new health status: ", h.Status)
				if !healthEquiv(localHealth, &h) {
					msg := fmt.Sprintf("Service %s is now %s", h.Service, h.Status)
					if health.Passing.Is(h.Status) {
						logger.NoFields().Infoln(msg)
					} else {
						logger.NoFields().Warnln(msg)
					}
				}
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
				if result.Throttle && throttle == nil {
					throttle = time.After(time.Duration(*HealthResumeLimit) * bucketRefreshRate)
					logger.NoFields().Warningf("Service %s health is flapping; throttling updates", result.Health.Service)
				}
			}
		case <-throttle:
			throttle = nil
			logger.NoFields().Warning("health is stable; resuming updates")
		}

		// Exit
		if checksStream == nil && remoteHealth == nil && write == nil {
			logger.NoFields().Debug("exiting update loop")
			return
		}

		// Send update to Consul
		if !healthEquiv(localHealth, remoteHealth) && session != "" && write == nil &&
			throttle == nil {
			writeLogger := logger.SubLogger(logrus.Fields{
				"session": session,
			})
			w := make(chan writeResult, 1)
			if localHealth == nil {
				// Don't wait on the rate limiter when removing the health status
				rateLimiter.TryUse(1)
				logger.NoFields().Debug("deleting remote health")
				key := HealthPath(remoteHealth.Service, remoteHealth.Node)
				go sendHealthUpdate(writeLogger, w, nil, false, func() error {
					_, err := client.KV().Delete(key, nil)
					if err != nil {
						return consulutil.NewKVError("delete", key, err)
					}
					return nil
				})
			} else {
				writeHealth := localHealth
				doThrottle := false
				if count, _ := rateLimiter.TryUse(1); count <= 1 {
					// This is the last update before the throttle will be engaged. Write a special
					// message.
					logger.NoFields().Debug("writing throttled health")
					writeHealth = toThrottled(localHealth)
					doThrottle = true
				} else {
					logger.NoFields().Debug("writing remote health")
				}
				kv, err := healthToKV(*writeHealth, session)
				if err != nil {
					// Practically, this should never happen.
					logger.WithErrorAndFields(err, logrus.Fields{
						"health": *writeHealth,
					}).Error("could not serialize health update")
					localHealth = nil
					continue
				}
				if remoteHealth == nil {
					go sendHealthUpdate(writeLogger, w, localHealth, doThrottle, func() error {
						ok, _, err := client.KV().Acquire(kv, nil)
						if err != nil {
							return consulutil.NewKVError("acquire", kv.Key, err)
						}
						if !ok {
							return fmt.Errorf("write denied")
						}
						return nil
					})
				} else {
					go sendHealthUpdate(writeLogger, w, localHealth, doThrottle, func() error {
						_, err := client.KV().Put(kv, nil)
						if err != nil {
							return consulutil.NewKVError("put", kv.Key, err)
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

func toThrottled(wr *WatchResult) *WatchResult {
	return &WatchResult{
		Node:    wr.Node,
		Id:      wr.Id,
		Service: wr.Service,
		Status:  string(health.Unknown),
	}
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
	doThrottle bool,
	sender func() error,
) {
	if err := sender(); err != nil {
		logger.WithError(err).Error("error writing health")
		// Try not to overwhelm Consul
		time.Sleep(time.Duration(*HealthRetryTimeSec) * time.Second)
		w <- writeResult{nil, false, doThrottle}
	} else {
		w <- writeResult{health, true, doThrottle}
	}
}
