package pp

import (
	"fmt"
	"github.com/armon/consul-api" // TODO: Lock to branch
	"os"
	"time"
)

type RealityClient struct {
	consul *consulapi.Client
}

type Lease struct {
	lock    string
	checkId string
	session string
}

func NewRealityClient() (*RealityClient, error) {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())

	if err != nil {
		return nil, err
	}

	x := RealityClient{
		consul: client,
	}
	return &x, nil
}

// Blocks until sufficient number of clients are healthy that it would be safe
// to take one out of the cluster.
func (c *RealityClient) AcquireShutdownLease(name string, minNodes int) *Lease {
	pid := os.Getpid()
	checkId := fmt.Sprintf("%s-shutdown-lease-%d", name, pid)

	// Make a check against liveliness of this process, so that if it crashes the
	// lock will be released.
	agent := c.consul.Agent()
	checkRegistration := consulapi.AgentCheckRegistration{
		Name: checkId,
		AgentServiceCheck: consulapi.AgentServiceCheck{
			// TODO: Check start time of process is before now, to guard against PID
			// reuse.
			Script:   fmt.Sprintf("kill -0 %d", pid),
			Interval: "3s",
		},
	}

	if err := agent.CheckRegister(&checkRegistration); err != nil {
		// TODO: Return err
		return nil
	}

	// Register a session using the check that was just created.
	session := c.consul.Session()
	// TODO: Does LockDelay make sense here?
	se := consulapi.SessionEntry{
		Checks: []string{"serfHealth", checkId},
	}
	sessionId, _, _ := session.Create(&se, nil)
	// TODO: Return err

	// Get a lock
	// TODO: Use a shared-semaphore instead (see ZK Curator) so that more than
	// one client can acquire a lease at the same time.
	lockKey := "locks/" + name + "/shutdown"
	kv := c.consul.KV()

	lockData := consulapi.KVPair{
		Session: sessionId,
		Key:     lockKey,
	}

	locked, _, _ := kv.Acquire(&lockData, nil)
	// TODO: Return err

	if locked {
		// TODO: Check sufficient number of nodes are healthy.
		/*
		   healthyNodes := c.CountHealthy(name)
		   // Add one since we are about to shut down a node
		   if healthyNodes > minNodes + 1 {
		   }
		*/

		ret := Lease{
			lock:    lockKey,
			session: sessionId,
			checkId: checkId,
		}
		return &ret
	} else {
		// TODO block instead
		return nil
	}
}

func (c *RealityClient) LocalHealthy(serviceName string) (bool, error) {
	data, err := c.consul.Agent().Self()

	if err != nil {
		return false, err
	}

	localName := data["Member"]["Name"].(string)

	checks, _, err := c.consul.Health().Node(localName, nil)

	if err != nil {
		return false, err
	}

	for _, check := range checks {
		if check.ServiceID == serviceName {
			return check.Status == "passing", nil
		}
	}

	// A service with only the default serfhealth check is always considered
	// passing.
	return true, nil
}

func (c *RealityClient) RegisterService(name string, checkCmd string) error {
	// TODO: How to do this for reals?
	service := consulapi.AgentServiceRegistration{
		Name: name,
	}
	check := consulapi.AgentServiceCheck{
		Script:   checkCmd,
		Interval: "3s",
	}

	service.Check = &check

	c.consul.Agent().ServiceDeregister(name)
	return c.consul.Agent().ServiceRegister(&service)
}

// TODO: Add timeout
func (c *RealityClient) WaitForLocalUnhealthy(name string) error {
	for {
		healthy, err := c.LocalHealthy(name)
		if err != nil {
			return err
		}

		if !healthy {
			break
		}

		// TODO: Long poll instead
		time.Sleep(300 * time.Millisecond)
	}
	return nil
}

// TODO: Add timeout
func (c *RealityClient) WaitForLocalHealthy(name string) error {
	for {
		healthy, err := c.LocalHealthy(name)
		if err != nil {
			return err
		}

		if healthy {
			break
		}

		// TODO: Long poll instead
		time.Sleep(300 * time.Millisecond)
	}
	return nil
}

// Best effort to release a lease
// TODO: Still return errors
func (c *RealityClient) ReleaseLease(l *Lease) error {
	pair := consulapi.KVPair{
		Key:     l.lock,
		Session: l.session,
	}
	c.consul.KV().Release(&pair, nil)

	c.consul.Session().Destroy(l.session, nil)

	c.consul.Agent().CheckDeregister(l.checkId)

	return nil
}
