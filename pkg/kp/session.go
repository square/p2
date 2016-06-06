package kp

import (
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/util"
)

// Represents a session that can be used to lock keys in the KV store. The only
// current implementation wraps consul sessions, which can be used to obtain
// locks on multiple keys, and must be periodically renewed.
type Session interface {
	Lock(key string) (consulutil.Unlocker, error)
	Renew() error
	Destroy() error
}

const (
	lockTTL         = "15s"
	renewalInterval = 10 * time.Second

	// Consul's minimum lock delay is 1ms. Requesting a lock delay of 0 will be
	// interpreted as "use the default," which is 15s at this time.
	lockDelay = 1 * time.Millisecond
)

func (c consulStore) NewSession(name string, renewalCh <-chan time.Time) (Session, chan error, error) {
	session, _, err := c.client.Session().CreateNoChecks(&api.SessionEntry{
		Name:      name,
		LockDelay: lockDelay,
		// locks should only be used with ephemeral keys
		Behavior: api.SessionBehaviorDelete,
		TTL:      lockTTL,
	}, nil)

	if err != nil {
		return consulutil.Session{}, nil, util.Errorf("Could not create session")
	}

	if renewalCh == nil {
		renewalCh = time.NewTicker(renewalInterval).C
	}

	quitCh := make(chan struct{})
	renewalErrCh := make(chan error, 1)
	consulSession := consulutil.NewManagedSession(
		c.client,
		session,
		name,
		quitCh,
		renewalErrCh,
		renewalCh)

	return consulSession, renewalErrCh, nil
}

func (c consulStore) NewUnmanagedSession(session, name string) Session {
	return consulutil.NewUnmanagedSession(c.client, session, name)

}

// determine the name and ID of the session that is locking this key, if any
func (c consulStore) LockHolder(key string) (string, string, error) {
	kvp, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return "", "", consulutil.NewKVError("get", key, err)
	}
	if kvp == nil || kvp.Session == "" {
		return "", "", nil
	}

	se, _, err := c.client.Session().Info(kvp.Session, nil)
	if err != nil {
		return "", "", util.Errorf("Could not get lock information for %q held by id %q", key, kvp.Session)
	}
	return se.Name, se.ID, nil
}

func (c consulStore) DestroyLockHolder(id string) error {
	_, err := c.client.Session().Destroy(id, nil)
	time.Sleep(lockDelay)
	return err
}
