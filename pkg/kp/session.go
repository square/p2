package kp

import (
	"fmt"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/util"
)

type Unlocker interface {
	Unlock() error
	Key() string
}

type unlocker struct {
	session consulSession

	key string
}

// Represents a session that can be used to lock keys in the KV store. The only
// current implementation wraps consul sessions, which can be used to obtain
// locks on multiple keys, and must be periodically renewed.
type Session interface {
	Lock(key string) (Unlocker, error)
	Renew() error
	Destroy() error
}

// Wraps a consul client and consul session, and provides coordination for
// renewals and errors
type consulSession struct {
	client  *api.Client
	session string
	name    string

	// Coordination channels
	//
	// signals that continual renewal of session should stop
	quitCh chan struct{}

	// communicates any error occuring during renewal
	renewalErrCh chan error

	// signals when a renewal on the consul session should be performed
	renewalCh <-chan time.Time
}

type AlreadyLockedError struct {
	Key string
}

const (
	lockTTL         = "15s"
	renewalInterval = 10 * time.Second

	// Consul's minimum lock delay is 1ms. Requesting a lock delay of 0 will be
	// interpreted as "use the default," which is 15s at this time.
	lockDelay = 1 * time.Millisecond
)

func (err AlreadyLockedError) Error() string {
	return fmt.Sprintf("Key %q is already locked", err.Key)
}

func (c consulStore) NewSession(name string, renewalCh <-chan time.Time) (Session, chan error, error) {
	session, _, err := c.client.Session().CreateNoChecks(&api.SessionEntry{
		Name:      name,
		LockDelay: lockDelay,
		// locks should only be used with ephemeral keys
		Behavior: api.SessionBehaviorDelete,
		TTL:      lockTTL,
	}, nil)

	if err != nil {
		return consulSession{}, nil, util.Errorf("Could not create session")
	}

	if renewalCh == nil {
		renewalCh = time.NewTicker(renewalInterval).C
	}

	quitCh := make(chan struct{})
	renewalErrCh := make(chan error, 1)
	consulSession := consulSession{
		client:       c.client,
		session:      session,
		name:         name,
		quitCh:       quitCh,
		renewalErrCh: renewalErrCh,
		renewalCh:    renewalCh,
	}

	// Could explore using c.client.Session().RenewPeriodic() instead, but
	// specifying a renewalCh is nice for testing
	go consulSession.continuallyRenew()
	return consulSession, renewalErrCh, nil
}

// Creates a consulSession struct using an existing consul session, and does
// not set up auto-renewal. Use this constructor when the underlying session
// already exists and should not be managed here.
func (c consulStore) NewUnmanagedSession(session, name string) Session {
	return consulSession{
		client:  c.client,
		session: session,
		name:    name,
	}
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

// attempts to acquire the lock on the targeted key. keys used for
// locking/synchronization should be ephemeral (ie their value does not matter
// and you don't care if they're deleted)
func (s consulSession) Lock(key string) (Unlocker, error) {
	success, _, err := s.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(s.name),
		Session: s.session,
	}, nil)

	if err != nil {
		return nil, consulutil.NewKVError("acquire lock", key, err)
	}
	if success {
		return unlocker{
			session: s,
			key:     key,
		}, nil
	}
	return nil, AlreadyLockedError{Key: key}
}

// attempts to unlock the targeted key - since lock keys are ephemeral, this
// will delete it, but only if it is held by the current lock
func (u unlocker) Unlock() error {
	kvp, meta, err := u.session.client.KV().Get(u.key, nil)
	if err != nil {
		return consulutil.NewKVError("get", u.key, err)
	}
	if kvp == nil {
		return nil
	}
	if kvp.Session != u.session.session {
		return AlreadyLockedError{Key: u.key}
	}

	success, _, err := u.session.client.KV().DeleteCAS(&api.KVPair{
		Key:         u.key,
		ModifyIndex: meta.LastIndex,
	}, nil)
	if err != nil {
		return consulutil.NewKVError("deletecas", u.key, err)
	}
	if !success {
		// the key has been mutated since we checked it - probably someone
		// overrode our lock on it or deleted it themselves
		return AlreadyLockedError{Key: u.key}
	}
	return nil
}

func (u unlocker) Key() string {
	return u.key
}

func (s consulSession) continuallyRenew() {
	defer close(s.renewalErrCh)
	for {
		select {
		case <-s.renewalCh:
			err := s.Renew()
			if err != nil {
				s.renewalErrCh <- err
				s.client.Session().Destroy(s.session, nil)
				return
			}
		case <-s.quitCh:
			return
		}
	}
}

// refresh the TTL on this lock
func (s consulSession) Renew() error {
	entry, _, err := s.client.Session().Renew(s.session, nil)
	if err != nil {
		return util.Errorf("Could not renew lock")
	}

	if entry == nil {
		return util.Errorf("Could not renew because session was destroyed")
	}
	return nil
}

// destroy a lock, releasing and deleting all the keys it holds
func (s consulSession) Destroy() error {
	if s.quitCh != nil {
		close(s.quitCh)
	}

	// There is a race here if (lockTTL - renewalInterval) time passes and
	// the lock is destroyed automatically before we do it explicitly. In
	// practice that shouldn't be an issue
	_, err := s.client.Session().Destroy(s.session, nil)
	if err != nil {
		return util.Errorf("Could not destroy lock")
	}
	return nil
}
