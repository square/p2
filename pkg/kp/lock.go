package kp

import (
	"fmt"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/util"
)

type Lock struct {
	client  *api.Client
	session string
	name    string

	// Coordination channels
	//
	// signals that continual renewal of lock should stop
	quitCh chan struct{}

	// communicates any error occuring during renewal
	renewalErrCh chan error

	// signals when a renewal on the consul lock should be performed
	renewalCh <-chan time.Time
}

type AlreadyLockedError struct {
	Key string
}

const (
	lockTTL         = "15s"
	renewalInterval = 10 * time.Second
)

func (err AlreadyLockedError) Error() string {
	return fmt.Sprintf("Key %q is already locked", err.Key)
}

func (c consulStore) NewLock(name string, renewalCh <-chan time.Time) (Lock, chan error, error) {
	session, _, err := c.client.Session().CreateNoChecks(&api.SessionEntry{
		Name: name,
		// if the lock delay is zero, it becomes the default value, which is 15s
		// we want to release locks right away (so that a losing process does
		// not delay a winning one), so we must set the shortest possible delay
		// that is nonzero
		LockDelay: 1 * time.Nanosecond,
		// locks should only be used with ephemeral keys
		Behavior: api.SessionBehaviorDelete,
		TTL:      lockTTL,
	}, nil)

	if err != nil {
		return Lock{}, nil, util.Errorf("Could not create lock")
	}

	if renewalCh == nil {
		renewalCh = time.NewTicker(renewalInterval).C
	}

	quitCh := make(chan struct{})
	renewalErrCh := make(chan error, 1)
	lock := Lock{
		client:       c.client,
		session:      session,
		name:         name,
		quitCh:       quitCh,
		renewalErrCh: renewalErrCh,
		renewalCh:    renewalCh,
	}

	// Could explore using c.client.Session().RenewPeriodic() instead, but
	// specifying a renewalCh is nice for testing
	go lock.continuallyRenew()
	return lock, renewalErrCh, nil
}

// determine the name and ID of the session that is locking this key, if any
func (c consulStore) LockHolder(key string) (string, string, error) {
	kvp, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return "", "", NewKVError("get", key, err)
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
	return err
}

// attempts to acquire the lock on the targeted key
// keys used for locking/synchronization should be ephemeral (ie their value
// does not matter and you don't care if they're deleted)
func (l Lock) Lock(key string) error {
	success, _, err := l.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(l.name),
		Session: l.session,
	}, nil)

	if err != nil {
		return NewKVError("acquire lock", key, err)
	}
	if success {
		return nil
	}
	return AlreadyLockedError{Key: key}
}

func (l Lock) continuallyRenew() {
	defer close(l.renewalErrCh)
	for {
		select {
		case <-l.renewalCh:
			err := l.Renew()
			if err != nil {
				l.renewalErrCh <- err
				l.client.Session().Destroy(l.session, nil)
				return
			}
		case <-l.quitCh:
			return
		}
	}
}

// refresh the TTL on this lock
func (l Lock) Renew() error {
	_, _, err := l.client.Session().Renew(l.session, nil)
	if err != nil {
		return util.Errorf("Could not renew lock")
	}
	return nil
}

// destroy a lock, releasing and deleting all the keys it holds
func (l Lock) Destroy() error {
	close(l.quitCh)

	// There is a race here if (lockTTL - renewalInterval) time passes and
	// the lock is destroyed automatically before we do it explicitly. In
	// practice that shouldn't be an issue
	_, err := l.client.Session().Destroy(l.session, nil)
	if err != nil {
		return util.Errorf("Could not destroy lock")
	}
	return nil
}
