package consulutil

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"
)

const (
	LOCK_TREE = "lock"
	// Consul's minimum lock delay is 1ms. Requesting a lock delay of 0 will be
	// interpreted as "use the default," which is 15s at this time.
	lockDelay       = 1 * time.Millisecond
	lockTTL         = "15s"
	renewalInterval = 10 * time.Second
)

// attempts to acquire the lock on the targeted key. keys used for
// locking/synchronization should be ephemeral (ie their value does not matter
// and you don't care if they're deleted)
func (s Session) Lock(key string) (Unlocker, error) {
	success, _, err := s.client.KV().Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(s.name),
		Session: s.session,
	}, nil)

	if err != nil {
		return nil, NewKVError("acquire lock", key, err)
	}
	if success {
		return unlocker{
			session: s,
			key:     key,
		}, nil
	}
	return nil, AlreadyLockedError{Key: key}
}

// LockTxn takes three contexts and a key to lock as arguments. It will do the
// following:
// 1) Add the KV operations required to lock key to lockCtx. The caller must
// commit the transaction with transaction.Commit() before any locks are
// acquired
// 2) Add the KV operations required to UNLOCK key to unlockCtx. The operations
// guarantee that the lock is still held by the session otherwise the transaction
// will fail
// 3) Add the KV operations required to check that the key is still locked to
// checkLockedCtx. This is useful for callers that want to take actions only
// while they hold certain locks.
func (s Session) LockTxn(
	lockCtx context.Context,
	unlockCtx context.Context,
	checkLockedCtx context.Context,
	key string,
) error {
	err := transaction.Add(lockCtx, api.KVTxnOp{
		Verb:    api.KVLock,
		Key:     key,
		Session: s.session,
		Value:   []byte(s.name),
	})
	if err != nil {
		return util.Errorf("could not build key locking transaction: %s", err)
	}

	// check that this session still holds the lock
	err = transaction.Add(checkLockedCtx, api.KVTxnOp{
		Verb:    api.KVCheckSession,
		Key:     key,
		Session: s.session,
	})
	if err != nil {
		return util.Errorf("could not build lock checking transaction: %s", err)
	}

	// check that this session still holds the lock
	err = transaction.Add(unlockCtx, api.KVTxnOp{
		Verb:    api.KVCheckSession,
		Key:     key,
		Session: s.session,
	})
	if err != nil {
		return util.Errorf("could not build key unlocking transaction: %s", err)
	}

	// delete the lock (which will fail if we're not still holding it
	// thanks to the check-session operation
	err = transaction.Add(unlockCtx, api.KVTxnOp{
		Verb: api.KVDelete,
		Key:  key,
	})
	if err != nil {
		return util.Errorf("could not build key unlocking transaction: %s", err)
	}

	return nil
}

// attempts to unlock the targeted key - since lock keys are ephemeral, this
// will delete it, but only if it is held by the current lock
func (u unlocker) Unlock() error {
	kvp, meta, err := u.session.client.KV().Get(u.key, nil)
	if err != nil {
		return NewKVError("get", u.key, err)
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
		return NewKVError("deletecas", u.key, err)
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

func (s Session) continuallyRenew() {
	defer close(s.renewalErrCh)
	for {
		select {
		case <-s.renewalCh:
			err := s.Renew()
			if err != nil {
				s.renewalErrCh <- err
				_, _ = s.client.Session().Destroy(s.session, nil)
				return
			}
		case <-s.quitCh:
			return
		}
	}
}

type Unlocker interface {
	Unlock() error
	Key() string
}

type unlocker struct {
	session Session

	key string
}

// Wraps a consul client and consul session, and provides coordination for
// renewals and errors
type Session struct {
	client  ConsulClient
	session string
	name    string

	// Coordination channels
	//
	// signals that continual renewal of session should stop
	quitCh chan struct{}

	// communicates any error occurring during renewal
	renewalErrCh chan error

	// signals when a renewal on the consul session should be performed
	renewalCh <-chan time.Time
}

func NewManagedSession(client ConsulClient, session string, name string, quitCh chan struct{}, renewalErrCh chan error, renewalCh <-chan time.Time) *Session {
	sess := &Session{
		client:       client,
		session:      session,
		name:         name,
		quitCh:       quitCh,
		renewalErrCh: renewalErrCh,
		renewalCh:    renewalCh,
	}
	// Could explore using c.client.Session().RenewPeriodic() instead, but
	// specifying a renewalCh is nice for testing
	go sess.continuallyRenew()
	return sess
}

// Creates a Session struct using an existing consul session, and does
// not set up auto-renewal. Use this constructor when the underlying session
// already exists and should not be managed here.
func NewUnmanagedSession(client ConsulClient, session, name string) Session {
	return Session{
		client:  client,
		session: session,
		name:    name,
	}
}

func NewSession(client ConsulClient, name string, renewalCh <-chan time.Time) (Session, chan error, error) {
	session, _, err := client.Session().CreateNoChecks(&api.SessionEntry{
		Name:      name,
		LockDelay: lockDelay,
		// locks should only be used with ephemeral keys
		Behavior: api.SessionBehaviorDelete,
		TTL:      lockTTL,
	}, nil)

	if err != nil {
		return Session{}, nil, util.Errorf("Could not create session")
	}

	if renewalCh == nil {
		renewalCh = time.NewTicker(renewalInterval).C
	}

	quitCh := make(chan struct{})
	renewalErrCh := make(chan error, 1)
	consulSession := NewManagedSession(
		client,
		session,
		name,
		quitCh,
		renewalErrCh,
		renewalCh)

	return *consulSession, renewalErrCh, nil
}

type AlreadyLockedError struct {
	Key string
}

func (err AlreadyLockedError) Error() string {
	return fmt.Sprintf("Key %q is already locked", err.Key)
}

func IsAlreadyLocked(err error) bool {
	_, ok := err.(AlreadyLockedError)
	return ok
}

// refresh the TTL on this lock
func (s Session) Renew() error {
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
func (s Session) Destroy() error {
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

// Session returns the string identifier for the session that is tracked by the consul
// server. This is useful to expose for transactions that wish to lock or unlock keys
// or check that a lock is still held by a session
func (s Session) Session() string {
	return s.session
}
