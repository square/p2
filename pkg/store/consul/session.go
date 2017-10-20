package consul

import (
	"context"
	"fmt"
	"time"

	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

const (
	// Consul's minimum lock delay is 1ms. Requesting a lock delay of 0 will be
	// interpreted as "use the default," which is 15s at this time.
	lockDelay       = 1 * time.Millisecond
	lockTTL         = "15s"
	renewalInterval = 10 * time.Second
)

// attempts to acquire the lock on the targeted key. keys used for

// Represents a session that can be used to lock keys in the KV store. The only
// current implementation wraps consul sessions, which can be used to obtain
// locks on multiple keys, and must be periodically renewed.
type Session interface {
	Lock(key string) (Unlocker, error)
	LockTxn(
		lockCtx context.Context,
		key string,
	) (TxnUnlocker, error)
	LockIfKeyNotExistsTxn(
		ctx context.Context,
		key string,
		value []byte,
	) (TxnUnlocker, error)
	Renew() error
	Destroy() error
	Session() string
}

func (c consulStore) NewSession(name string, renewalCh <-chan time.Time) (Session, chan error, error) {
	return NewSession(c.client, name, renewalCh)
}

func (c consulStore) NewUnmanagedSession(session, name string) Session {
	return NewUnmanagedSession(c.client, session, name)
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

// locking/synchronization should be ephemeral (ie their value does not matter
// and you don't care if they're deleted)
func (s session) Lock(key string) (Unlocker, error) {
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

// LockTxn takes a context and a key to lock as arguments. It will do the
// following:
// 1) Add the KV operations required to lock key to lockCtx. The caller must
// commit the transaction with transaction.Commit() before any locks are
// acquired
// 2) return an TxnUnlocker which allows for building consul transactions for both
// unlocking the key and checking that the lock is held
func (s session) LockTxn(
	lockCtx context.Context,
	key string,
) (TxnUnlocker, error) {
	err := transaction.Add(lockCtx, api.KVTxnOp{
		Verb:    api.KVLock,
		Key:     key,
		Session: s.session,
		Value:   []byte(s.name),
	})
	if err != nil {
		return nil, util.Errorf("could not build key locking transaction: %s", err)
	}

	return &txnUnlocker{
		session: s.session,
		key:     key,
	}, nil
}

// LockIfKeyNotExistsTxn will lock and set a key, guaranteeing the key did not
// exist.
func (s session) LockIfKeyNotExistsTxn(
	ctx context.Context,
	key string,
	value []byte,
) (TxnUnlocker, error) {
	// KVDeleteCAS with index 0 is used because it ensures that the subsequent
	// lock will fail if the key existed.
	err := transaction.Add(ctx, api.KVTxnOp{
		Verb:  string(api.KVDeleteCAS),
		Key:   key,
		Index: 0,
	})
	if err != nil {
		return nil, err
	}

	err = transaction.Add(ctx, api.KVTxnOp{
		Verb:    string(api.KVLock),
		Key:     key,
		Value:   value,
		Session: s.session,
	})
	if err != nil {
		return nil, err
	}

	return &txnUnlocker{
		session: s.session,
		key:     key,
	}, nil
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

func (s session) continuallyRenew() {
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
	session session

	key string
}

// TxnUnlocker represents a lock that is held in consul, with convenience
// functions for operating with the lock
type TxnUnlocker interface {
	// UnlockTxn adds consul operations to the transaction within the passed
	// context to unlock the lock. The caller must call transaction.Commit() with
	// the passed context for the lock to actually be released
	UnlockTxn(ctx context.Context) error

	// CheckLockedTxn adds consul operations to the transaction within the passed
	// context to check that the lock is still held. This is useful for callers
	// that wish for the operations they are attempting to fail if the lock is no
	// longer held for any reason
	CheckLockedTxn(ctx context.Context) error
}

type txnUnlocker struct {
	session string

	key string
}

func (t *txnUnlocker) UnlockTxn(unlockCtx context.Context) error {
	// check that this session still holds the lock
	err := transaction.Add(unlockCtx, api.KVTxnOp{
		Verb:    api.KVCheckSession,
		Key:     t.key,
		Session: t.session,
	})
	if err != nil {
		return util.Errorf("could not build key unlocking transaction: %s", err)
	}

	// delete the lock (which will fail if we're not still holding it
	// thanks to the check-session operation
	err = transaction.Add(unlockCtx, api.KVTxnOp{
		Verb: api.KVDelete,
		Key:  t.key,
	})
	if err != nil {
		return util.Errorf("could not build key unlocking transaction: %s", err)
	}

	return nil
}

func (t *txnUnlocker) CheckLockedTxn(checkLockedCtx context.Context) error {
	// check that this session still holds the lock
	err := transaction.Add(checkLockedCtx, api.KVTxnOp{
		Verb:    api.KVCheckSession,
		Key:     t.key,
		Session: t.session,
	})
	if err != nil {
		return util.Errorf("could not build lock checking transaction: %s", err)
	}

	return nil
}

// Wraps a consul client and consul session, and provides coordination for
// renewals and errors
type session struct {
	client  consulutil.ConsulClient
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

func NewManagedSession(client consulutil.ConsulClient, sessionID string, name string, quitCh chan struct{}, renewalErrCh chan error, renewalCh <-chan time.Time) Session {
	sess := &session{
		client:       client,
		session:      sessionID,
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
func NewUnmanagedSession(client consulutil.ConsulClient, sessionID, name string) Session {
	return session{
		client:  client,
		session: sessionID,
		name:    name,
	}
}

func NewSession(client consulutil.ConsulClient, name string, renewalCh <-chan time.Time) (Session, chan error, error) {
	sessionID, _, err := client.Session().CreateNoChecks(&api.SessionEntry{
		Name:      name,
		LockDelay: lockDelay,
		// locks should only be used with ephemeral keys
		Behavior: api.SessionBehaviorDelete,
		TTL:      lockTTL,
	}, nil)

	if err != nil {
		return session{}, nil, util.Errorf("Could not create session")
	}

	if renewalCh == nil {
		renewalCh = time.NewTicker(renewalInterval).C
	}

	quitCh := make(chan struct{})
	renewalErrCh := make(chan error, 1)
	consulSession := NewManagedSession(
		client,
		sessionID,
		name,
		quitCh,
		renewalErrCh,
		renewalCh)

	return consulSession, renewalErrCh, nil
}

// SessionContext creates a consul session and keeps it renewed until either a
// renewal error occurs or the passed context is canceled. If a renewal error
// occurs, it will cancel the output context
func SessionContext(ctx context.Context, client consulutil.ConsulClient, name string) (context.Context, Session, error) {
	sessionID, _, err := client.Session().CreateNoChecks(&api.SessionEntry{
		Name:      name,
		LockDelay: lockDelay,
		// locks should only be used with ephemeral keys
		Behavior: api.SessionBehaviorDelete,
		TTL:      lockTTL,
	}, nil)
	if err != nil {
		return nil, nil, util.Errorf("could not create session: %s", err)
	}

	// adapt the passed context do a quit channel
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		_, _ = client.Session().Destroy(sessionID, nil)
	}()

	retCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		_ = client.Session().RenewPeriodic(lockTTL, sessionID, nil, done)
	}()

	return retCtx, NewUnmanagedSession(client, sessionID, name), nil
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
func (s session) Renew() error {
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
func (s session) Destroy() error {
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
func (s session) Session() string {
	return s.session
}
