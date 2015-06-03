package kp

import (
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/util"
)

type Lock struct {
	client  *api.Client
	session string
	name    string
}

type AlreadyLockedError struct {
	Key string
}

func (err AlreadyLockedError) Error() string {
	return fmt.Sprintf("Key %q is already locked", err.Key)
}

func (s *Store) NewLock(name string) (Lock, error) {
	session, _, err := s.client.Session().CreateNoChecks(&api.SessionEntry{
		Name: name,
		// if the lock delay is zero, it becomes the default value, which is 15s
		// we want to release locks right away (so that a losing process does
		// not delay a winning one), so we must set the shortest possible delay
		// that is nonzero
		LockDelay: 1 * time.Nanosecond,
		// locks should only be used with ephemeral keys
		Behavior: api.SessionBehaviorDelete,
		TTL:      "15s",
	}, nil)

	if err != nil {
		return Lock{}, util.Errorf("Could not create lock")
	}
	return Lock{
		client:  s.client,
		session: session,
		name:    name,
	}, nil
}

// determine the name and ID of the session that is locking this key, if any
func (s *Store) LockHolder(key string) (string, string, error) {
	kvp, _, err := s.client.KV().Get(key, nil)
	if err != nil {
		return "", "", KVError{Op: "get", Key: key, UnsafeError: err}
	}
	if kvp == nil || kvp.Session == "" {
		return "", "", nil
	}

	se, _, err := s.client.Session().Info(kvp.Session, nil)
	if err != nil {
		return "", "", util.Errorf("Could not get lock information for %q held by id %q", key, kvp.Session)
	}
	return se.Name, se.ID, nil
}

func (s *Store) DestroyLockHolder(id string) error {
	_, err := s.client.Session().Destroy(id, nil)
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
		return util.Errorf("Could not acquire lock on %s", key)
	}
	if success {
		return nil
	}
	return AlreadyLockedError{Key: key}
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
	_, err := l.client.Session().Destroy(l.session, nil)
	if err != nil {
		return util.Errorf("Could not destroy lock")
	}
	return nil
}
