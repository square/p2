package consul

import (
	"context"
	"time"

	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/util"
)

// Represents a session that can be used to lock keys in the KV store. The only
// current implementation wraps consul sessions, which can be used to obtain
// locks on multiple keys, and must be periodically renewed.
type Session interface {
	Lock(key string) (consulutil.Unlocker, error)
	LockTxn(
		lockCtx context.Context,
		cleanupCtx context.Context,
		checkLockedCtx context.Context,
		key string,
	) error
	Renew() error
	Destroy() error
	Session() string
}

const (
	lockDelay = 1 * time.Millisecond
)

func (c consulStore) NewSession(name string, renewalCh <-chan time.Time) (Session, chan error, error) {
	return consulutil.NewSession(c.client, name, renewalCh)
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
