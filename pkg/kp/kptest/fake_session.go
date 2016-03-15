package kptest

import (
	"sync"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util"
)

// This cannot currently be used to test sessions competing for the same locks
// as the fake session keeps its own lock struct.
// TODO: make a global lock store that is shared between all instances of
// fakeSession
type fakeSession struct {
	locks     map[string]bool
	mu        sync.Mutex
	destroyed bool

	renewalErrCh chan error

	// list of locks held to release when session is destroyed
	locksHeld map[string]bool
}

func newFakeSession(globalLocks map[string]bool, lockMutex sync.Mutex, renewalErrCh chan error) kp.Session {
	return &fakeSession{
		locks:        globalLocks,
		mu:           lockMutex,
		renewalErrCh: renewalErrCh,
		locksHeld:    make(map[string]bool),
	}
}

var _ kp.Session = &fakeSession{}

type fakeUnlocker struct {
	key     string
	session *fakeSession
}

func (u *fakeUnlocker) Unlock() error {
	u.session.mu.Lock()
	defer u.session.mu.Unlock()
	if u.session.destroyed {
		return util.Errorf("Fake session destroyed, cannot unlock")
	}

	u.session.locks[u.key] = false
	delete(u.session.locksHeld, u.key)
	return nil
}

func (u *fakeUnlocker) Key() string {
	return u.key
}

func (f *fakeSession) Lock(key string) (kp.Unlocker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.destroyed {
		return nil, util.Errorf("Fake session destroyed, cannot lock")
	}

	if f.locks[key] {
		return nil, kp.AlreadyLockedError{
			Key: key,
		}
	}

	f.locks[key] = true
	f.locksHeld[key] = true
	return &fakeUnlocker{
		key:     key,
		session: f,
	}, nil
}

// Not currently implemented
func (f *fakeSession) Renew() error {
	return nil
}

func (f *fakeSession) Destroy() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroyed = true
	for k, _ := range f.locksHeld {
		delete(f.locks, k)
	}
	close(f.renewalErrCh)
	return nil
}
