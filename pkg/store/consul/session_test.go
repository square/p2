// +build !race

package consul

import (
	"context"
	"testing"
	"time"

	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"
)

const (
	lockMessage = "Locked by lock_test.go"
)

func TestSession(t *testing.T) {
	fixture := NewConsulTestFixture(t)
	defer fixture.Close()

	sessionRenewalCh := make(chan time.Time)
	session, _, err := fixture.Store.NewSession(lockMessage, sessionRenewalCh)
	if err != nil {
		t.Fatalf("Unable to create session: %s", err)
	}
	defer session.Destroy()

	key := "some_key"
	_, err = session.Lock(key)
	if err != nil {
		t.Fatalf("Unable to acquire lock: %s", err)
	}

	bytes := fixture.GetKV(key)
	if string(bytes) != lockMessage {
		t.Errorf("Expected lock message for '%s' to be '%s', was '%s'", key, lockMessage, string(bytes))
	}

	select {
	case sessionRenewalCh <- time.Now():
	case <-time.After(1 * time.Second):
		t.Fatalf("Sending to renewal channel blocked, session renewal is probably broken")
	}
}

func TestLockExclusion(t *testing.T) {
	fixture := NewConsulTestFixture(t)
	defer fixture.Close()

	session, _, err := fixture.Store.NewSession(lockMessage, make(chan time.Time))
	if err != nil {
		t.Fatalf("Unable to create session: %s", err)
	}
	defer session.Destroy()

	key := "some_key"
	_, err = session.Lock(key)
	if err != nil {
		t.Fatalf("Unable to acquire lock: %s", err)
	}

	// Now try to create a new session and lock the same key, which
	// should fail
	session2, _, err := fixture.Store.NewSession(lockMessage, make(chan time.Time))
	if err != nil {
		t.Fatalf("Unable to create second session: %s", err)
	}
	defer session2.Destroy()

	_, err = session2.Lock(key)
	if err == nil {
		t.Fatalf("Should have failed to acquire the same lock using a second session")
	}
}

func TestRenewalFailsWhen404(t *testing.T) {
	fixture := NewConsulTestFixture(t)
	defer fixture.Close()

	session, _, err := fixture.Store.NewSession(lockMessage, make(chan time.Time))
	if err != nil {
		t.Fatalf("Unable to create session: %s", err)
	}

	err = session.Renew()
	if err != nil {
		t.Errorf("Renewal should have succeeded: %s", err)
	}

	session.Destroy()
	err = session.Renew()
	if err == nil {
		t.Errorf("Renewing a destroyed session should have failed, but it succeeded")
	}
}

func TestLockTxn(t *testing.T) {
	fixture := NewConsulTestFixture(t)
	defer fixture.Close()

	session, renewalErrCh, err := NewSession(fixture.Client, "session-name", nil)
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error)
	go func() {
		defer close(errCh)

		lockCtx, lockCtxCancel := transaction.New(context.Background())
		defer lockCtxCancel()
		unlocker, err := session.LockTxn(lockCtx, "some_key")
		if err != nil {
			errCh <- err
			return
		}

		checkLockedCtx, checkLockedCtxCancel := transaction.New(context.Background())
		defer checkLockedCtxCancel()
		err = unlocker.CheckLockedTxn(checkLockedCtx)
		if err != nil {
			t.Fatal(err)
		}

		// confirm that the checkLockedCtx transaction fails to apply
		// (because it checks the locks are held but we haven't
		// committed the locking transaction yet)
		ok, _, err := transaction.Commit(checkLockedCtx, fixture.Client.KV())
		if err != nil {
			errCh <- err
			return
		}
		if ok {
			errCh <- util.Errorf("checkLockedCtx transaction should have been rolled back since the locks aren't held yet")
			return
		}

		unlockCtx, unlockCtxCancel := transaction.New(context.Background())
		defer unlockCtxCancel()

		err = unlocker.UnlockTxn(unlockCtx)
		if err != nil {
			t.Fatal(err)
		}

		// confirm that the unlockCtx transaction fails to apply because we don't hold the locks yet
		ok, _, err = transaction.Commit(unlockCtx, fixture.Client.KV())
		if err != nil {
			errCh <- err
			return
		}
		if ok {
			errCh <- util.Errorf("unlockCtx transaction should have been rolled back since the locks aren't held yet")
			return
		}

		// now grab the locks
		err = transaction.MustCommit(lockCtx, fixture.Client.KV())
		if err != nil {
			errCh <- err
			return
		}

		// now confirm the checkLockedCtx can be committed. we have to copy it first because you can't commit the same transaction twice
		checkLockedCtx, checkLockedCtxCancel = transaction.New(checkLockedCtx)
		defer checkLockedCtxCancel()
		err = transaction.MustCommit(checkLockedCtx, fixture.Client.KV())
		if err != nil {
			errCh <- err
			return
		}

		// now unlock, but we have to copy the transaction first because you can't commit the same one twice
		unlockCtx, unlockCtxCancel = transaction.New(unlockCtx)
		defer unlockCtxCancel()
		err = transaction.MustCommit(unlockCtx, fixture.Client.KV())
		if err != nil {
			errCh <- err
			return
		}

		// now confirm that the check locked txn fails again
		checkLockedCtx, checkLockedCtxCancel = transaction.New(checkLockedCtx)
		defer checkLockedCtxCancel()
		ok, _, err = transaction.Commit(checkLockedCtx, fixture.Client.KV())
		if err != nil {
			errCh <- err
			return
		}
		if ok {
			errCh <- util.Errorf("checkLockedCtx transaction should have been rolled back since the locks were released")
			return
		}
	}()

	select {
	case err := <-renewalErrCh:
		t.Fatal(err)
	case err, ok := <-errCh:
		if ok {
			t.Fatal(err)
		}
	}
}
