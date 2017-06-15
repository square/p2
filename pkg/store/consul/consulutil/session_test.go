// +build !race

package consulutil

import (
	"context"
	"testing"

	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"
)

func TestLockTxn(t *testing.T) {
	fixture := NewFixture(t)

	session, renewalErrCh, err := NewSession(fixture.Client, "session-name", nil)
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error)
	go func() {
		defer close(errCh)

		lockCtx, lockCtxCancel := transaction.New(context.Background())
		defer lockCtxCancel()
		unlockCtx, unlockCtxCancel := transaction.New(context.Background())
		defer unlockCtxCancel()
		checkLockedCtx, checkLockedCtxCancel := transaction.New(context.Background())
		defer checkLockedCtxCancel()
		err := session.LockTxn(lockCtx, unlockCtx, checkLockedCtx, "some_key")
		if err != nil {
			errCh <- err
			return
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
			errCh <- util.Errorf("checkLockedCtx transaction should have been rolled back since the locks aren't held yet")
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
