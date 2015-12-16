package kp

import (
	"testing"
	"time"
)

const (
	lockMessage = "Locked by lock_test.go"
)

func TestLock(t *testing.T) {
	fixture := NewConsulTestFixture(t)
	defer fixture.Close()

	lockRenewalCh := make(chan time.Time)
	lock, _, err := fixture.Store.NewLock(lockMessage, lockRenewalCh)
	if err != nil {
		t.Fatalf("Unable to create lock: %s", err)
	}
	defer lock.Destroy()

	key := "some_key"
	err = lock.Lock(key)
	if err != nil {
		t.Fatalf("Unable to acquire lock: %s", err)
	}

	bytes := fixture.Server.GetKV(key)
	if string(bytes) != lockMessage {
		t.Errorf("Expected lock message for '%s' to be '%s', was '%s'", key, lockMessage, string(bytes))
	}

	select {
	case lockRenewalCh <- time.Now():
	case <-time.After(1 * time.Second):
		t.Fatalf("Sending to renewal channel blocked, lock renewal is probably broken")
	}
}

func TestLockExclusion(t *testing.T) {
	fixture := NewConsulTestFixture(t)
	defer fixture.Close()

	lock, _, err := fixture.Store.NewLock(lockMessage, make(chan time.Time))
	if err != nil {
		t.Fatalf("Unable to create lock: %s", err)
	}
	defer lock.Destroy()

	key := "some_key"
	err = lock.Lock(key)
	if err != nil {
		t.Fatalf("Unable to acquire lock: %s", err)
	}

	// Now try to create a new lock session and lock the same key, which
	// should fail
	lock2, _, err := fixture.Store.NewLock(lockMessage, make(chan time.Time))
	if err != nil {
		t.Fatalf("Unable to create second lock: %s", err)
	}
	defer lock2.Destroy()

	err = lock2.Lock(key)
	if err == nil {
		t.Fatalf("Should have failed to acquire the same lock using a second session")
	}
}

func TestRenewalFailsWhen404(t *testing.T) {
	fixture := NewConsulTestFixture(t)
	defer fixture.Close()

	lock, _, err := fixture.Store.NewLock(lockMessage, make(chan time.Time))
	if err != nil {
		t.Fatalf("Unable to create lock: %s", err)
	}

	err = lock.Renew()
	if err != nil {
		t.Errorf("Renewal should have succeeded: %s", err)
	}

	lock.Destroy()
	err = lock.Renew()
	if err == nil {
		t.Errorf("Renewing a destroyed lock should have failed, but it succeeded")
	}
}
