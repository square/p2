// +build !race

package consul

import (
	"testing"
	"time"
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
