package kp

import (
	"testing"
	"time"
)

const (
	lockMessage = "Locked by lock_test.go"
)

func TestLock(t *testing.T) {
	store, server := makeStore(t)
	defer server.Stop()

	lockRenewalCh := make(chan time.Time)
	lock, _, err := store.NewLock(lockMessage, lockRenewalCh)
	if err != nil {
		t.Fatalf("Unable to create lock: %s", err)
	}
	defer lock.Destroy()

	key := "some_key"
	err = lock.Lock(key)
	if err != nil {
		t.Fatalf("Unable to acquire lock: %s", err)
	}

	bytes := server.GetKV(key)
	if string(bytes) != lockMessage {
		t.Errorf("Expected lock message for '%s' to be '%s', was '%s'", key, lockMessage, string(bytes))
	}

	select {
	case lockRenewalCh <- time.Now():
	case <-time.After(1 * time.Second):
		t.Fatalf("Sending to renewal channel blocked, lock renewal is probably broken")
	}
}
