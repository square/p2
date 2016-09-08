package consulutil

import (
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/logging"
)

// A basic test of the ConsulSessionManager: create a session, then quit.
func TestSessionBasics(t *testing.T) {
	t.Parallel()
	f := NewFixture(t)
	defer f.Stop()

	sessions := make(chan string)
	done := make(chan struct{})
	go SessionManager(
		api.SessionEntry{
			Behavior: api.SessionBehaviorDelete,
			TTL:      "10s",
		},
		f.Client,
		sessions,
		done,
		logging.TestLogger(),
	)

	ok := false
	for s := range sessions {
		if s != "" && done != nil {
			ok = true
			close(done)
			done = nil
		}
	}
	if !ok {
		t.Error("valid session never appeared")
	}
}

// TestExit checks that the session manager function exits once the session reader quits
// reading and signals to the manager to exit.
func TestExit(t *testing.T) {
	t.Parallel()
	f := NewFixture(t)
	defer f.Stop()

	sessions := make(chan string)
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		SessionManager(
			api.SessionEntry{TTL: "10s"},
			f.Client,
			sessions,
			done,
			logging.TestLogger(),
		)
		close(exited)
	}()

	// Wait for a session to appear
	for s := ""; s == ""; s = <-sessions {
	}

	// Closing the "done" channel signals to the session manager that the reader has quit
	// reading the stream. It should exit soon thereafter.
	close(done)
	select {
	case <-exited:
	case <-time.After(20 * time.Second):
		t.Error("timed out waiting for SessionManager to exit")
	}
}

// A basic test of WithSession: create and destroy sessions
func TestWithSession(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	defer wg.Wait()
	sessions := make(chan string)
	defer close(sessions)

	sessionsReceived := make(chan string)
	wg.Add(1)
	go func() {
		defer wg.Done()
		WithSession(nil, sessions, func(done <-chan struct{}, session string) {
			wg.Add(1)
			defer wg.Done()
			t.Log("started with session", session)
			sessionsReceived <- session
			<-done
			t.Log("terminated")
		})
	}()

	sessions <- ""
	sessions <- "1"
	if s := <-sessionsReceived; s != "1" {
		t.Error("received session", s)
	}
	sessions <- ""

	sessions <- "2"
	if s := <-sessionsReceived; s != "2" {
		t.Error("received session", s)
	}
	// no "" this time

	sessions <- "3"
	if s := <-sessionsReceived; s != "3" {
		t.Error("received session", s)
	}
}

// Test that WithSession passes the correct session to f if f is slow to terminate.
func TestWithSessionSlowF(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	defer wg.Wait()
	sessions := make(chan string)
	defer close(sessions)

	sessionsReceived := make(chan string)
	ready := make(chan struct{})
	release := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		WithSession(nil, sessions, func(done <-chan struct{}, session string) {
			wg.Add(1)
			defer wg.Done()
			t.Log("started with session", session)
			sessionsReceived <- session
			<-done
			t.Log("terminated")
			// wait to exit until signaled
			ready <- struct{}{}
			t.Log("released")
			<-release
		})
	}()

	// Make sure that f is running under session 1
	t.Log("beginning session 1")
	sessions <- "1"
	if s := <-sessionsReceived; s != "1" {
		t.Error("received session ", s)
	}

	// Session 2 will come and go before f can be started again
	t.Log("ending session 1")
	sessions <- ""
	<-ready
	t.Log("beginning session 2")
	sessions <- "2"
	t.Log("ending session 2")
	sessions <- ""
	t.Log("releasing f")
	release <- struct{}{}

	// Session 2 shouldn't be started
	select {
	case s := <-sessionsReceived:
		t.Error("received session ", s)
	case <-time.After(50 * time.Millisecond):
	}

	// Make sure it can still process new sessions
	t.Log("beginning session 3")
	sessions <- "3"
	if s := <-sessionsReceived; s != "3" {
		t.Error("received session ", s)
	}
	t.Log("ending session 3")
	sessions <- ""
	<-ready
	release <- struct{}{}
}

// TestWithSessionPanic verifies that if a session function panics, the panic value is
// propagated out of the WithSession() call.
func TestWithSessionPanic(t *testing.T) {
	t.Parallel()
	sessions := make(chan string, 1)
	sessions <- "100"

	defer func() {
		if p := recover(); p.(string) != "hello" {
			t.Errorf("panic value did not propagate: %#v", p)
		}
	}()
	WithSession(nil, sessions, func(done <-chan struct{}, session string) {
		panic("hello")
	})
}
