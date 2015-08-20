package kp

import (
	"testing"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/logging"
)

// A basic test of the ConsulSessionManager: create a session, then quit.
func TestSessionBasics(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	sessions := make(chan string)
	done := make(chan struct{})
	go ConsulSessionManager(
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
