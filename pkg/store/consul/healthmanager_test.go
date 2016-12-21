package consul

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/logging"
)

var (
	// A few different potential helth check results
	h1     = WatchResult{Id: "svc", Node: "node", Service: "svc", Status: "ok"}
	h2     = WatchResult{Id: "svc", Node: "node", Service: "svc", Status: "fox"}
	h3     = WatchResult{Id: "svc", Node: "node", Service: "svc", Status: "fuzzy"}
	hEmpty = WatchResult{}

	// The Consul key for accessing the health check
	hKey = HealthPath("svc", "node")
)

// Basic test of the full health manager: create a service, update its health, then
// destroy it.
func TestHealthBasic(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	waiter := f.NewKeyWaiter(hKey)
	manager := f.Store.NewHealthManager("node", logging.TestLogger())
	defer manager.Close()
	updater := manager.NewUpdater("svc", "svc")

	// Creating an updater with no health statuses shouldn't write anything
	time.Sleep(100 * time.Millisecond)
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || r != hEmpty {
		t.Fatalf("health expected to be empty, got value %#v error %#v", r, err)
	}

	// Write one health check result
	if err := updater.PutHealth(h1); err != nil {
		t.Error("error writing new health value: ", err)
	}

	// Check health result in Consul
	waiter.WaitForChange()
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || !r.ValueEquiv(h1) {
		t.Fatalf("unexpected health, got value %#v error %#v", r, err)
	}

	// Destroy the service, health check should disappear
	updater.Close()
	waiter.WaitForChange()
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || r != hEmpty {
		t.Fatalf("health expected to be empty, got value %#v error %#v", r, err)
	}
}

// Test that new service statuses are written to Consul and equivalent statues write nothing.
func TestHealthUpdate(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	manager := f.Store.NewHealthManager("node", logging.TestLogger())
	defer manager.Close()
	updater := manager.NewUpdater("svc", "svc")
	defer updater.Close()
	waiter := f.NewKeyWaiter(hKey)

	// Count how many changes to the health value there are
	counterChan := make(chan chan int, 1)
	counterWaiter := f.NewKeyWaiter(hKey)
	go func() {
		count := 0
		for {
			counterWaiter.WaitForChange()
			count += 1
			select {
			case c := <-counterChan:
				c <- count
				return
			default:
			}
		}
	}()

	if err := updater.PutHealth(h1); err != nil { // 1
		t.Error("error writing health: ", err)
	}
	waiter.WaitForChange()
	if err := updater.PutHealth(h1); err != nil { // still 1
		t.Error("error writing health: ", err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := updater.PutHealth(h1); err != nil { // still 1
		t.Error("error writing health: ", err)
	}
	if err := updater.PutHealth(h2); err != nil { // 2
		t.Error("error writing health: ", err)
	}
	waiter.WaitForChange()
	if err := updater.PutHealth(h2); err != nil { // still 2
		t.Error("error writing health: ", err)
	}
	c := make(chan int)
	counterChan <- c
	if err := updater.PutHealth(h1); err != nil { // 3
		t.Error("error writing health: ", err)
	}

	count := <-c
	t.Logf("Consul received %d updates", count)
	// Counter is asynchronous, so it's possible for it to miss an update.
	if !(2 <= count && count <= 3) {
		t.Fail()
	}
}

// Test that as long as there is no session, there should be no updates.
func TestHealthSessionRequired(t *testing.T) {
	// Standard Consul test fixture
	f := NewConsulTestFixture(t)
	defer f.Close()

	// Launch an updater with manual control over health checks and session management
	checks := make(chan WatchResult)
	sessions := make(chan string)
	go processHealthUpdater(f.Client, checks, sessions, logging.TestLogger())

	// There should be no health check initially
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || r != hEmpty {
		t.Fatalf("health expected to be empty, got value %#v error %#v", r, err)
	}

	// Adding health checks pre-session shouldn't affect Consul
	checks <- h1
	time.Sleep(50 * time.Millisecond)
	checks <- h1
	time.Sleep(50 * time.Millisecond)
	checks <- h2
	time.Sleep(100 * time.Millisecond)
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || r != hEmpty {
		t.Fatalf("health expected to be empty, got value %#v error %#v", r, err)
	}
}

// Test that if the session restarts, health checks should be restored.
func TestHealthSessionRestart(t *testing.T) {
	// Standard Consul test fixture
	f := NewConsulTestFixture(t)
	defer f.Close()

	// Launch an updater with manual control over health checks and session management
	checks := make(chan WatchResult)
	sessions := make(chan string)
	go processHealthUpdater(f.Client, checks, sessions, logging.TestLogger())
	waiter := f.NewKeyWaiter(hKey)

	// Add check & add session => write
	s1 := f.CreateSession()
	sessions <- s1
	checks <- h1
	waiter.WaitForChange()
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || !r.ValueEquiv(h1) {
		t.Fatalf("unexpected health, got value %#v error %#v", r, err)
	}

	// Changing the health status should send an update
	checks <- h2
	waiter.WaitForChange()
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || !r.ValueEquiv(h2) {
		t.Fatalf("unexpected health, got value %#v error %#v", r, err)
	}

	// Destroying the session should automatically clear the update
	f.DestroySession(s1)
	sessions <- ""
	waiter.WaitForChange()
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || r != hEmpty {
		t.Fatalf("health expected to be empty, got value %#v error %#v", r, err)
	}

	// No change when updating health mid-session
	checks <- h3
	time.Sleep(50 * time.Millisecond)
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || r != hEmpty {
		t.Fatalf("health expected to be empty, got value %#v error %#v", r, err)
	}

	// When the new session is refreshed, the key should reappear
	s2 := f.CreateSession()
	sessions <- s2
	waiter.WaitForChange()
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || !r.ValueEquiv(h3) {
		t.Fatalf("unexpected health, got value %#v error %#v", r, err)
	}

	// Shut down the health checker, deleting the health check
	close(checks)
	waiter.WaitForChange()
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || r != hEmpty {
		t.Fatalf("health expected to be empty, got value %#v error %#v", r, err)
	}
}

// When there is no session, an update + close should never be written.
func TestHealthSessionDestroy(t *testing.T) {
	// Standard Consul test fixture
	f := NewConsulTestFixture(t)
	defer f.Close()

	// Launch an updater with manual control over health checks and session management
	checks := make(chan WatchResult)
	sessions := make(chan string)
	go processHealthUpdater(f.Client, checks, sessions, logging.TestLogger())
	waiter := f.NewKeyWaiter(hKey)

	// Create health result & create session => write
	session := f.CreateSession()
	sessions <- session
	checks <- h1
	waiter.WaitForChange()
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || !r.ValueEquiv(h1) {
		t.Fatalf("unexpected health, got value %#v error %#v", r, err)
	}

	// Tell the health updater that the session ended => no action
	sessions <- ""
	time.Sleep(50 * time.Millisecond)

	// Give the updater a new check => no action
	checks <- h2
	time.Sleep(50 * time.Millisecond)

	// No more health checks => exit
	close(checks)
	time.Sleep(100 * time.Millisecond)
	if r, err := f.Store.GetHealth("svc", "node"); err != nil || !r.ValueEquiv(h1) {
		t.Fatalf("unexpected health, got value %#v error %#v", r, err)
	}
}

func TestThrottleChecks(t *testing.T) {
	type throttleTest struct {
		In            []health.HealthState
		ExpectedOut   []health.HealthState
		MaxBucketSize int64
		TestName      string
	}

	tests := []throttleTest{
		{ // Super high limit, no throttling expected
			In:            []health.HealthState{health.Passing, health.Passing, health.Critical, health.Passing},
			ExpectedOut:   []health.HealthState{health.Passing, health.Passing, health.Critical, health.Passing},
			MaxBucketSize: 100000,
			TestName:      "no throttle 1",
		},
		{ // Super high limit, no throttling expected
			In:            []health.HealthState{health.Unknown, health.Warning},
			ExpectedOut:   []health.HealthState{health.Unknown, health.Warning},
			MaxBucketSize: 100000,
			TestName:      "no throttle 2",
		},
		{ // Super low limit, throttling expected after 1 value
			In:            []health.HealthState{health.Passing, health.Critical, health.Passing, health.Critical},
			ExpectedOut:   []health.HealthState{health.Passing, health.Unknown, health.Unknown, health.Unknown},
			MaxBucketSize: 2,
			TestName:      "high throttle",
		},
		{ // throttling expected after 3 values
			In:            []health.HealthState{health.Passing, health.Critical, health.Passing, health.Critical},
			ExpectedOut:   []health.HealthState{health.Passing, health.Critical, health.Passing, health.Unknown},
			MaxBucketSize: 4,
			TestName:      "high throttle",
		},
	}

	for _, test := range tests {
		in := make(chan WatchResult)
		out := throttleChecks(in, test.MaxBucketSize, logging.TestLogger())

		bufferedOut := make(chan WatchResult, len(test.In))
		go func() {
			for val := range out {
				bufferedOut <- val
			}
		}()

		for i, val := range test.In {
			select {
			case in <- WatchResult{
				Id:      "pod_id",
				Service: "service_name",
				Status:  string(val),
			}:
			case <-time.After(1 * time.Second):
				t.Fatalf("timed out writing value %d to throttleChecks input channel in %s", i, test.TestName)
			}
		}

		for i, val := range test.ExpectedOut {
			select {
			case outVal := <-bufferedOut:
				if !healthEquiv(&WatchResult{
					Id:      "pod_id",
					Service: "service_name",
					Status:  string(val),
				}, &outVal) {
					t.Errorf("%s failed: expected value %d to be %s but was %s", test.TestName, i, val, outVal.Status)
				}
			case <-time.After(1 * time.Second):
				t.Fatalf("timed out reading from throttleChecks output channel in %s", test.TestName)
			}
		}
		close(in)

		select {
		case _, ok := <-out:
			if ok {
				t.Fatalf("got an extra value in %s", test.TestName)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("output channel wasnt closed in %s before timeout", test.TestName)
		}
	}
}
