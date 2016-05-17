package replication

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/square/p2/pkg/consultest"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/labels"
)

func TestEnact(t *testing.T) {
	var wg sync.WaitGroup
	replicator, _, f := testReplicatorAndServer(t)
	defer f.Stop()

	// Make the kv store look like preparer is installed on test nodes
	setupPreparers(f)

	replication, errCh, err := replicator.InitializeReplication(false, false)
	if err != nil {
		t.Fatalf("Unable to initialize replication: %s", err)
	}

	doneCh := make(chan struct{})

	failIfErrors(errCh, doneCh, t)
	imitatePreparers(f, doneCh, &wg)

	go func() {
		replication.Enact()
		close(doneCh)
	}()
	select {
	case <-time.After(10 * time.Second):
		t.Fatalf("Replication did not finish within timeout period")
	case <-doneCh:
	}
	wg.Wait()
}

func TestWaitsForHealthy(t *testing.T) {
	var wg sync.WaitGroup
	active := 1
	store, f := makeStore(t)
	defer f.Stop()

	healthChecker, resultsCh := channelHealthChecker(testNodes, t)
	threshold := health.Passing
	replicator, err := NewReplicator(
		basicManifest(),
		basicLogger(),
		testNodes,
		active,
		store,
		labels.NewFakeApplicator(),
		healthChecker,
		threshold,
		testLockMessage,
	)
	if err != nil {
		t.Fatalf("Unable to initialize replicator: %s", err)
	}

	// Make the kv store look like preparer is installed on test nodes
	setupPreparers(f)

	replication, errCh, err := replicator.InitializeReplication(false, false)
	if err != nil {
		t.Fatalf("Unable to initialize replication: %s", err)
	}

	doneCh := make(chan struct{})

	failIfErrors(errCh, doneCh, t)
	imitatePreparers(f, doneCh, &wg)

	// If replication finishes before we mark all nodes as healthy, the
	// test fails. This bool tracks whether replication ending is okay
	okayToFinish := false

	go func() {
		replication.Enact()
		if !okayToFinish {
			t.Fatalf("Replication finished before all nodes were healthy")
		}
		close(doneCh)
	}()

	// Mark first node as unhealthy and the remainder as healthy
	for i, node := range testNodes {
		if i == 0 {
			go func(node string) {
				for x := 0; x < 5; x++ {
					resultsCh[node] <- health.Result{
						ID:     testPodId,
						Status: health.Critical,
					}
				}

				okayToFinish = true
				// Now report as healthy, which means it's okay for replication to end
				resultsCh[node] <- health.Result{
					ID:     testPodId,
					Status: health.Passing,
				}
				close(resultsCh[node])
			}(node)
		} else {
			// Mark the rest of the nodes as healthy constantly and
			// quit once replication is over
			go func(node string) {
				for {
					select {
					case resultsCh[node] <- health.Result{
						ID:     testPodId,
						Status: health.Passing,
					}:
					case <-doneCh:
						close(resultsCh[node])
						return
					}
				}
			}(node)
		}
	}

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Replication took longer than test timeout")
	case <-doneCh:
	}
	wg.Wait()
}

func TestReplicationStopsIfCanceled(t *testing.T) {
	var wg sync.WaitGroup
	active := 1
	store, f := makeStore(t)
	defer f.Stop()

	healthChecker, resultsCh := channelHealthChecker(testNodes, t)
	threshold := health.Passing
	manifest := basicManifest()
	replicator, err := NewReplicator(
		manifest,
		basicLogger(),
		testNodes,
		active,
		store,
		labels.NewFakeApplicator(),
		healthChecker,
		threshold,
		testLockMessage,
	)
	if err != nil {
		t.Fatalf("Unable to initialize replicator: %s", err)
	}

	// Make the kv store look like preparer is installed on test nodes
	setupPreparers(f)

	replication, errCh, err := replicator.InitializeReplication(false, false)
	if err != nil {
		t.Fatalf("Unable to initialize replication: %s", err)
	}

	doneCh := make(chan struct{})

	failIfErrors(errCh, doneCh, t)
	imitatePreparers(f, doneCh, &wg)

	// If replication finishes before we cancel it, test fails. This bool
	// tracks whether replication ending is okay
	okayToFinish := false
	go func() {
		replication.Enact()
		if !okayToFinish {
			t.Fatalf("Replication finished before cancellation occurred")
		}
	}()

	// Report unhealthy for a few iterations; replication should not
	// succeed successfully
	healthFedChannel := make(chan struct{})
	for _, node := range testNodes {
		go func(node string) {
			for i := 0; i < 5; i++ {
				select {
				case resultsCh[node] <- health.Result{
					ID:     testPodId,
					Status: health.Critical,
				}:
				case <-doneCh:
					return
				}
			}
			close(healthFedChannel)
		}(node)
	}
	select {
	case <-healthFedChannel:
	case <-time.After(5 * time.Second):
		t.Fatalf("Test timed out feeding health values, replication probably deadlocked")
	}
	okayToFinish = true
	replication.Cancel()
	close(doneCh)

	// One node should have been updated because active == 1, the other
	// should not have been because health never passed
	realityBytes := f.GetKV(fmt.Sprintf("reality/%s/%s", testNodes[0], testPodId))
	manifestBytes, err := manifest.Marshal()
	if err != nil {
		t.Fatalf("Unable to get bytes from manifest: %s", err)
	}

	if !bytes.Equal(realityBytes, manifestBytes) {
		t.Fatalf("Expected reality for %s to be %s: was %s", testNodes[0], string(manifestBytes), string(realityBytes))
	}

	realityBytes = f.GetKV(fmt.Sprintf("reality/%s/%s", testNodes[1], testPodId))
	if bytes.Equal(realityBytes, manifestBytes) {
		t.Fatalf("The second node shouldn't have been deployed to but it was")
	}
	wg.Wait()
}

func TestStopsIfLockDestroyed(t *testing.T) {
	var wg sync.WaitGroup
	active := 1
	store, f := makeStore(t)
	defer f.Stop()

	healthChecker, resultsCh := channelHealthChecker(testNodes, t)
	threshold := health.Passing
	manifest := basicManifest()

	// Make the kv store look like preparer is installed on test nodes
	setupPreparers(f)

	// Create the replication manually for this test so we can trigger lock
	// renewals on a faster interval (to keep test short)
	errCh := make(chan error)
	replication := &replication{
		active:    active,
		nodes:     testNodes,
		store:     store,
		manifest:  manifest,
		health:    healthChecker,
		threshold: threshold,
		logger:    basicLogger(),
		errCh:     errCh,
		replicationCancelledCh: make(chan struct{}),
		replicationDoneCh:      make(chan struct{}),
		quitCh:                 make(chan struct{}),
	}

	triggerRenewalCh := make(chan time.Time)
	session, renewalErrCh, err := store.NewSession(testLockMessage, triggerRenewalCh)
	if err != nil {
		t.Fatalf("Unable to create initial replication session: %s", err)
	}

	lockPath := kp.ReplicationLockPath(manifest.ID())

	_, err = replication.lock(session, lockPath, false)
	if err != nil {
		t.Fatalf("Unable to perform initial replication lock: %s", err)
	}

	go replication.handleRenewalErrors(session, renewalErrCh)

	doneCh := make(chan struct{})

	go func() {
		select {
		case err := <-errCh:
			if err == nil || !IsFatalError(err) {
				t.Fatalf("Should have seen a fatal lock renewal error before replication finished")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not get expected lock renewal error within timeout")
		}
	}()
	imitatePreparers(f, doneCh, &wg)

	go func() {
		replication.Enact()
		close(doneCh)
	}()

	// Report unhealthy for all nodes so replication cannot finish without
	// interruption
	for _, node := range testNodes {
		go func(node string) {
			for {
				select {
				case resultsCh[node] <- health.Result{
					ID:     testPodId,
					Status: health.Critical,
				}:
				case <-doneCh:
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
		}(node)
	}

	// Wait for the first node to be deployed
	firstNodeDeployed := make(chan struct{})
	manifestBytes, err := manifest.Marshal()
	if err != nil {
		t.Fatalf("Unable to get bytes from manifest: %s", err)
	}
	go func() {
		realityKey := fmt.Sprintf("reality/%s/%s", testNodes[0], testPodId)
		for range time.Tick(10 * time.Millisecond) {
			if bytes.Equal(f.GetKV(realityKey), manifestBytes) {
				close(firstNodeDeployed)
				return
			}
		}
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Took too long for first node to be deployed")
	case <-firstNodeDeployed:
	}

	// Trigger some lock renewals, confirm that replication is still going (doneCh not closed)
	for i := 0; i < 3; i++ {
		select {
		case triggerRenewalCh <- time.Now():
		case <-doneCh:
			t.Fatalf("Replication ended prematurely (lock couldn't be renewed but wasn't destroyed yet)")
		case <-time.After(1 * time.Second):
			t.Fatalf("Test timed out triggering a lock renewal")
		}
	}

	// Destroy lock holder so the next renewal will fail
	lockPath = kp.ReplicationLockPath(manifest.ID())

	_, id, err := store.LockHolder(lockPath)
	if err != nil {
		t.Fatalf("Unable to determine lock holder in order to destroy the lock: %s", err)
	}

	err = store.DestroyLockHolder(id)
	if err != nil {
		t.Fatalf("Unable to destroy lock holder")
	}

	// Trigger one more renewal which should cause replication to stop
	select {
	case triggerRenewalCh <- time.Now():
	case <-time.After(1 * time.Second):
		t.Fatalf("Test timed out triggering a lock renewal")
	case <-doneCh:
		t.Fatalf("Replication ended prematurely")
	}

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Took too long for replication to end after lock cancellation")
	case <-doneCh:
	}

	// One node should have been updated because active == 1, the other
	// should not have been because health never passed
	realityBytes := f.GetKV(fmt.Sprintf("reality/%s/%s", testNodes[0], testPodId))

	if !bytes.Equal(realityBytes, manifestBytes) {
		t.Fatalf("Expected reality for %s to be %s: was %s", testNodes[0], string(manifestBytes), string(realityBytes))
	}

	realityBytes = f.GetKV(fmt.Sprintf("reality/%s/%s", testNodes[1], testPodId))
	if bytes.Equal(realityBytes, manifestBytes) {
		t.Fatalf("The second node shouldn't have been deployed to but it was")
	}
	wg.Wait()
}

// Imitate preparers by copying data from /intent tree to /reality tree
// to simulate deployment
func imitatePreparers(fixture consultest.Fixture, quitCh <-chan struct{}, wg *sync.WaitGroup) {
	// testutil.Server calls t.Fatalf() if a key doesn't exist, so put some
	// dummy data into /intent and /reality for the test pod so we don't
	// accidentally fail tests by merely testing a key
	dummyManifest := []byte("id: wrong_manifest")
	for _, node := range testNodes {
		intentKey := fmt.Sprintf("intent/%s/%s", node, testPodId)
		fixture.SetKV(intentKey, dummyManifest)
		realityKey := fmt.Sprintf("reality/%s/%s", node, testPodId)
		fixture.SetKV(realityKey, dummyManifest)
	}

	// Now do the actual copies
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-quitCh:
				return
			default:
				for _, node := range testNodes {
					intentKey := fmt.Sprintf("intent/%s/%s", node, testPodId)
					realityKey := fmt.Sprintf("reality/%s/%s", node, testPodId)
					intentBytes := fixture.GetKV(intentKey)
					if !bytes.Equal(intentBytes, dummyManifest) {
						fixture.SetKV(realityKey, intentBytes)
					}
				}
			}
		}
	}()
}

func failIfErrors(errCh <-chan error, quitCh <-chan struct{}, t *testing.T) {
	go func() {
		for {
			select {
			case <-quitCh:
				return
			case err := <-errCh:
				if err != nil {
					t.Fatalf("Unexpected error during replication: %s", err)
				}
			}
		}
	}()
}
