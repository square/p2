package replication

import (
	"fmt"
	"path"
	"regexp"
	"testing"
	"time"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/rc"
)

func TestInitializeReplication(t *testing.T) {
	replicator, store, f := testReplicatorAndServer(t)
	defer f.Stop()

	// Make the kv store look like preparer is installed on test nodes
	setupPreparers(f)

	// err being nil ensures that checking preparers and locking the hosts
	// succeeded
	replication, _, err := replicator.InitializeReplication(false, false)
	if err != nil {
		t.Fatalf("Error initializing replication: %s", err)
	}
	defer replication.Cancel()

	// Confirm that the appropriate kv keys have been locked
	for _, node := range testNodes {
		lockPath, err := kp.PodLockPath(kp.INTENT_TREE, node, testPodId)
		if err != nil {
			t.Fatalf("Unable to compute pod lock path: %s", err)
		}

		lockHolder, _, err := store.LockHolder(lockPath)
		if err != nil {
			t.Fatalf("Unexpected error checking for lock holder: %s", err)
		}

		if lockHolder != testLockMessage {
			t.Errorf("Expected lock holder for key '%s' to be '%s', was '%s'", lockPath, testLockMessage, lockHolder)
		}
	}
}

func TestInitializeReplicationFailsIfNoPreparers(t *testing.T) {
	replicator, _, f := testReplicatorAndServer(t)
	defer f.Stop()

	// We expect an error here because the reality keys for the preparer
	// have no data, which in production would mean that the preparer is
	// not installed on the hosts being replicated to
	_, _, testErr := replicator.InitializeReplication(false, false)
	if testErr == nil {
		t.Fatalf("Expected error due to preparer not existing in reality, but no error occurred")
	}

	matched, err := regexp.MatchString(fmt.Sprintf("verify %s state", preparer.POD_ID), testErr.Error())
	if err != nil {
		t.Fatalf("Unable to compare error message to expected string")
	}

	if !matched {
		t.Fatalf("Expected error message to be related to preparer state, but was %s", testErr.Error())
	}
}

func TestInitializeReplicationFailsIfLockExists(t *testing.T) {
	replicator, store, f := testReplicatorAndServer(t)
	defer f.Stop()

	// This makes it look like the preparers are installed on the hosts
	// we're deploying to
	for _, node := range testNodes {
		key := fmt.Sprintf("reality/%s/p2-preparer", node)
		f.SetKV(key, []byte(testPreparerManifest))
	}

	// Claim a lock on a host and verify that InitializeReplication fails
	session, _, err := store.NewSession("competing session", nil)
	if err != nil {
		t.Fatalf("Unable to set up competing session: %s", err)
	}
	defer session.Destroy()
	lockPath, err := kp.PodLockPath(kp.INTENT_TREE, testNodes[0], testPodId)
	if err != nil {
		t.Fatalf("Unable to compute pod lock path: %s", err)
	}

	_, err = session.Lock(lockPath)
	if err != nil {
		t.Fatalf("Unable to acquire lock with competing session: %s", err)
	}

	_, _, testErr := replicator.InitializeReplication(false, false)
	if testErr == nil {
		t.Fatalf("Expected error due to competing lock, but no error occurred")
	}

	matched, err := regexp.MatchString("already held", testErr.Error())
	if err != nil {
		t.Fatalf("Unable to compare error message to expected string")
	}

	if !matched {
		t.Fatalf("Expected error message to be related to a lock already being held, but was %s", testErr.Error())
	}
}

func TestInitializeReplicationReleasesLocks(t *testing.T) {
	replicator, store, f := testReplicatorAndServer(t)
	defer f.Stop()
	setupPreparers(f)

	// Claim a lock on test node 1
	session, _, err := store.NewSession("competing session", nil)
	if err != nil {
		t.Fatalf("Unable to set up competing session: %s", err)
	}
	defer session.Destroy()
	lockPath1, err := kp.PodLockPath(kp.INTENT_TREE, testNodes[1], testPodId)
	if err != nil {
		t.Fatalf("Unable to compute pod lock path: %s", err)
	}
	_, err = session.Lock(lockPath1)
	if err != nil {
		t.Fatalf("Unable to set up competing lock: %s", err)
	}

	// Replication should start acquiring locks but fail to complete because of the lock
	// on test node 1
	_, _, testErr := replicator.InitializeReplication(false, false)
	if testErr == nil {
		t.Fatalf("Expected error due to competing lock, but no error occurred")
	}

	// After a failed attempt at replication (+ Consul lock delay), test node 0 should
	// still be lockable
	time.Sleep(10 * time.Millisecond)
	lockPath0, err := kp.PodLockPath(kp.INTENT_TREE, testNodes[0], testPodId)
	if err != nil {
		t.Fatal(err)
	}
	_, err = session.Lock(lockPath0)
	if err != nil {
		t.Fatalf("unable to acquire lock. replicator probably didn't release its lock on error: %v", err)
	}
}

func TestInitializeReplicationCanOverrideLocks(t *testing.T) {
	replicator, store, f := testReplicatorAndServer(t)
	defer f.Stop()

	// This makes it look like the preparers are installed on the hosts
	// we're deploying to
	for _, node := range testNodes {
		key := fmt.Sprintf("reality/%s/p2-preparer", node)
		f.SetKV(key, []byte(testPreparerManifest))
	}

	// Claim a lock on a host and verify that InitializeReplication fails
	session, _, err := store.NewSession("competing session", nil)
	if err != nil {
		t.Fatalf("Unable to set up competing session: %s", err)
	}
	defer session.Destroy()
	lockPath, err := kp.PodLockPath(kp.INTENT_TREE, testNodes[0], testPodId)
	if err != nil {
		t.Fatalf("Unable to compute pod lock path: %s", err)
	}

	_, err = session.Lock(lockPath)
	if err != nil {
		t.Fatalf("Unable to set up competing lock: %s", err)
	}

	replication, _, err := replicator.InitializeReplication(true, false)
	if err != nil {
		t.Fatalf("Expected InitializeReplication to override competing lock, but error occurred: %s", err)
	}
	replication.Cancel()
}

func TestInitializeReplicationWithManaged(t *testing.T) {
	replicator, _, f := testReplicatorAndServer(t)
	defer f.Stop()
	setupPreparers(f)

	// Make one node appear to be managed by a replication controller
	err := labels.NewConsulApplicator(f.Client, 1).SetLabel(
		labels.POD,
		path.Join(testNodes[0], testPodId),
		rc.RCIDLabel,
		"controller GUID ignored",
	)
	if err != nil {
		t.Fatal(err)
	}

	// Replication should fail because one node is managed
	replication, _, err := replicator.InitializeReplication(false, false)
	if err == nil {
		t.Errorf("replication did not reject managed node")
		replication.Cancel()
	}

	// Replication should succeed when explicitly ignoring the controllers
	time.Sleep(50 * time.Millisecond)
	replication, _, err = replicator.InitializeReplication(false, true)
	if err != nil {
		t.Fatal("replication could not ignore managed node:", err)
	}
	replication.Cancel()
}
