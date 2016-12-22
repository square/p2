package replication

import (
	"testing"

	"time"

	"github.com/square/p2/pkg/logging"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker/test"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consultest"
	"github.com/square/p2/pkg/types"
)

// TODO mark as slow test so these don't flake travis
func TestEnactHappyPath(t *testing.T) {
	errCh := make(chan error)
	go proccessErrors(errCh, t)
	defer close(errCh)

	r, podStore := newTestReplication(errCh)
	r.Enact()

	intent, _, err := podStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatalf("Encountered error while fetching intent: %v\n", err)
	}

	if len(intent) != 2 {
		t.Errorf("Expected to have 2 intent records scheduled but got %d.\n%v", len(intent), intent)
	}
	reality, _, err := podStore.AllPods(consul.REALITY_TREE)
	if err != nil {
		t.Fatalf("Encountered error while fetching reality: %v\n", err)
	}
	if len(reality) != 2 {
		t.Errorf("Expected to have 2 reality records scheduled but got %d.\n%v", len(reality), reality)

	}
}

func TestEnactCancellation(t *testing.T) {
	errCh := make(chan error)
	go proccessErrors(errCh, t)
	defer close(errCh)
	r, podStore := newTestReplication(errCh)
	r.active = 1
	r.rateLimiter = time.NewTicker(2 * time.Second) // we need time to cancel

	enactHaltCh := make(chan bool)
	go func() {
		r.Enact()
		enactHaltCh <- true
	}()
	r.Cancel()
	select {
	case <-enactHaltCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for replication to halt")
	}

	intent, _, err := podStore.AllPods(consul.INTENT_TREE)
	if err != nil {
		t.Fatalf("Encountered error while fetching intent: %v\n", err)
	}

	if len(intent) == 2 {
		t.Errorf("Expected Cancel to halt replication promptly but all pods were installed")
	}
	reality, _, err := podStore.AllPods(consul.REALITY_TREE)
	if err != nil {
		t.Fatalf("Encountered error while fetching reality: %v\n", err)
	}

	if len(reality) == 2 {
		t.Errorf("Expected Cancel to halt replication promptly but all pods were installed")
	}
}

// newTestReplication returns a replication and podStore suitable for test
// The errCh is managed and
// podStore is passed via secondary returv value so it can be used to read
// intent,reality. An alternative is to expand the replication.Store type to
// implement AllPods()
func newTestReplication(errCh chan error) (*replication, *consultest.FakePodStore) {
	podID := types.PodID("testPod")
	mb := manifest.NewBuilder()
	mb.SetID(podID)
	nodes := []types.NodeName{"abc123.example.com", "def456.example.com"}
	podStore := consultest.NewFakePodStore(nil, nil)

	logger := logging.TestLogger()
	preparer := consultest.NewFakePreparer(podStore, logger)
	preparer.Enable()

	replicationErrChan := make(chan error)
	replicationCancelledChan := make(chan struct{})
	replicationDoneCh := make(chan struct{})
	enactedCh := make(chan struct{})
	quitCh := make(chan struct{})
	concurrentRealityRequests := make(chan struct{}, 100)
	timeout := 10 * time.Second
	return &replication{
		active:      2,
		nodes:       nodes,
		store:       podStore,
		labeler:     nil,
		manifest:    mb.GetManifest(),
		health:      test.HappyHealthChecker(nodes),
		threshold:   health.Passing,
		logger:      logger,
		rateLimiter: time.NewTicker(1 * time.Millisecond), // TODO fake this out with an interface?
		errCh:       replicationErrChan,
		replicationCancelledCh: replicationCancelledChan,
		replicationDoneCh:      replicationDoneCh,
		enactedCh:              enactedCh,
		quitCh:                 quitCh,
		concurrentRealityRequests: concurrentRealityRequests,
		timeout:                   timeout,
	}, podStore

}

func proccessErrors(errCh chan error, t *testing.T) {
	for err := range errCh {
		t.Errorf("Encountered error: %v", err)
	}
}
