package roll

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/square/p2/pkg/health"
	checkertest "github.com/square/p2/pkg/health/checker/test"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/types"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

func TestWouldBlock(t *testing.T) {
	// in the following cases, rollAlgorithm should return 0
	Assert(t).AreEqual(rollAlgorithm(1, 1, 3, 4), 0, "should do nothing if below minimum")
	Assert(t).AreEqual(rollAlgorithm(1, 1, 2, 4), 0, "should do nothing if at minimum")
	Assert(t).AreEqual(rollAlgorithm(1, 4, 3, 4), 0, "should do nothing if done")

	Assert(t).AreEqual(rollAlgorithm(2, 2, 6, 3), 1, "should schedule difference if minimum must be maintained")
	Assert(t).AreEqual(rollAlgorithm(1, 3, 6, 3), 3, "should schedule remaining if minimum is satisfied by new")
	Assert(t).AreEqual(rollAlgorithm(3, 0, 6, 0), 6, "should schedule remaining if no minimum")
}

func TestShouldContinue(t *testing.T) {
	u := update{Update: fields.Update{MinimumReplicas: 2, DesiredReplicas: 3}}
	oldNodes := rcNodeCounts{Desired: 3, Healthy: 3}
	newNodes := rcNodeCounts{Desired: 0, Healthy: 0}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldContinue, "RU should continue if there is work to be done")
}

func TestShouldContinue2(t *testing.T) {
	u := update{Update: fields.Update{MinimumReplicas: 2, DesiredReplicas: 3}}
	oldNodes := rcNodeCounts{Desired: 1, Healthy: 1}
	newNodes := rcNodeCounts{Desired: 2, Healthy: 2}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldContinue, "RU should continue if there is work to be done")
}

func TestShouldStopIfNodesHealthy(t *testing.T) {
	u := update{Update: fields.Update{MinimumReplicas: 2, DesiredReplicas: 3}}
	oldNodes := rcNodeCounts{Desired: 0, Healthy: 0}
	newNodes := rcNodeCounts{Desired: 3, Healthy: 2}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldTerminate, "RU should terminate if enough nodes are healthy")
}

func TestShouldBlockIfWaitingForHealthyNodes(t *testing.T) {
	u := update{Update: fields.Update{MinimumReplicas: 2, DesiredReplicas: 3}}
	oldNodes := rcNodeCounts{Desired: 0, Healthy: 0}
	newNodes := rcNodeCounts{Desired: 3, Healthy: 1}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldBlock, "RU should block if not enough nodes are healthy")
}

func TestShouldBlockIfWaitingForHealthyCanaryNodes(t *testing.T) {
	u := update{Update: fields.Update{MinimumReplicas: 2, DesiredReplicas: 1}}
	oldNodes := rcNodeCounts{Desired: 2, Healthy: 1}
	newNodes := rcNodeCounts{Desired: 1, Healthy: 0}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldBlock, "RU should block if canary node isn't yet healthy")
}

func TestShouldTerminateIfCanaryFinished(t *testing.T) {
	u := update{Update: fields.Update{MinimumReplicas: 2, DesiredReplicas: 1}}
	oldNodes := rcNodeCounts{Desired: 2, Healthy: 2}
	newNodes := rcNodeCounts{Desired: 1, Healthy: 1}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldTerminate, "RU should terminate if canary node is healthy")
}

func TestWouldWorkOn(t *testing.T) {
	fakeLabels := labels.NewFakeApplicator()
	fakeLabels.SetLabel(labels.RC, "abc-123", "color", "red")
	fakeLabels.SetLabel(labels.RC, "def-456", "color", "blue")

	f := &Farm{
		labeler:    fakeLabels,
		rcSelector: klabels.Everything().Add("color", klabels.EqualsOperator, []string{"red"}),
	}

	workOn, err := f.shouldWorkOn(rc_fields.ID("abc-123"))
	Assert(t).IsNil(err, "should not have erred on abc-123")
	Assert(t).IsTrue(workOn, "should have worked on abc-123, but didn't")

	dontWorkOn, err := f.shouldWorkOn(rc_fields.ID("def-456"))
	Assert(t).IsNil(err, "should not have erred on def-456")
	Assert(t).IsFalse(dontWorkOn, "should not have worked on def-456, but did")

	dontWorkOn, err = f.shouldWorkOn(rc_fields.ID("987-cba"))
	Assert(t).IsNil(err, "should not have erred on 987-cba")
	Assert(t).IsFalse(dontWorkOn, "should not have worked on 987-cba, but did")

	f.rcSelector = klabels.Everything()

	workOn, err = f.shouldWorkOn(rc_fields.ID("def-456"))
	Assert(t).IsNil(err, "should not have erred on def-456")
	Assert(t).IsTrue(workOn, "should have worked on def-456, but didn't")
}

func TestLockRCs(t *testing.T) {
	fakeStore := kptest.NewFakePodStore(nil, nil)
	session, _, err := fakeStore.NewSession("fake rc lock session", nil)
	Assert(t).IsNil(err, "Should not have erred getting fake session")

	update := NewUpdate(fields.Update{
		NewRC: rc_fields.ID("new_rc"),
		OldRC: rc_fields.ID("old_rc"),
	},
		nil,
		rcstore.NewFake(),
		nil,
		nil,
		nil,
		logging.DefaultLogger,
		session,
	).(*update)
	err = update.lockRCs(make(<-chan struct{}))
	Assert(t).IsNil(err, "should not have erred locking RCs")
	Assert(t).IsNotNil(update.newRCUnlocker, "should have kp.Unlocker for unlocking new rc")
	Assert(t).IsNotNil(update.oldRCUnlocker, "should have kp.Unlocker for unlocking old rc")
}

func TestSimulateRollingUpgradeDisable(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < 20000; i++ {
		SimulateRollingUpgradeDisable(t, false, false)
	}
}

// this fuzzer tests the rolling upgrade rollAlgorithm in an environment where new
// pods replace old pods (eg hoist artifacts). it creates an imaginary list of
// nodes, some of which may have old pods on them, and attempts to run a rolling
// upgrade across this list.
//
// the flags can constrain the fuzzer to certain circumstances. specifically:
// - if full=true, then every node will start off with the old pod scheduled on
//   it, and the new pod will eventually be scheduled on every node. this
//   simulates a common use case where you have a fixed list of nodes, all of
//   which are currently on the old pod, and all of them should transition to
//   the new pod
// - normally the simulation can create a nonzero number of new pods in the
//   starting world. pass nonew=true to disable this behavior, ie the new RC
//   will always start with zero pods. note that full=true implies nonew=true,
//   since if every node has an old pod there are clearly no new pods.
func SimulateRollingUpgradeDisable(t *testing.T, full, nonew bool) {
	// generate a slice of "nodes": each element represents a single node
	// 0 = node is empty, 1 = node has new pod, -1 = node has old pod
	nodes := make([]int, rand.Intn(20)+1)
	shuffledIndices := rand.Perm(len(nodes))

	// out of those n nodes, we will choose some of them at random, and assume
	// that the old pod is already scheduled there
	oldCount := rand.Intn(len(nodes) + 1)
	if full {
		oldCount = len(nodes)
	}
	for _, index := range shuffledIndices[:oldCount] {
		nodes[index] = -1
	}

	// seed in some new nodes as well
	newCount := rand.Intn(len(nodes) - oldCount + 1)
	if nonew {
		newCount = 0
	}
	for _, index := range shuffledIndices[len(nodes)-newCount:] {
		nodes[index] = 1
	}

	// minimum ranges over [0, oldCount+newCount-1]
	// there must always be at least one free old node that can be unscheduled
	// when the new one is added
	minimum := 0
	if oldCount > 0 {
		minimum = rand.Intn(oldCount + newCount)
	}

	// target ranges over [max(minimum, newCount), len(nodes)]
	target := rand.Intn(len(nodes)-minimum+1) + minimum
	if newCount > minimum {
		target = rand.Intn(len(nodes)-newCount+1) + newCount
	}
	if full {
		target = len(nodes)
	}

	for {
		// count how many new and old pods there are, and what indices can take
		// another new pod
		var old, new, eligible []int
		for index, node := range nodes {
			if node > 0 {
				new = append(new, index)
			} else {
				eligible = append(eligible, index)
			}
			if node < 0 {
				old = append(old, index)
			}
		}
		t.Logf("State: %v (total %d, old %d, new %d, want %d, need %d)\n", nodes, len(nodes), len(old), len(new), target, minimum)

		// validate test conditions
		Assert(t).IsTrue(len(old)+len(new) >= minimum, fmt.Sprintf("went below %d minimum nodes (nodes %v)\n", minimum, nodes))
		Assert(t).IsTrue(len(new) <= target, fmt.Sprintf("went above %d target nodes (nodes %v)\n", target, nodes))
		if len(new) == target {
			Assert(t).AreEqual(rollAlgorithm(len(old), len(new), target, minimum), 0, "update should be done")
			t.Logf("Simulation complete\n\n")
			break
		}

		// calculate the next update
		nextUpdate := rollAlgorithm(len(old), len(new), target, minimum)
		t.Logf("Scheduling %d new out of %v eligible\n", nextUpdate, eligible)
		Assert(t).AreNotEqual(nextUpdate, 0, "got noop update, would never terminate")
		// choose nodes from the eligible list, randomly, and put the new pod
		// on them
		for _, index := range rand.Perm(len(eligible))[:nextUpdate] {
			nodes[eligible[index]] = 1
		}
	}
}

func podWithIDAndPort(id string, port int) pods.Manifest {
	builder := pods.NewManifestBuilder()
	builder.SetID(types.PodID(id))
	builder.SetStatusPort(port)
	return builder.GetManifest()
}

func assignManifestsToNodes(
	podID types.PodID,
	nodes map[string]bool,
	pods map[kptest.FakePodStoreKey]pods.Manifest,
	ifCurrent, ifNotCurrent pods.Manifest,
) {
	for node, current := range nodes {
		key := kptest.FakePodStoreKeyFor(kp.REALITY_TREE, node, podID)
		if current {
			pods[key] = ifCurrent
		} else {
			pods[key] = ifNotCurrent
		}
	}
}

func createRC(
	rcs rcstore.Store,
	applicator labels.Applicator,
	manifest pods.Manifest,
	desired int,
	nodes map[string]bool,
) (rc_fields.RC, error) {
	created, err := rcs.Create(manifest, nil, nil)
	if err != nil {
		return rc_fields.RC{}, fmt.Errorf("Error creating RC: %s", err)
	}

	podID := string(manifest.ID())

	for node := range nodes {
		if err = applicator.SetLabel(labels.POD, node+"/"+podID, rc.RCIDLabel, string(created.ID)); err != nil {
			return rc_fields.RC{}, fmt.Errorf("Error applying RC ID label: %s", err)
		}
	}

	return created, rcs.SetDesiredReplicas(created.ID, desired)
}

func updateWithHealth(t *testing.T,
	desiredOld, desiredNew int,
	oldNodes, newNodes map[string]bool,
	checks map[string]health.Result,
) (update, pods.Manifest, pods.Manifest) {
	podID := "mypod"

	oldManifest := podWithIDAndPort(podID, 9001)
	newManifest := podWithIDAndPort(podID, 9002)

	podMap := map[kptest.FakePodStoreKey]pods.Manifest{}
	assignManifestsToNodes(types.PodID(podID), oldNodes, podMap, oldManifest, newManifest)
	assignManifestsToNodes(types.PodID(podID), newNodes, podMap, newManifest, oldManifest)
	kps := kptest.NewFakePodStore(podMap, nil)

	rcs := rcstore.NewFake()
	applicator := labels.NewFakeApplicator()

	oldRC, err := createRC(rcs, applicator, oldManifest, desiredOld, oldNodes)
	Assert(t).IsNil(err, "expected no error setting up old RC")

	newRC, err := createRC(rcs, applicator, newManifest, desiredNew, newNodes)
	Assert(t).IsNil(err, "expected no error setting up new RC")

	return update{
		kps:     kps,
		rcs:     rcs,
		hcheck:  checkertest.NewSingleService(string(podID), checks),
		labeler: applicator,
		Update: fields.Update{
			OldRC: oldRC.ID,
			NewRC: newRC.ID,
		},
	}, oldManifest, newManifest
}

func updateWithUniformHealth(t *testing.T, numNodes int, status health.HealthState) (update, map[string]health.Result) {
	current := map[string]bool{}
	checks := map[string]health.Result{}

	for i := 0; i < numNodes; i++ {
		node := fmt.Sprintf("node%d", i)
		current[node] = true
		checks[node] = health.Result{Status: status}
	}

	upd, _, _ := updateWithHealth(t, numNodes, 0, current, nil, nil)
	return upd, checks
}

func TestCountHealthyNormal(t *testing.T) {
	upd, checks := updateWithUniformHealth(t, 3, health.Passing)
	counts, err := upd.countHealthy(upd.OldRC, checks)
	Assert(t).IsNil(err, "expected no error counting health")
	expected := rcNodeCounts{
		Desired: 3,
		Current: 3,
		Real:    3,
		Healthy: 3,
	}
	Assert(t).AreEqual(counts, expected, "incorrect health counts")
}

func TestShouldRollInitial(t *testing.T) {
	checks := map[string]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
	}
	upd, _, manifest := updateWithHealth(t, 3, 0, map[string]bool{
		"node1": true,
		"node2": true,
		"node3": true,
	}, nil, checks)
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, err := upd.shouldRollAfterDelay(rc_fields.RC{ID: upd.NewRC, Manifest: manifest})
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(roll, 1, "expected to only roll one node")
}

func TestShouldRollMidwayUnhealthy(t *testing.T) {
	checks := map[string]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Critical},
	}
	upd, _, manifest := updateWithHealth(t, 2, 1, map[string]bool{
		"node1": true,
		"node2": true,
	}, map[string]bool{
		"node3": true,
	}, checks)
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, _ := upd.shouldRollAfterDelay(rc_fields.RC{ID: upd.NewRC, Manifest: manifest})
	Assert(t).AreEqual(roll, 0, "expected to roll no nodes")
}

func TestShouldRollMidwayUnhealthyMigration(t *testing.T) {
	checks := map[string]health.Result{
		"node3": {Status: health.Critical},
	}
	upd, _, manifest := updateWithHealth(t, 2, 1, nil, map[string]bool{
		"node3": true,
	}, checks)
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, _ := upd.shouldRollAfterDelay(rc_fields.RC{ID: upd.NewRC, Manifest: manifest})
	Assert(t).AreEqual(roll, 0, "expected to roll no nodes")
}

func TestShouldRollMidwayHealthy(t *testing.T) {
	checks := map[string]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
	}
	upd, _, manifest := updateWithHealth(t, 2, 1, map[string]bool{
		"node1": true,
		"node2": true,
	}, map[string]bool{
		"node3": true,
	}, checks)
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, err := upd.shouldRollAfterDelay(rc_fields.RC{ID: upd.NewRC, Manifest: manifest})
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(roll, 1, "expected to roll one node")
}
