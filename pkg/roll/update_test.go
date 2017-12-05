// +build !race

package roll

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/square/p2/pkg/health"
	checkertest "github.com/square/p2/pkg/health/checker/test"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/rc"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/types"

	. "github.com/anthonybishopric/gotcha"
	klabels "k8s.io/kubernetes/pkg/labels"
)

func uniformRollAlgorithm(t *testing.T, old, new, want, need int) int {
	// For these, oldDesired + newDesired should == targetDesired.
	// We'll just craft numbers that meet this requirement.
	remove, add := rollAlgorithm(old, new, want-new, new, want, need)

	Assert(t).AreEqual(remove, add, "expected nodes removed and nodes added to be equal")
	return add
}

func TestWouldBlock(t *testing.T) {
	// in the following cases, rollAlgorithm should return 0
	Assert(t).AreEqual(uniformRollAlgorithm(t, 1, 1, 3, 4), 0, "should do nothing if below minimum")
	Assert(t).AreEqual(uniformRollAlgorithm(t, 1, 1, 2, 4), 0, "should do nothing if at minimum")
	Assert(t).AreEqual(uniformRollAlgorithm(t, 1, 4, 3, 4), 0, "should do nothing if done")

	Assert(t).AreEqual(uniformRollAlgorithm(t, 2, 2, 6, 3), 1, "should schedule difference if minimum must be maintained")
	Assert(t).AreEqual(uniformRollAlgorithm(t, 1, 3, 6, 3), 3, "should schedule remaining if minimum is satisfied by new")
	Assert(t).AreEqual(uniformRollAlgorithm(t, 3, 0, 6, 0), 6, "should schedule remaining if no minimum")
}

func assertRollAlgorithmResults(t *testing.T, old, new, final, need, remove, add int, message string) {
	// For these, oldDesired == oldHealthy, newDesired == newHealthy.
	// This may allow oldDesired + newDesired < final.
	gotRemove, gotAdd := rollAlgorithm(old, new, old, new, final, need)
	Assert(t).AreEqual(gotRemove, remove, "removed nodes incorrect: "+message)
	Assert(t).AreEqual(gotAdd, add, "added nodes incorrect: "+message)
}

func TestRollAlgorithmDoesNotExceed(t *testing.T) {
	// newHealthy < newDesired, and newHealthy >= minHealthy.
	// In this case, we schedule the remaining nodes.
	// We want to ensure that remaining == targetDesired - newDesired
	// instead of targetDesired - newHealthy
	gotRemove, gotAdd := rollAlgorithm(1, 1, 1, 2, 3, 1)
	Assert(t).AreEqual(gotRemove, 1, "expected only one node to be removed")
	Assert(t).AreEqual(gotAdd, 1, "expected only one node to be added")
}

func TestRollAlgorithmIncreases(t *testing.T) {
	assertRollAlgorithmResults(t, 0, 0, 3, 2, 0, 1, "should schedule difference if increasing capacity from zero")
	assertRollAlgorithmResults(t, 0, 1, 3, 2, 0, 1, "should schedule difference if partway through increasing capacity from zero")
	assertRollAlgorithmResults(t, 0, 2, 3, 2, 0, 1, "should schedule remaining if increasing capacity from zero and new nodes satisfy minimum")

	assertRollAlgorithmResults(t, 0, 0, 3, 0, 0, 3, "should schedule all if increasing capacity from zero with no minimum")

	assertRollAlgorithmResults(t, 3, 0, 4, 2, 1, 2, "should schedule difference if increasing capacity with existing nodes")
	assertRollAlgorithmResults(t, 3, 0, 4, 3, 0, 1, "should schedule only new node if increasing capacity with existing nodes and no headroom")
}

func TestShouldContinue(t *testing.T) {
	u := update{Update: fields.Update{DesiredReplicas: 3}}
	oldNodes := rcNodeCounts{Desired: 3, Current: 3}
	newNodes := rcNodeCounts{Desired: 0, Current: 0}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldContinue, "RU should continue if there is work to be done")
}

func TestShouldContinue2(t *testing.T) {
	u := update{Update: fields.Update{DesiredReplicas: 3}}
	oldNodes := rcNodeCounts{Desired: 1, Current: 1}
	newNodes := rcNodeCounts{Desired: 2, Current: 2}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldContinue, "RU should continue if there is work to be done")
}

func TestShouldStopIfNodesCurrent(t *testing.T) {
	u := update{Update: fields.Update{DesiredReplicas: 3}}
	oldNodes := rcNodeCounts{Desired: 0, Current: 0}
	newNodes := rcNodeCounts{Desired: 3, Current: 3}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldTerminate, "RU should terminate if enough nodes are current")
}

func TestShouldBlockIfWaitingForCurrentNodes(t *testing.T) {
	u := update{Update: fields.Update{DesiredReplicas: 3}}
	oldNodes := rcNodeCounts{Desired: 0, Current: 0}
	newNodes := rcNodeCounts{Desired: 3, Current: 2}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldBlock, "RU should block if not enough nodes are current")
}

func TestShouldBlockIfWaitingForCurrentCanaryNodes(t *testing.T) {
	u := update{Update: fields.Update{DesiredReplicas: 1}}
	oldNodes := rcNodeCounts{Desired: 2, Current: 2}
	newNodes := rcNodeCounts{Desired: 1, Current: 0}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldBlock, "RU should block if canary node isn't yet current")
}

func TestShouldTerminateIfCanaryFinished(t *testing.T) {
	u := update{Update: fields.Update{DesiredReplicas: 1}}
	oldNodes := rcNodeCounts{Desired: 2, Current: 2}
	newNodes := rcNodeCounts{Desired: 1, Current: 1}
	Assert(t).AreEqual(u.shouldStop(oldNodes, newNodes), ruShouldTerminate, "RU should terminate if canary node is current")
}

func TestRollAlgorithmParams(t *testing.T) {
	u := &update{Update: fields.Update{
		MinimumReplicas: 4096,
		DesiredReplicas: 8192,
	}}
	oldHealth := rcNodeCounts{
		Current:   1,
		Real:      2,
		Healthy:   4,
		Unhealthy: 8,
		Unknown:   16,
		Desired:   32,
	}
	newHealth := rcNodeCounts{
		Current:   64,
		Real:      128,
		Healthy:   256,
		Unhealthy: 512,
		Unknown:   1024,
		Desired:   2048,
	}
	old, new, oldDesired, newDesired, targetDesired, minHealthy := u.rollAlgorithmParams(oldHealth, newHealth)
	Assert(t).AreEqual(old, 4, "incorrect old healthy param")
	Assert(t).AreEqual(new, 256, "incorrect new healthy param")
	Assert(t).AreEqual(oldDesired, 32, "incorrect old desired param")
	Assert(t).AreEqual(newDesired, 2048, "incorrect new desired param")
	Assert(t).AreEqual(targetDesired, 8192, "incorrect target desired param")
	Assert(t).AreEqual(minHealthy, 4096, "incorrect min healthy param")
}

func TestRollAlgorithmParamsFewerDesiredThanHealthy(t *testing.T) {
	u := &update{}
	oldHealth := rcNodeCounts{Healthy: 4, Desired: 3}
	newHealth := rcNodeCounts{}
	old, _, _, _, _, _ := u.rollAlgorithmParams(oldHealth, newHealth)
	Assert(t).AreEqual(old, 3, "incorrect old healthy param (expected to be old desired, since it's smaller than old healthy)")
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

func TestSimulateRollingUpgrade(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < 20000; i++ {
		SimulateRollingUpgrade(t, false, false, false)
	}
}

func TestSimulateRollingUpgradeStrictRemove(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < 20000; i++ {
		SimulateRollingUpgrade(t, false, false, true)
	}
}

// this fuzzer tests the rolling upgrade rollAlgorithm. It creates a list of
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
// - if strictRemove=true:
//   - the number of nodes removed each iteration will be exactly nodesToRemove
//     as indicated by rollAlgorithm
//   - the desired number of old nodes will be the number of actual old nodes
//   - this is useful for testing immutable deployments
// - if strictRemove=false:
//   - old nodes are never explicitly removed (nodesToRemove isn't respected)
//   - old nodes may be removed if a new pod is randomly scheduled on them
//   - therefore, the desired number of old nodes is unreliable
//   - this is useful for testing mutable deployments
//     (where new pods may replace old pods)
func SimulateRollingUpgrade(t *testing.T, full, nonew, strictRemove bool) {
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
			} else if node < 0 {
				old = append(old, index)
				// All old nodes are eligible if removing is non-strict.
				// If removing is strict, only empty nodes are initially eligible.
				// Nodes from which the old pod gets removed also become eligible.
				if !strictRemove {
					eligible = append(eligible, index)
				}
			} else {
				eligible = append(eligible, index)
			}
		}
		t.Logf("State: %v (total %d, old %d, new %d, want %d, need %d)\n", nodes, len(nodes), len(old), len(new), target, minimum)

		// validate test conditions
		Assert(t).IsTrue(len(old)+len(new) >= minimum, fmt.Sprintf("went below %d minimum nodes (nodes %v)\n", minimum, nodes))
		Assert(t).IsTrue(len(new) <= target, fmt.Sprintf("went above %d target nodes (nodes %v)\n", target, nodes))
		if len(new) == target {
			nextRemove, nextAdd := rollAlgorithm(len(old), len(new), len(old), target, target, minimum)
			Assert(t).AreEqual(nextRemove, 0, "update should be done, should remove nothing")
			Assert(t).AreEqual(nextAdd, 0, "update should be done, should add nothing")
			t.Logf("Simulation complete\n\n")
			break
		}

		// calculate the next update
		oldDesired := target - len(new)
		if strictRemove {
			oldDesired = len(old)
		}
		nextRemove, nextAdd := rollAlgorithm(len(old), len(new), oldDesired, len(new), target, minimum)

		if !strictRemove {
			Assert(t).AreEqual(nextRemove, nextAdd, "got asymmetric update, not expected for this fuzz test")
		}

		t.Logf("Scheduling %d new out of %v eligible\n", nextAdd, eligible)
		Assert(t).AreNotEqual(nextAdd, 0, "got noop update, would never terminate")

		if strictRemove {
			// choose nodes from the old list, randomly, and remove the pod from them.
			t.Logf("Unscheduling %d out of %v old\n", nextRemove, old)
			for _, index := range rand.Perm(len(old))[:nextRemove] {
				nodes[old[index]] = 0
				eligible = append(eligible, old[index])
			}
			t.Logf("Eligible nodes now %v\n", eligible)
		}

		// choose nodes from the eligible list, randomly, and put the new pod
		// on them
		for _, index := range rand.Perm(len(eligible))[:nextAdd] {
			nodes[eligible[index]] = 1
		}
	}
}

func podWithIDAndPort(id string, port int) manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID(types.PodID(id))
	builder.SetStatusPort(port)
	return builder.GetManifest()
}

type testPodStore interface {
	SetPod(podPrefix consul.PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
}

func assignManifestsToNodes(
	t *testing.T,
	nodes map[types.NodeName]bool,
	podStore testPodStore,
	ifCurrent manifest.Manifest,
	ifNotCurrent manifest.Manifest,
) {
	var err error
	for node, current := range nodes {
		if current {
			_, err = podStore.SetPod(consul.REALITY_TREE, node, ifCurrent)
		} else {
			_, err = podStore.SetPod(consul.REALITY_TREE, node, ifNotCurrent)
		}

		if err != nil {
			t.Fatal(err)
		}
	}
}

type testRCCreator interface {
	Create(
		manifest manifest.Manifest,
		nodeSelector klabels.Selector,
		availabilityZone pc_fields.AvailabilityZone,
		clusterName pc_fields.ClusterName,
		podLabels klabels.Set,
		additionalLabels klabels.Set,
		allocationStrategy rc_fields.Strategy,
	) (rc_fields.RC, error)
	SetDesiredReplicas(id rc_fields.ID, n int) error
}

func createRC(
	rcs testRCCreator,
	applicator labels.Applicator,
	manifest manifest.Manifest,
	desired int,
	nodes map[types.NodeName]bool,
) (rc_fields.RC, error) {
	created, err := rcs.Create(manifest, nil, "some_az", "some_cn", nil, nil, "some_strategy")
	if err != nil {
		return rc_fields.RC{}, fmt.Errorf("Error creating RC: %s", err)
	}

	podID := string(manifest.ID())

	for node := range nodes {
		if err = applicator.SetLabel(labels.POD, node.String()+"/"+podID, rc.RCIDLabel, string(created.ID)); err != nil {
			return rc_fields.RC{}, fmt.Errorf("Error applying RC ID label: %s", err)
		}
	}

	return created, rcs.SetDesiredReplicas(created.ID, desired)
}

func updateWithHealth(t *testing.T,
	desiredOld, desiredNew int,
	oldNodes, newNodes map[types.NodeName]bool,
	checks map[types.NodeName]health.Result,
) (update, manifest.Manifest, manifest.Manifest, testReplicationControllerWatcher, func()) {
	podID := "mypod"

	oldManifest := podWithIDAndPort(podID, 9001)
	newManifest := podWithIDAndPort(podID, 9002)

	fixture := consulutil.NewFixture(t)
	podStore := consul.NewConsulStore(fixture.Client)
	assignManifestsToNodes(t, oldNodes, podStore, oldManifest, newManifest)
	assignManifestsToNodes(t, newNodes, podStore, newManifest, oldManifest)

	applicator := labels.NewConsulApplicator(fixture.Client, 0, 0)

	// seed the label store with some labels so we don't trigger failsafes
	err := applicator.SetLabel(labels.NODE, "node1", "key1", "val1")
	if err != nil {
		t.Fatal(err)
	}
	err = applicator.SetLabel(labels.POD, "pod1", "key1", "val1")
	if err != nil {
		t.Fatal(err)
	}
	rcs := rcstore.NewConsul(fixture.Client, applicator, 0)

	oldRC, err := createRC(rcs, applicator, oldManifest, desiredOld, oldNodes)
	Assert(t).IsNil(err, "expected no error setting up old RC")

	newRC, err := createRC(rcs, applicator, newManifest, desiredNew, newNodes)
	Assert(t).IsNil(err, "expected no error setting up new RC")

	return update{
		consuls: podStore,
		txner:   fixture.Client.KV(),
		rcStore: rcs,
		hcheck:  checkertest.NewSingleService(podID, checks),
		labeler: applicator,
		logger:  logging.TestLogger(),
		Update: fields.Update{
			OldRC: oldRC.ID,
			NewRC: newRC.ID,
		},
	}, oldManifest, newManifest, rcs, fixture.Stop
}

func updateWithUniformHealth(t *testing.T, numNodes int, status health.HealthState) (update, map[types.NodeName]health.Result, func()) {
	current := map[types.NodeName]bool{}
	checks := map[types.NodeName]health.Result{}

	for i := 0; i < numNodes; i++ {
		node := types.NodeName(fmt.Sprintf("node%d", i))
		current[node] = true
		checks[node] = health.Result{Status: status}
	}

	upd, _, _, _, f := updateWithHealth(t, numNodes, 0, current, nil, nil)
	return upd, checks, f
}

func TestCountHealthyNormal(t *testing.T) {
	upd, checks, f := updateWithUniformHealth(t, 3, health.Passing)
	defer f()
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

func TestCountHealthAllUnhealthy(t *testing.T) {
	upd, checks, f := updateWithUniformHealth(t, 3, health.Critical)
	defer f()
	counts, err := upd.countHealthy(upd.OldRC, checks)
	Assert(t).IsNil(err, "expected no error counting health")
	expected := rcNodeCounts{
		Desired:   3,
		Current:   3,
		Real:      3,
		Unhealthy: 3,
	}
	Assert(t).AreEqual(counts, expected, "incorrect health counts")
}

func TestCountHealthAllExplicitUnknown(t *testing.T) {
	upd, checks, f := updateWithUniformHealth(t, 3, health.Unknown)
	defer f()
	counts, err := upd.countHealthy(upd.OldRC, checks)
	Assert(t).IsNil(err, "expected no error counting health")
	expected := rcNodeCounts{
		Desired: 3,
		Current: 3,
		Real:    3,
		Unknown: 3,
	}
	Assert(t).AreEqual(counts, expected, "incorrect health counts")
}

func TestCountHealthAllImplicitUnknown(t *testing.T) {
	upd, _, f := updateWithUniformHealth(t, 3, health.Unknown)
	defer f()
	counts, err := upd.countHealthy(upd.OldRC, nil)
	Assert(t).IsNil(err, "expected no error counting health")
	expected := rcNodeCounts{
		Desired: 3,
		Current: 3,
		Real:    3,
		Unknown: 3,
	}
	Assert(t).AreEqual(counts, expected, "incorrect health counts")
}

func TestCountHealthNonReal(t *testing.T) {
	upd, _, _, _, f := updateWithHealth(t, 3, 0, map[types.NodeName]bool{"node1": true, "node2": true, "node3": false}, nil, nil)
	defer f()
	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Critical},
	}
	counts, err := upd.countHealthy(upd.OldRC, checks)
	Assert(t).IsNil(err, "expected no error counting health")
	expected := rcNodeCounts{
		Desired: 3,
		Current: 3,
		Real:    2,
		Healthy: 2,
		Unknown: 0,
	}
	Assert(t).AreEqual(counts, expected, "incorrect health counts")
}

func TestCountHealthNonCurrent(t *testing.T) {
	upd, _, _, _, f := updateWithHealth(t, 3, 0, map[types.NodeName]bool{}, nil, nil)
	defer f()
	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Critical},
	}
	counts, err := upd.countHealthy(upd.OldRC, checks)
	Assert(t).IsNil(err, "expected no error counting health")
	expected := rcNodeCounts{
		Desired: 3,
		Unknown: 3,
	}
	Assert(t).AreEqual(counts, expected, "incorrect health counts")
}

func (u *update) uniformShouldRollAfterDelay(t *testing.T, podID types.PodID) (int, error) {
	remove, add, err := u.shouldRollAfterDelay(podID)
	Assert(t).AreEqual(remove, add, "expected nodes removed and nodes added to be equal")
	return add, err
}

func TestShouldRollInitial(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 3, 0, map[types.NodeName]bool{
		"node1": true,
		"node2": true,
		"node3": true,
	}, nil, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, err := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(roll, 1, "expected to only roll one node")
}

func TestShouldRollInitialUnknown(t *testing.T) {
	upd, _, manifest, _, f := updateWithHealth(t, 3, 0, nil, nil, nil)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, _ := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).AreEqual(roll, 0, "expected to roll no nodes if health is unknown")
}

func TestShouldRollInitialMigrationFromZero(t *testing.T) {
	upd, _, manifest, _, f := updateWithHealth(t, 0, 0, nil, nil, nil)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	remove, add, err := upd.shouldRollAfterDelay(manifest.ID())
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(remove, 0, "expected to remove no nodes")
	Assert(t).AreEqual(add, 1, "expected to add one node")
}

func TestShouldRollMidwayUnhealthy(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Critical},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 2, 1, map[types.NodeName]bool{
		"node1": true,
		"node2": true,
	}, map[types.NodeName]bool{
		"node3": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, _ := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).AreEqual(roll, 0, "expected to roll no nodes")
}

func TestShouldRollMidwayUnhealthyMigration(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node3": {Status: health.Critical},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 2, 1, nil, map[types.NodeName]bool{
		"node3": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, _ := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).AreEqual(roll, 0, "expected to roll no nodes")
}

func TestShouldRollMidwayUnhealthyMigrationFromZero(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node3": {Status: health.Critical},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 0, 1, nil, map[types.NodeName]bool{
		"node3": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	remove, add, _ := upd.shouldRollAfterDelay(manifest.ID())
	Assert(t).AreEqual(remove, 0, "expected to remove no nodes")
	Assert(t).AreEqual(add, 0, "expected to add no nodes")
}

func TestShouldRollMidwayHealthy(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 2, 1, map[types.NodeName]bool{
		"node1": true,
		"node2": true,
	}, map[types.NodeName]bool{
		"node3": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, err := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(roll, 1, "expected to roll one node")
}

func TestShouldRollMidwayUnknkown(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node3": {Status: health.Passing},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 2, 1, nil, map[types.NodeName]bool{
		"node3": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	roll, _ := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).AreEqual(roll, 0, "expected to roll no nodes when old nodes all have unknown health")
}

func TestShouldRollMidwayDesireLessThanHealthy(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
		"node4": {Status: health.Passing},
		"node5": {Status: health.Passing},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 3, 2, map[types.NodeName]bool{
		// This is something that may happen in a rolling update:
		// old RC only desires three nodes, but still has all five.
		"node1": true,
		"node2": true,
		"node3": true,
		"node4": true,
		"node5": true,
	}, map[types.NodeName]bool{}, checks)
	defer f()
	upd.DesiredReplicas = 5
	upd.MinimumReplicas = 3

	roll, _ := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).AreEqual(roll, 0, "expected to roll no nodes")
}

func TestShouldRollMidwayDesireLessThanHealthyPartial(t *testing.T) {
	// This test is like the above, but ensures that we are not too conservative.
	// If we have a minimum health of 3, desire 3 on the old side,
	// and have 1 healthy on the new side, we should have room to roll one node.
	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
		"node4": {Status: health.Passing},
		"node5": {Status: health.Passing},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 3, 2, map[types.NodeName]bool{
		// This is something that may happen in a rolling update:
		// old RC only desires three nodes, but still has four of them.
		"node1": true,
		"node2": true,
		"node3": true,
		"node4": true,
	}, map[types.NodeName]bool{
		"node5": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 5
	upd.MinimumReplicas = 3

	roll, err := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(roll, 1, "expected to roll one node")
}

func TestShouldRollWhenNewSatisfiesButNotAllDesiredHealthy(t *testing.T) {
	// newHealthy < newDesired, and newHealthy >= minHealthy.
	// In this case, we schedule the remaining nodes.
	// We want to ensure that remaining == targetDesired - newDesired
	// instead of targetDesired - newHealthy
	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Critical},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 1, 2, map[types.NodeName]bool{
		"node1": true,
	}, map[types.NodeName]bool{
		"node2": true,
		"node3": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 1

	roll, err := upd.uniformShouldRollAfterDelay(t, manifest.ID())
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(roll, 1, "expected to roll one node")
}

func TestShouldRollMidwayHealthyMigrationFromZero(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node3": {Status: health.Passing},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 0, 1, nil, map[types.NodeName]bool{
		"node3": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	remove, add, err := upd.shouldRollAfterDelay(manifest.ID())
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(remove, 0, "expected to remove no nodes")
	Assert(t).AreEqual(add, 1, "expected to add one node")
}

func TestShouldRollMidwayHealthyMigrationFromZeroWhenNewSatisfies(t *testing.T) {
	checks := map[types.NodeName]health.Result{
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
	}
	upd, _, manifest, _, f := updateWithHealth(t, 0, 2, nil, map[types.NodeName]bool{
		"node2": true,
		"node3": true,
	}, checks)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	remove, add, err := upd.shouldRollAfterDelay(manifest.ID())
	Assert(t).IsNil(err, "expected no error determining nodes to roll")
	Assert(t).AreEqual(remove, 0, "expected to remove no nodes")
	Assert(t).AreEqual(add, 1, "expected to add one node")
}

type testReplicationControllerWatcher interface {
	Watch(rc rc_fields.ID, quit <-chan struct{}) (<-chan rc_fields.RC, <-chan error)
}

func watchRCOrFail(ctx context.Context, t *testing.T, rcs testReplicationControllerWatcher, id rc_fields.ID, desc string, wg *sync.WaitGroup) <-chan rc_fields.RC {
	rcCh, errors := rcs.Watch(id, nil)
	wg.Add(1)
	go failOnError(ctx, t, desc, errors, wg)
	return rcCh
}

func failOnError(ctx context.Context, t *testing.T, desc string, errs <-chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case err, ok := <-errs:
			if !ok {
				return
			}

			t.Errorf("Error received on %s: %v", desc, err)
		case <-ctx.Done():
			// test is over
			return
		}
	}
}

type testLabeler interface {
	SetLabel(labels.Type, string, string, string) error
}

// Transfers the named node from the old RC to the new RC
func transferNode(node types.NodeName, manifest manifest.Manifest, upd update) error {
	if _, err := upd.consuls.SetPod(consul.REALITY_TREE, node, manifest); err != nil {
		return err
	}

	fullLabeler := upd.labeler.(testLabeler)
	return fullLabeler.SetLabel(labels.POD, labels.MakePodLabelKey(node, manifest.ID()), rc.RCIDLabel, string(upd.NewRC))
}

func assertRCUpdates(t *testing.T, rcCh <-chan rc_fields.RC, expect int, desc string) {
	rc := <-rcCh
	if rc.ReplicasDesired != expect {
		t.Fatalf("expected replicas desired count to be %d but was %d", expect, rc.ReplicasDesired)
	}
}

func assertRollLoopResult(t *testing.T, channel <-chan bool, expect bool) {
	select {
	case observed := <-channel:
		expectMessage := "expected roll loop to terminate with true (successful)"
		if !expect {
			expectMessage = "expected roll loop to terminate with false (asked to quit)"
		}
		Assert(t).AreEqual(observed, expect, expectMessage)
	case <-time.After(1 * time.Second):
		t.Fatalf("Roll loop didn't give result after one second, was waiting for value %t", expect)
	}
}

func TestRollLoopTypicalCase(t *testing.T) {
	upd, _, manifest, rcWatcher, f := updateWithHealth(t, 3, 0, map[types.NodeName]bool{
		"node1": true,
		"node2": true,
		"node3": true,
	}, nil, nil)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	healths := make(chan map[types.NodeName]health.Result)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	oldRCCh := watchRCOrFail(ctx, t, rcWatcher, upd.OldRC, "old RC", &wg)
	newRCCh := watchRCOrFail(ctx, t, rcWatcher, upd.NewRC, "new RC", &wg)

	rollLoopResult := make(chan bool)

	go func() {
		rollLoopResult <- upd.rollLoop(ctx, manifest.ID(), healths, nil)
		close(rollLoopResult)
	}()

	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
	}
	healths <- checks

	assertRCUpdates(t, oldRCCh, 3, "old RC")
	assertRCUpdates(t, newRCCh, 0, "new RC")

	healths <- checks

	assertRCUpdates(t, oldRCCh, 2, "old RC")
	assertRCUpdates(t, newRCCh, 1, "new RC")

	err := transferNode("node1", manifest, upd)
	if err != nil {
		t.Fatal(err)
	}
	healths <- checks

	assertRCUpdates(t, oldRCCh, 1, "old RC")
	assertRCUpdates(t, newRCCh, 2, "new RC")

	err = transferNode("node2", manifest, upd)
	if err != nil {
		t.Fatal(err)
	}
	healths <- checks

	assertRCUpdates(t, oldRCCh, 0, "old RC")
	assertRCUpdates(t, newRCCh, 3, "new RC")

	err = transferNode("node3", manifest, upd)
	if err != nil {
		t.Fatal(err)
	}
	healths <- checks

	assertRollLoopResult(t, rollLoopResult, true)

	cancel()
	wg.Wait()
}

func failIfRCDesireChanges(t *testing.T, rcCh <-chan rc_fields.RC, expected int) {
	for rc := range rcCh {
		if rc.ReplicasDesired != expected {
			t.Errorf("expected replicas desired count to be %d but was %d", expected, rc.ReplicasDesired)
		}
	}
}

func TestRollLoopMigrateFromZero(t *testing.T) {
	upd, _, manifest, rcWatcher, f := updateWithHealth(t, 0, 0, nil, nil, nil)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	healths := make(chan map[types.NodeName]health.Result)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldRCCh := watchRCOrFail(ctx, t, rcWatcher, upd.OldRC, "old RC", &wg)
	newRCCh := watchRCOrFail(ctx, t, rcWatcher, upd.NewRC, "new RC", &wg)
	go failIfRCDesireChanges(t, oldRCCh, 0)

	rollLoopResult := make(chan bool)

	go func() {
		rollLoopResult <- upd.rollLoop(ctx, manifest.ID(), healths, nil)
		close(rollLoopResult)
	}()

	checks := map[types.NodeName]health.Result{}
	healths <- checks

	assertRCUpdates(t, newRCCh, 0, "new RC")

	healths <- checks

	assertRCUpdates(t, newRCCh, 1, "new RC")

	checks["node1"] = health.Result{Status: health.Passing}
	err := transferNode("node1", manifest, upd)
	if err != nil {
		t.Fatal(err)
	}
	healths <- checks

	assertRCUpdates(t, newRCCh, 2, "new RC")

	checks["node2"] = health.Result{Status: health.Passing}
	err = transferNode("node2", manifest, upd)
	if err != nil {
		t.Fatal(err)
	}
	healths <- checks

	assertRCUpdates(t, newRCCh, 3, "new RC")

	checks["node3"] = health.Result{Status: health.Passing}
	err = transferNode("node3", manifest, upd)
	if err != nil {
		t.Fatal(err)
	}
	healths <- checks

	assertRollLoopResult(t, rollLoopResult, true)

	cancel()
	wg.Wait()
}

func TestRollLoopStallsIfUnhealthy(t *testing.T) {
	upd, _, manifest, rcWatcher, f := updateWithHealth(t, 3, 0, map[types.NodeName]bool{
		"node1": true,
		"node2": true,
		"node3": true,
	}, nil, nil)
	defer f()
	upd.DesiredReplicas = 3
	upd.MinimumReplicas = 2

	healths := make(chan map[types.NodeName]health.Result)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	oldRCCh := watchRCOrFail(ctx, t, rcWatcher, upd.OldRC, "old RC", &wg)
	newRCCh := watchRCOrFail(ctx, t, rcWatcher, upd.NewRC, "new RC", &wg)

	rollLoopResult := make(chan bool)

	go func() {
		rollLoopResult <- upd.rollLoop(ctx, manifest.ID(), healths, nil)
		close(rollLoopResult)
	}()

	checks := map[types.NodeName]health.Result{
		"node1": {Status: health.Passing},
		"node2": {Status: health.Passing},
		"node3": {Status: health.Passing},
	}
	healths <- checks

	assertRCUpdates(t, oldRCCh, 3, "old RC")
	assertRCUpdates(t, newRCCh, 0, "new RC")

	healths <- checks

	assertRCUpdates(t, oldRCCh, 2, "old RC")
	assertRCUpdates(t, newRCCh, 1, "new RC")

	err := transferNode("node1", manifest, upd)
	if err != nil {
		t.Fatal(err)
	}
	checks["node1"] = health.Result{Status: health.Critical}
	go failIfRCDesireChanges(t, oldRCCh, 2)
	go failIfRCDesireChanges(t, newRCCh, 1)
	for i := 0; i < 5; i++ {
		healths <- checks
	}

	cancel()
	wg.Wait()
	assertRollLoopResult(t, rollLoopResult, false)
}
