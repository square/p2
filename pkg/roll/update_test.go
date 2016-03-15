package roll

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/square/p2/pkg/kp/kptest"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll/fields"

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
