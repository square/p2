package labels

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/logging"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

func alterAggregationTime(dur time.Duration) {
	AggregationRateCap = dur
}

func fakeLabeledPods() map[string][]byte {
	return map[string][]byte{
		objectPath(POD, "maroono"):  []byte(`{"color": "red", "deployment": "production"}`),
		objectPath(POD, "emeralda"): []byte(`{"color": "green", "deployment": "canary"}`),
		objectPath(POD, "slashi"):   []byte(`{"color": "red", "deployment": "canary"}`),
	}
}

// Check that two clients can share an aggregator
func TestTwoClients(t *testing.T) {
	alterAggregationTime(time.Millisecond)

	fakeKV := &fakeLabelStore{fakeLabeledPods(), nil}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger)
	go aggreg.Aggregate()
	defer aggreg.Quit()

	quitCh := make(chan struct{})
	labeledChannel1 := aggreg.Watch(labels.Everything().Add("color", labels.EqualsOperator, []string{"green"}), quitCh)
	labeledChannel2 := aggreg.Watch(labels.Everything().Add("deployment", labels.EqualsOperator, []string{"canary"}), quitCh)

	var checked string
	for i := 0; i < 2; i++ {
		select {
		case <-time.After(time.Second):
			t.Fatal("Should not have taken a second to get results")
		case labeledWatch := <-labeledChannel1:
			Assert(t).IsTrue(labeledWatch.Valid, "valid should not have been true")
			labeled := labeledWatch.Labeled
			Assert(t).AreNotEqual("green", checked, "Should not have already checked the green selector result")
			checked = "green" // ensure that both sides get checked
			Assert(t).AreEqual(1, len(labeled), "Should have received one result from the color watch")
			Assert(t).AreEqual("emeralda", labeled[0].ID, "should have received the emerald app")
		case labeledWatch := <-labeledChannel2:
			Assert(t).IsTrue(labeledWatch.Valid, "valid should not have been true")
			labeled := labeledWatch.Labeled
			Assert(t).AreNotEqual("canary", checked, "Should not have already checked the canary selector result")
			checked = "canary" // ensure that both sides get checked
			Assert(t).AreEqual(2, len(labeled), "Should have received two results from the canary watch")
			emeraldaIndex := 0
			slashiIndex := 1
			if labeled[0].ID == "slashi" { // order doesn't matter
				emeraldaIndex, slashiIndex = slashiIndex, emeraldaIndex
			}
			Assert(t).AreEqual("emeralda", labeled[emeraldaIndex].ID, "should have received the emerald app")
			Assert(t).AreEqual("slashi", labeled[slashiIndex].ID, "should have received the slashi app")
		}
	}
}

func TestQuitAggregateAfterResults(t *testing.T) {
	alterAggregationTime(time.Millisecond)

	fakeKV := &fakeLabelStore{fakeLabeledPods(), nil}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger)
	go aggreg.Aggregate()

	quitCh := make(chan struct{})
	res := aggreg.Watch(labels.Everything().Add("color", labels.EqualsOperator, []string{"green"}), quitCh)

	// Quit now. We expect that the aggregator will close the res channels
	aggreg.Quit()
	success := make(chan struct{})
	go func() {
		for _ = range res {
		}
		success <- struct{}{}
	}()

	select {
	case <-success:
	case <-time.After(time.Second):
		t.Fatal("Should not be waiting or processing results after a second")
	}
}

func TestQuitAggregateBeforeResults(t *testing.T) {
	alterAggregationTime(time.Millisecond)

	// this channel prevents the List from returning, so the aggregator
	// must quit prior to entering the loop
	trigger := make(chan struct{})
	fakeKV := &fakeLabelStore{fakeLabeledPods(), trigger}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger)
	go aggreg.Aggregate()

	quitCh := make(chan struct{})
	res := aggreg.Watch(labels.Everything().Add("color", labels.EqualsOperator, []string{"green"}), quitCh)

	// Quit now. We expect that the aggregator will close the res channels
	aggreg.Quit()

	select {
	case labeled := <-res:
		Assert(t).IsNotNil(labeled, "Should not have received any results")
	case <-time.After(time.Second):
		t.Fatal("Should still be waiting or processing results after a second")
	}
}

func TestQuitIndividualWatch(t *testing.T) {
	alterAggregationTime(time.Millisecond)

	fakeKV := &fakeLabelStore{fakeLabeledPods(), nil}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger)
	go aggreg.Aggregate()

	quitCh1 := make(chan struct{})
	labeledChannel1 := aggreg.Watch(labels.Everything().Add("color", labels.EqualsOperator, []string{"green"}), quitCh1)

	quitCh2 := make(chan struct{})
	labeledChannel2 := aggreg.Watch(labels.Everything().Add("deployment", labels.EqualsOperator, []string{"production"}), quitCh2)

	close(quitCh1) // this should not interrupt the flow of messages to the second channel

	// iterate twice to show that we are not waiting on other now-closed channels
	for i := 0; i < 2; i++ {
		select {
		case <-time.After(time.Second):
			t.Fatalf("Should not have taken a second to get results on iteration %v", i)
		case labeledWatch := <-labeledChannel2:
			Assert(t).IsTrue(labeledWatch.Valid, "valid should not have been true")
			labeled := labeledWatch.Labeled
			Assert(t).AreEqual(1, len(labeled), "Should have one result with a production deployment")
			Assert(t).AreEqual("maroono", labeled[0].ID, "Should have received maroono as the one production deployment")
		}
	}

	// drain the first channel to show that it was closed. We do this
	// in a loop since it is possible that a value was sent on the channel
	success := make(chan struct{})
	go func() {
		for _ = range labeledChannel1 {
		}
		success <- struct{}{}
	}()
	select {
	case <-time.After(time.Second):
		t.Fatal("Should not have taken a second to see the closed label channel")
	case <-success:
	}
}
