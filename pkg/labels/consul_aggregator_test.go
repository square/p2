package labels

import (
	"path"
	"testing"
	"time"

	"github.com/square/p2/pkg/logging"

	. "github.com/anthonybishopric/gotcha"
	"github.com/rcrowley/go-metrics"
	"k8s.io/kubernetes/pkg/labels"
)

func alterAggregationTime(dur time.Duration) {
	DefaultAggregationRate = dur
}

func fakeLabeledPods() map[string][]byte {
	return map[string][]byte{
		path.Join(typePath(POD), "maroono"):  []byte(`{"color": "red", "deployment": "production"}`),
		path.Join(typePath(POD), "emeralda"): []byte(`{"color": "green", "deployment": "canary"}`),
		path.Join(typePath(POD), "slashi"):   []byte(`{"color": "red", "deployment": "canary"}`),
	}
}

// Check that two clients can share an aggregator
func TestTwoClients(t *testing.T) {
	alterAggregationTime(100 * time.Millisecond)

	fakeKV := &fakeLabelStore{
		data:         fakeLabeledPods(),
		watchTrigger: nil,
	}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger, metrics.NewRegistry(), 0)
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
		case labeled, ok := <-labeledChannel1:
			Assert(t).IsTrue(ok, "should have been okay")
			Assert(t).AreNotEqual("green", checked, "Should not have already checked the green selector result")
			checked = "green" // ensure that both sides get checked
			Assert(t).AreEqual(1, len(labeled), "Should have received one result from the color watch")
			Assert(t).AreEqual("emeralda", labeled[0].ID, "should have received the emerald app")
		case labeled, ok := <-labeledChannel2:
			Assert(t).IsTrue(ok, "should have been okay")
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
	alterAggregationTime(100 * time.Millisecond)

	fakeKV := &fakeLabelStore{
		data:         fakeLabeledPods(),
		watchTrigger: nil,
	}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger, metrics.NewRegistry(), 0)
	go aggreg.Aggregate()

	quitCh := make(chan struct{})
	res := aggreg.Watch(labels.Everything().Add("color", labels.EqualsOperator, []string{"green"}), quitCh)

	// Quit now. We expect that the aggregator will close the res channels
	aggreg.Quit()
	success := make(chan struct{})
	go func() {
		for range res {
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
	fakeKV := &fakeLabelStore{
		data:         fakeLabeledPods(),
		watchTrigger: trigger,
	}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger, metrics.NewRegistry(), 0)
	go aggreg.Aggregate()

	quitCh := make(chan struct{})
	res := aggreg.Watch(labels.Everything().Add("color", labels.EqualsOperator, []string{"green"}), quitCh)

	// Quit now. We expect that the aggregator will close the res channels
	aggreg.Quit()

	select {
	case labeled, ok := <-res:
		Assert(t).IsFalse(ok, "should have been okay")
		Assert(t).IsTrue(labeled == nil, "Should not have received any results")
	case <-time.After(time.Second):
		t.Fatal("Should still be waiting or processing results after a second")
	}
}

func TestQuitIndividualWatch(t *testing.T) {
	alterAggregationTime(time.Millisecond)

	fakeKV := &fakeLabelStore{
		data:         fakeLabeledPods(),
		watchTrigger: nil,
	}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger, metrics.NewRegistry(), 0)
	go aggreg.Aggregate()

	quitCh1 := make(chan struct{})
	labeledChannel1 := aggreg.Watch(labels.Everything().Add("color", labels.EqualsOperator, []string{"green"}), quitCh1)

	quitCh2 := make(chan struct{})
	labeledChannel2 := aggreg.Watch(labels.Everything().Add("deployment", labels.EqualsOperator, []string{"production"}), quitCh2)

	Assert(t).AreEqual(int64(2), aggreg.metWatchCount.Value(), "should currently have two watchers")

	close(quitCh1) // this should not interrupt the flow of messages to the second channel

	// iterate twice to show that we are not waiting on other now-closed channels
	for i := 0; i < 2; i++ {
		select {
		case <-time.After(time.Second):
			t.Fatalf("Should not have taken a second to get results on iteration %v", i)
		case labeled, ok := <-labeledChannel2:
			Assert(t).IsTrue(ok, "should have been okay")
			Assert(t).AreEqual(1, len(labeled), "Should have one result with a production deployment")
			Assert(t).AreEqual("maroono", labeled[0].ID, "Should have received maroono as the one production deployment")
		}
	}

	// drain the first channel to show that it was closed. We do this
	// in a loop since it is possible that a value was sent on the channel
	success := make(chan struct{})
	go func() {
		for range labeledChannel1 {
		}
		success <- struct{}{}
	}()
	select {
	case <-time.After(time.Second):
		t.Fatal("Should not have taken a second to see the closed label channel")
	case <-success:
	}

	Assert(t).AreEqual(int64(1), aggreg.metWatchCount.Value(), "should currently have one watcher")
}

// This test is identical to the quit test, except that it does not explicitly quit
// the first result. This is to test that one misbehaving client cannot interrupt
// the flow of messages to all other clients
func TestIgnoreIndividualWatch(t *testing.T) {
	alterAggregationTime(time.Millisecond)

	fakeKV := &fakeLabelStore{
		data:         fakeLabeledPods(),
		watchTrigger: nil,
	}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger, metrics.NewRegistry(), 0)
	go aggreg.Aggregate()
	defer aggreg.Quit()

	quitCh1 := make(chan struct{})
	_ = aggreg.Watch(labels.Everything().Add("color", labels.EqualsOperator, []string{"green"}), quitCh1)

	quitCh2 := make(chan struct{})
	labeledChannel2 := aggreg.Watch(labels.Everything().Add("deployment", labels.EqualsOperator, []string{"production"}), quitCh2)

	Assert(t).AreEqual(int64(2), aggreg.metWatchCount.Value(), "should currently have two watchers")

	// iterate 3 times to show that we are not waiting on other now-closed
	// channels
	for i := 0; i < 3; i++ {
		select {
		case <-time.After(time.Second):
			t.Fatalf("Should not have taken a second to get results on iteration %v", i)
		case labeled, ok := <-labeledChannel2:
			Assert(t).IsTrue(ok, "should have been okay")
			Assert(t).AreEqual(1, len(labeled), "Should have one result with a production deployment")
			Assert(t).AreEqual("maroono", labeled[0].ID, "Should have received maroono as the one production deployment")
		}
	}

	// this is a range because sending matches is parallelized, so by the
	// time we've read 3 values from labeledChannel2 we don't know if we've
	// failed to send 2 values or 3 values. additionally, metWatchSendMiss
	// lags the count by 1, because we don't count a value as missed until
	// the next value comes along
	missed := aggreg.metWatchSendMiss.Value()
	if missed < 1 || missed > 2 {
		t.Errorf("should have missed between one and two sends, but missed %d", aggreg.metWatchSendMiss.Value())
	}
}

func TestCachedValueImmediatelySent(t *testing.T) {
	fakeKV := &fakeLabelStore{
		data:         fakeLabeledPods(),
		watchTrigger: nil,
	}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger, metrics.NewRegistry(), 0)
	aggreg.labeledCache = []Labeled{
		{
			LabelType: POD,
			ID:        "heyo",
			Labels: labels.Set{
				"color": "brown",
			},
		},
	}

	selector := labels.Everything().Add("color", labels.EqualsOperator, []string{"brown"})
	quitCh := make(chan struct{})
	defer close(quitCh)
	watch := aggreg.Watch(selector, quitCh)

	// even though we have not called Aggregate() on the aggregator, we expect
	// that the cached value we have added will be present on the result channel.

	select {
	case res, ok := <-watch:
		Assert(t).IsTrue(ok, "Should have had a valid result")
		Assert(t).AreEqual(res[0].ID, "heyo", "should have matched heyo based on the query")
	case <-time.After(time.Second):
		t.Fatal("Could not read result from new watch channel")
	}
}

func TestLabelSelectorEquivalence(t *testing.T) {
	// Write two selectors slightly differently and confirm their equivalence
	// after being parsed. This equivalence property will be used to deduplicate
	// the work done if two identical selectors are registered on the consul
	// aggregator
	selector1, err := labels.Parse("roses=red,violets=blue")
	if err != nil {
		t.Fatal(err)
	}

	selector2, err := labels.Parse("violets=blue,roses=red")
	if err != nil {
		t.Fatal(err)
	}

	if !selectorsEqual(selector1, selector2) {
		t.Fatal("expected two selectors written in different order to be equivalent, but they weren't")
	}
}

func TestIdenticalSelectors(t *testing.T) {
	fakeKV := &fakeLabelStore{
		data:         fakeLabeledPods(),
		watchTrigger: nil,
	}
	aggreg := NewConsulAggregator(POD, fakeKV, logging.DefaultLogger, metrics.NewRegistry(), 0)
	go aggreg.Aggregate()
	defer aggreg.Quit()

	quitCh := make(chan struct{})
	quitCh2 := make(chan struct{})
	selector := labels.Everything().Add("color", labels.EqualsOperator, []string{"green"})
	labeledChannel1 := aggreg.Watch(selector, quitCh)
	labeledChannel2 := aggreg.Watch(selector, quitCh2)

	aggreg.watcherLock.Lock()
	if aggreg.watchers.len() != 2 {
		t.Errorf("expected 2 watchers to be reported but there were %d", aggreg.watchers.len())
	}
	aggreg.watcherLock.Unlock()

	select {
	case <-time.After(time.Second):
		t.Fatal("didn't get a value on the first selector")
	case <-labeledChannel1:
	}

	select {
	case <-time.After(time.Second):
		t.Fatal("didn't get a value on the first selector")
	case <-labeledChannel2:
	}

	close(quitCh2)

	select {
	case <-time.After(time.Second):
	case _, ok := <-labeledChannel2:
		if ok {
			t.Fatal("the result channel for the second selector should have been closed")
		}
	}

	aggreg.watcherLock.Lock()
	if aggreg.watchers.len() != 1 {
		t.Errorf("expected 1 watcher (after closing one of them) to be reported but there were %d", aggreg.watchers.len())
	}
	aggreg.watcherLock.Unlock()

	select {
	case <-time.After(time.Second):
		t.Fatal("didn't get a value on first selector after closing second")
	case <-labeledChannel1:
	}
}
