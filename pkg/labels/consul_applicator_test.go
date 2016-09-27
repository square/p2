package labels

import (
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/hashicorp/consul/api"
	"github.com/rcrowley/go-metrics"
	"k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/logging"
)

type fakeLabelStore struct {
	data map[string][]byte
	// If this channel is set, fakeApplicator will wait to return content until
	// this channel receives a value
	watchTrigger chan struct{}
}

func (f *fakeLabelStore) List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	var ret api.KVPairs

	if f.watchTrigger != nil {
		<-f.watchTrigger
	}

	for k, v := range f.data {
		if strings.HasPrefix(k, prefix) {
			ret = append(ret, &api.KVPair{
				Key:   k,
				Value: v,
			})
		}
	}

	return ret, &api.QueryMeta{}, nil
}

func (f *fakeLabelStore) Delete(key string, opts *api.WriteOptions) (*api.WriteMeta, error) {
	delete(f.data, key)
	return &api.WriteMeta{}, nil
}

func (f *fakeLabelStore) DeleteCAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error) {
	delete(f.data, pair.Key)
	return true, &api.WriteMeta{}, nil
}

func (f *fakeLabelStore) CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error) {
	f.data[pair.Key] = pair.Value
	return true, &api.WriteMeta{}, nil
}

func (f *fakeLabelStore) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	if v, ok := f.data[key]; ok {
		return &api.KVPair{
			Key:   key,
			Value: v,
		}, nil, nil
	} else {
		return nil, nil, nil
	}
}

func TestBasicSetGetRemove(t *testing.T) {
	c := &consulApplicator{
		kv:     &fakeLabelStore{data: map[string][]byte{}},
		logger: logging.DefaultLogger,
	}

	Assert(t).IsNil(c.SetLabel(POD, "object", "label", "value"), "should have had nil error when setting label")

	labeledObject, err := c.GetLabels(POD, "object")
	Assert(t).IsNil(err, "should have had nil error when getting labels")
	Assert(t).IsTrue(reflect.DeepEqual(labeledObject.Labels, labels.Set{
		"label": "value",
	}), "should have had matching label map {\"label\": \"value\"}")

	Assert(t).IsNil(c.RemoveLabel(POD, "object", "label"), "should have had nil error when clearing label")

	labeledObject, err = c.GetLabels(POD, "object")
	Assert(t).IsNil(err, "should have had nil error when getting labels")
	Assert(t).IsTrue(reflect.DeepEqual(labeledObject.Labels, labels.Set{}), "should have had empty label map")
}

func TestSetGetRemoveAll(t *testing.T) {
	c := &consulApplicator{
		kv:     &fakeLabelStore{data: map[string][]byte{}},
		logger: logging.DefaultLogger,
	}

	Assert(t).IsNil(c.SetLabel(POD, "object", "label", "value"), "should have had nil error when setting label")
	Assert(t).IsNil(c.SetLabel(POD, "object", "label1", "value1"), "should have had nil error when setting label")
	labeledObject, err := c.GetLabels(POD, "object")
	Assert(t).IsTrue(reflect.DeepEqual(labeledObject.Labels, labels.Set{
		"label":  "value",
		"label1": "value1",
	}), "should have had matching label maps")

	Assert(t).IsNil(c.RemoveLabel(POD, "object", "label"), "should have had nil error when removing label")
	labeledObject, err = c.GetLabels(POD, "object")
	Assert(t).IsTrue(reflect.DeepEqual(labeledObject.Labels, labels.Set{
		"label1": "value1",
	}), "should have had matching label maps after removing one label")

	Assert(t).IsNil(c.RemoveAllLabels(POD, "object"), "should have had nil error when removing all labels")
	labeledObject, err = c.GetLabels(POD, "object")
	Assert(t).IsNil(err, "should have had nil error when getting labels")
	Assert(t).IsTrue(reflect.DeepEqual(labeledObject.Labels, labels.Set{}), "should have had empty label map")
}

func TestEmptyGetRemove(t *testing.T) {
	c := &consulApplicator{
		kv:     &fakeLabelStore{data: map[string][]byte{}},
		logger: logging.DefaultLogger,
	}

	labeledObject, err := c.GetLabels(POD, "object")
	Assert(t).IsNil(err, "should have had nil error when getting labels")
	Assert(t).IsTrue(reflect.DeepEqual(labeledObject.Labels, labels.Set{}), "should have had empty label map")

	Assert(t).IsNil(c.RemoveLabel(POD, "object", "label"), "no error when removing nonexistent label or object")
}

func TestBasicMatch(t *testing.T) {
	c := &consulApplicator{
		kv:     &fakeLabelStore{data: map[string][]byte{}},
		logger: logging.DefaultLogger,
	}

	Assert(t).IsNil(c.SetLabel(POD, "object", "label", "value"), "should have had nil error when setting label")

	matches, err := c.GetMatches(labels.Everything().Add("label", labels.EqualsOperator, []string{"value"}), POD, false)
	Assert(t).IsNil(err, "should have had nil error fetching positive matches")
	Assert(t).AreEqual(len(matches), 1, "should have had exactly one positive match")

	matches, err = c.GetMatches(labels.Everything().Add("label", labels.EqualsOperator, []string{"value"}), NODE, false)
	Assert(t).IsNil(err, "should have had nil error fetching positive matches for wrong type")
	Assert(t).AreEqual(len(matches), 0, "should have had exactly zero mistyped matches")

	matches, err = c.GetMatches(labels.Everything().Add("label", labels.NotInOperator, []string{"value"}), POD, false)
	Assert(t).IsNil(err, "should have had nil error fetching negative matches")
	Assert(t).AreEqual(len(matches), 0, "should have had exactly zero negative matches")
}

func TestSetLabels(t *testing.T) {
	c := &consulApplicator{
		kv:     &fakeLabelStore{data: map[string][]byte{}},
		logger: logging.DefaultLogger,
	}

	Assert(t).IsNil(c.SetLabel(POD, "object", "label", "value"), "should have had nil error when setting label")

	matches, err := c.GetMatches(labels.Everything().Add("label", labels.EqualsOperator, []string{"value"}), POD, false)
	Assert(t).IsNil(err, "should have had nil error fetching positive matches")
	Assert(t).AreEqual(len(matches), 1, "should have had exactly one positive match")

	labelsToSet := map[string]string{
		"label1": "value1",
		"label2": "value2",
	}
	Assert(t).IsNil(c.SetLabels(POD, "object", labelsToSet), "should not have erred setting multiple labels")

	sel := labels.Everything().
		Add("label", labels.EqualsOperator, []string{"value"}).
		Add("label1", labels.EqualsOperator, []string{"value1"}).
		Add("label2", labels.EqualsOperator, []string{"value2"})

	matches, err = c.GetMatches(sel, POD, false)
	Assert(t).IsNil(err, "should have had nil error fetching positive matches")
	Assert(t).AreEqual(len(matches), 1, "should have had exactly one positive match")
}

type failOnceLabelStore struct {
	inner      consulKV
	succeedCAS bool
}

func (f *failOnceLabelStore) List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	return f.inner.List(prefix, opts)
}

func (f *failOnceLabelStore) Delete(key string, opts *api.WriteOptions) (*api.WriteMeta, error) {
	return f.inner.Delete(key, opts)
}

func (f *failOnceLabelStore) DeleteCAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error) {
	if !f.succeedCAS {
		f.succeedCAS = true
		return false, &api.WriteMeta{}, nil
	}
	return f.inner.DeleteCAS(pair, opts)
}

func (f *failOnceLabelStore) CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error) {
	if !f.succeedCAS {
		f.succeedCAS = true
		return false, &api.WriteMeta{}, nil
	}
	return f.inner.CAS(pair, opts)
}

func (f *failOnceLabelStore) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	return f.inner.Get(key, q)
}

func TestCASRetries(t *testing.T) {
	c := &consulApplicator{
		kv:          &failOnceLabelStore{inner: &fakeLabelStore{data: map[string][]byte{}}},
		logger:      logging.DefaultLogger,
		retries:     3,
		retryMetric: metrics.NewGauge(),
	}

	Assert(t).IsNil(c.SetLabel(POD, "object", "label", "value"), "should have retried despite failing once")
	Assert(t).AreEqual(c.retryMetric.Value(), int64(1), "should have recorded a retry in metrics gauge")
}

func TestCASNoRetries(t *testing.T) {
	c := &consulApplicator{
		kv:      &failOnceLabelStore{inner: &fakeLabelStore{data: map[string][]byte{}}},
		logger:  logging.DefaultLogger,
		retries: 0,
	}

	err := c.SetLabel(POD, "object", "label", "value")
	Assert(t).IsNotNil(err, "should have failed on first try")
	_, ok := err.(CASError)
	Assert(t).IsTrue(ok, "should have returned a CASError")
}

func TestWatchMatchDiff(t *testing.T) {
	c := &consulApplicator{
		logger:      logging.DefaultLogger,
		kv:          &failOnceLabelStore{inner: &fakeLabelStore{data: map[string][]byte{}}},
		retries:     3,
		aggregators: map[Type]*consulAggregator{},
		retryMetric: metrics.NewGauge(),
	}

	quitCh := make(chan struct{})
	defer close(quitCh)
	inCh := c.WatchMatchDiff(labels.Everything(), NODE, quitCh)

	var changes *LabeledChanges
	select {
	case changes = <-inCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	Assert(t).AreEqual(len(changes.Created), 0, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 0, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 0, "expected number of deleted labels to match")

	// Create a label and verify that it was created
	err := c.SetLabel(NODE, "node1", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")

	select {
	case changes = <-inCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	Assert(t).AreEqual(len(changes.Created), 1, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 0, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 0, "expected number of deleted labels to match")

	// Create another label and update one and verify
	err = c.SetLabel(NODE, "node2", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")
	err = c.SetLabel(NODE, "node1", "foo", "foo")
	Assert(t).IsNil(err, "expected no error setting label")

	select {
	case changes = <-inCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	Assert(t).AreEqual(len(changes.Created), 1, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 1, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 0, "expected number of deleted labels to match")

	// Delete a label and create one
	err = c.RemoveAllLabels(NODE, "node1")
	Assert(t).IsNil(err, "expected no error removing labels")
	err = c.SetLabel(NODE, "node3", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")

	select {
	case changes = <-inCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	Assert(t).AreEqual(len(changes.Created), 1, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 0, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 1, "expected number of deleted labels to match")

	// Create, Update, and Delete a label
	err = c.SetLabel(NODE, "node4", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")
	err = c.SetLabel(NODE, "node3", "foo", "foo")
	Assert(t).IsNil(err, "expected no error setting label")
	err = c.RemoveAllLabels(NODE, "node2")
	Assert(t).IsNil(err, "expected no error removing labels")

	select {
	case changes = <-inCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	Assert(t).AreEqual(len(changes.Created), 1, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 1, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 1, "expected number of deleted labels to match")

	// Remove the remaining two labels
	err = c.RemoveAllLabels(NODE, "node3")
	Assert(t).IsNil(err, "expected no error removing labels")
	err = c.RemoveAllLabels(NODE, "node4")
	Assert(t).IsNil(err, "expected no error removing labels")

	select {
	case changes = <-inCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	Assert(t).AreEqual(len(changes.Created), 0, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 0, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 2, "expected number of deleted labels to match")
}
