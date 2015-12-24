package labels

import (
	"reflect"
	"strings"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"

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

	matches, err := c.GetMatches(labels.Everything().Add("label", labels.EqualsOperator, []string{"value"}), POD)
	Assert(t).IsNil(err, "should have had nil error fetching positive matches")
	Assert(t).AreEqual(len(matches), 1, "should have had exactly one positive match")

	matches, err = c.GetMatches(labels.Everything().Add("label", labels.EqualsOperator, []string{"value"}), NODE)
	Assert(t).IsNil(err, "should have had nil error fetching positive matches for wrong type")
	Assert(t).AreEqual(len(matches), 0, "should have had exactly zero mistyped matches")

	matches, err = c.GetMatches(labels.Everything().Add("label", labels.NotInOperator, []string{"value"}), POD)
	Assert(t).IsNil(err, "should have had nil error fetching negative matches")
	Assert(t).AreEqual(len(matches), 0, "should have had exactly zero negative matches")
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
		kv:      &failOnceLabelStore{inner: &fakeLabelStore{data: map[string][]byte{}}},
		logger:  logging.DefaultLogger,
		retries: 3,
	}

	Assert(t).IsNil(c.SetLabel(POD, "object", "label", "value"), "should have retried despite failing once")
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
