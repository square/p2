package labels

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"
)

const labelRoot = "labels"

type CASError struct {
	Key string
}

func (e CASError) Error() string {
	return fmt.Sprintf("Could not check-and-set key %q", e.Key)
}

type consulKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	Delete(key string, opts *api.WriteOptions) (*api.WriteMeta, error)
	DeleteCAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
}

type consulApplicator struct {
	kv          consulKV
	logger      logging.Logger
	retries     int
	aggregators map[Type]*consulAggregator
}

func NewConsulApplicator(client *api.Client, retries int) *consulApplicator {
	return &consulApplicator{
		logger:      logging.DefaultLogger,
		kv:          client.KV(),
		retries:     retries,
		aggregators: map[Type]*consulAggregator{},
	}
}

func typePath(labelType Type) string {
	return path.Join(labelRoot, labelType.String())
}

func objectPath(labelType Type, id string) string {
	return path.Join(typePath(labelType), id)
}

func (c *consulApplicator) getLabels(labelType Type, id string) (Labeled, uint64, error) {
	kvp, _, err := c.kv.Get(objectPath(labelType, id), nil)
	if err != nil || kvp == nil {
		return Labeled{
			ID:        id,
			LabelType: labelType,
			Labels:    labels.Set{},
		}, 0, err
	}

	l, err := convertKVPToLabeled(kvp)
	return l, kvp.ModifyIndex, err
}
func (c *consulApplicator) GetLabels(labelType Type, id string) (Labeled, error) {
	l, _, err := c.getLabels(labelType, id)
	return l, err
}

func (c *consulApplicator) GetMatches(selector labels.Selector, labelType Type) ([]Labeled, error) {
	// TODO: use aggregator to enable caching
	allMatches, _, err := c.kv.List(typePath(labelType)+"/", nil)
	if err != nil {
		return nil, err
	}

	res := []Labeled{}
	for _, kvp := range allMatches {
		l, err := convertKVPToLabeled(kvp)
		if err != nil {
			return res, err
		}
		if selector.Matches(l.Labels) {
			res = append(res, l)
		}
	}
	return res, nil
}

// generalized label mutator function - pass nil value to delete that label
func (c *consulApplicator) mutateLabel(labelType Type, id, label string, value *string) error {
	l, index, err := c.getLabels(labelType, id)
	if err != nil {
		return err
	}

	if value == nil {
		delete(l.Labels, label)
	} else {
		l.Labels[label] = *value
	}
	setkvp, err := convertLabeledToKVP(l)
	if err != nil {
		return err
	}
	setkvp.ModifyIndex = index

	var success bool
	if len(l.Labels) == 0 {
		// still have to use CAS when deleting, to avoid discarding someone
		// else's concurrent modification
		// DeleteCAS ignores the value on the KVPair, so it doesn't matter if
		// we set it earlier
		success, _, err = c.kv.DeleteCAS(setkvp, nil)
	} else {
		success, _, err = c.kv.CAS(setkvp, nil)
	}
	if err != nil {
		return err
	}
	if !success {
		return CASError{setkvp.Key}
	}
	return nil
}

// this function will attempt to mutateLabel. if it gets a CAS error, then it
// will retry up to the number of attempts specified in c.Retries
func (c *consulApplicator) retryMutate(labelType Type, id, label string, value *string) error {
	err := c.mutateLabel(labelType, id, label, value)
	for i := 0; i < c.retries; i++ {
		if _, ok := err.(CASError); ok {
			err = c.mutateLabel(labelType, id, label, value)
		} else {
			break
		}
	}
	return err
}

func (c *consulApplicator) SetLabel(labelType Type, id, label, value string) error {
	return c.retryMutate(labelType, id, label, &value)
}

func (c *consulApplicator) RemoveLabel(labelType Type, id, label string) error {
	return c.retryMutate(labelType, id, label, nil)
}

func (c *consulApplicator) RemoveAllLabels(labelType Type, id string) error {
	_, err := c.kv.Delete(objectPath(labelType, id), nil)
	return err
}

// kvp must be non-nil
func convertKVPToLabeled(kvp *api.KVPair) (Labeled, error) {
	// /<root>/<type>/<id>
	// We need to split instead of using path.Base, path.Dir.
	// This is because <id> could contain "/"
	parts := strings.SplitN(kvp.Key, "/", 3)
	if len(parts) < 3 {
		return Labeled{}, util.Errorf("Malformed label key %s", kvp.Key)
	}

	ret := Labeled{
		ID:     parts[2],
		Labels: labels.Set{},
	}

	labelType, err := AsType(parts[1])
	if err != nil {
		return ret, err
	}
	ret.LabelType = labelType

	err = json.Unmarshal(kvp.Value, &ret.Labels)
	return ret, err
}

func convertLabeledToKVP(l Labeled) (*api.KVPair, error) {
	value, err := json.Marshal(l.Labels)
	if err != nil {
		return nil, err
	}

	return &api.KVPair{
		Key:   objectPath(l.LabelType, l.ID),
		Value: value,
	}, nil
}

// The current schema of labels in Consul is optimized for label retrieval on a single
// object of a given ID. This layout is less effective when attempting to perform a
// watch on the results of an arbitrary label selector, which is necessarily un-indexable.
// This implementation will perform the simplest possible optimization, which is to cache
// the entire contents of the tree under the given label type and share it with other watches.
//
// Due to the possibility that this tree might change quite frequently in environments
// with lots of concurrent deployments, the aggregated result from Consul will only be queried
// by a maximum frequency of once per LabelAggregationCap.
//
// Preparers should not use the consulApplicator's implementation of WatchMatches directly due
// to the cost of querying for this subtree on any sizeable fleet of machines. Instead, preparers should
// use the httpApplicator from a server that exposes the results of this (or another)
// implementation's watch.
func (c *consulApplicator) WatchMatches(selector labels.Selector, labelType Type, quitCh chan struct{}) chan *[]Labeled {
	aggregator, ok := c.aggregators[labelType]
	if !ok {
		aggregator = NewConsulAggregator(labelType, c.kv, c.logger)
		c.aggregators[labelType] = aggregator
	}
	return aggregator.Watch(selector, quitCh)
}

// confirm at compile time that consulApplicator is an implementation of the Applicator interface
var _ Applicator = &consulApplicator{}
