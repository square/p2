package labels

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/hashicorp/consul/api"
	"github.com/rcrowley/go-metrics"
	"k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

const labelRoot = "labels"

// NoLabelsFound represents a 404 error from consul. In most cases the results
// should be ignored if this error is encountered because under normal
// operation there should always be labels for most types such as replication
// controllers. Resources that trend to zero such as rolling updates are an
// example of a case where this error might be expected under normal operation.
// The client must know the safety and likelihood of missing labels and decide
// what to do based on that information.
var NoLabelsFound = errors.New("No labels found")

func IsNoLabelsFound(err error) bool {
	return err == NoLabelsFound
}

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
	kv            consulKV
	logger        logging.Logger
	retries       int
	aggregators   map[Type]*consulAggregator
	aggregatorMux sync.Mutex
	metReg        MetricsRegistry
	retryMetric   metrics.Gauge
}

func NewConsulApplicator(client consulutil.ConsulClient, retries int) *consulApplicator {
	return &consulApplicator{
		logger:      logging.DefaultLogger,
		kv:          client.KV(),
		retries:     retries,
		aggregators: map[Type]*consulAggregator{},
		retryMetric: metrics.NewGauge(),
	}
}

func (c *consulApplicator) SetMetricsRegistry(metReg MetricsRegistry) {
	c.metReg = metReg
	c.retryMetric = metrics.NewGauge()
	_ = c.metReg.Register("label_mutation_retries", c.retryMetric)
}

func typePath(labelType Type) string {
	return path.Join(labelRoot, labelType.String())
}

func objectPath(labelType Type, id string) (string, error) {
	if id == "" {
		return "", util.Errorf("Empty ID in label path ")
	}
	return path.Join(typePath(labelType), id), nil
}

func (c *consulApplicator) getLabels(labelType Type, id string) (Labeled, uint64, error) {
	path, err := objectPath(labelType, id)
	if err != nil {
		return Labeled{}, 0, err
	}
	kvp, _, err := c.kv.Get(path, nil)
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

func (c *consulApplicator) GetMatches(selector labels.Selector, labelType Type, cachedMatch bool) ([]Labeled, error) {

	var allLabeled []Labeled

	if cachedMatch {
		aggregator := c.initAggregator(labelType)
		cache, err := aggregator.getCache()
		if err == nil {
			allLabeled = cache
		} else {
			c.logger.Warnln("Cache was empty on query, falling back to direct Consul query")
		}
	}
	var err error
	if len(allLabeled) == 0 {
		allLabeled, err = c.ListLabels(labelType)
		if err != nil {
			return nil, err
		}
	}

	res := []Labeled{}
	for _, l := range allLabeled {
		if selector.Matches(l.Labels) {
			res = append(res, l)
		}
	}
	return res, nil
}

func (c *consulApplicator) ListLabels(labelType Type) ([]Labeled, error) {
	allLabeled := []Labeled{}
	allKV, _, err := c.kv.List(typePath(labelType)+"/", nil)
	if err != nil {
		return nil, err
	}
	for _, kvp := range allKV {
		l, err := convertKVPToLabeled(kvp)
		if err != nil {
			return nil, err
		}
		allLabeled = append(allLabeled, l)
	}

	if len(allKV) == 0 {
		return allLabeled, NoLabelsFound
	}
	return allLabeled, nil
}

// generalized label mutator function - pass nil value for any label to delete it
func (c *consulApplicator) mutateLabels(labelType Type, id string, labels map[string]*string) error {
	l, index, err := c.getLabels(labelType, id)
	if err != nil {
		return err
	}

	for key, value := range labels {
		if value == nil {
			delete(l.Labels, key)
		} else {
			l.Labels[key] = *value
		}
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

// TODO: replace mutateLabels() with this transaction-using implementation
func (c *consulApplicator) mutateLabelsTxn(txn *transaction.Tx, labelType Type, id string, labels map[string]*string) error {
	l, index, err := c.getLabels(labelType, id)
	if err != nil {
		return err
	}

	for key, value := range labels {
		if value == nil {
			delete(l.Labels, key)
		} else {
			l.Labels[key] = *value
		}
	}

	// TODO: we don't need convertLabeledToKVP to return an api.KVPair anymore
	// because we deconstruct it to put it in a transaction
	setkvp, err := convertLabeledToKVP(l)
	if err != nil {
		return err
	}

	var op api.KVTxnOp
	if len(l.Labels) == 0 {
		// still have to use CAS when deleting, to avoid discarding someone
		// else's concurrent modification
		// DeleteCAS ignores the value on the KVPair, so it doesn't matter if
		// we set it earlier
		op = api.KVTxnOp{
			Verb:  api.KVDeleteCAS,
			Key:   setkvp.Key,
			Index: index,
		}
	} else {
		op = api.KVTxnOp{
			Verb:  api.KVCAS,
			Key:   setkvp.Key,
			Value: setkvp.Value,
			Index: index,
		}
	}

	return txn.Add(op)
}

func labelsFromKeyValue(label string, value *string) map[string]*string {
	return map[string]*string{
		label: value,
	}
}

// this function will attempt to mutateLabel. if it gets a CAS error, then it
// will retry up to the number of attempts specified in c.Retries
func (c *consulApplicator) retryMutate(labelType Type, id string, labels map[string]*string) error {
	err := c.mutateLabels(labelType, id, labels)
	for i := 0; i < c.retries; i++ {
		if _, ok := err.(CASError); ok {
			err = c.mutateLabels(labelType, id, labels)
		} else {
			c.updateRetryCount(i)
			break
		}
	}
	return err
}

func (c *consulApplicator) updateRetryCount(retryMetric int) {
	c.retryMetric.Update(int64(retryMetric))
}

func (c *consulApplicator) SetLabel(labelType Type, id, label, value string) error {
	return c.retryMutate(labelType, id, labelsFromKeyValue(label, &value))
}

func (c *consulApplicator) SetLabels(labelType Type, id string, labels map[string]string) error {
	labelsToPointers := make(map[string]*string)
	for label, value := range labels {
		// We can't just use &value because that would be a pointer to
		// the iteration variable
		var valPtr string
		valPtr = value
		labelsToPointers[label] = &valPtr
	}
	return c.retryMutate(labelType, id, labelsToPointers)
}

// TODO: replace SetLabels() with this implementation. It's just separate right now to make
// exploring solutions require less code churn
func (c *consulApplicator) SetLabelsTxn(txn *transaction.Tx, labelType Type, id string, labels map[string]string) error {
	labelsToPointers := make(map[string]*string)
	for label, value := range labels {
		// We can't just use &value because that would be a pointer to
		// the iteration variable
		var valPtr string
		valPtr = value
		labelsToPointers[label] = &valPtr
	}

	return c.mutateLabelsTxn(txn, labelType, id, labelsToPointers)
}

func (c *consulApplicator) RemoveLabel(labelType Type, id, label string) error {
	return c.retryMutate(labelType, id, labelsFromKeyValue(label, nil))
}

func (c *consulApplicator) RemoveAllLabels(labelType Type, id string) error {
	path, err := objectPath(labelType, id)
	if err != nil {
		return err
	}
	_, err = c.kv.Delete(path, nil)
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

	path, err := objectPath(l.LabelType, l.ID)
	if err != nil {
		return nil, err
	}
	return &api.KVPair{
		Key:   path,
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
func (c *consulApplicator) WatchMatches(selector labels.Selector, labelType Type, quitCh <-chan struct{}) (chan []Labeled, error) {
	aggregator := c.initAggregator(labelType)
	return aggregator.Watch(selector, quitCh), nil
}

func (c *consulApplicator) initAggregator(labelType Type) *consulAggregator {
	c.aggregatorMux.Lock()
	defer c.aggregatorMux.Unlock()
	aggregator, ok := c.aggregators[labelType]
	if !ok {
		aggregator = NewConsulAggregator(labelType, c.kv, c.logger, c.metReg)
		go aggregator.Aggregate()
		c.aggregators[labelType] = aggregator
	}
	return aggregator
}

func (c *consulApplicator) WatchMatchDiff(
	selector labels.Selector,
	labelType Type,
	quitCh <-chan struct{},
) <-chan *LabeledChanges {
	inCh, _ := c.WatchMatches(selector, labelType, quitCh)
	return watchDiffLabels(inCh, quitCh, c.logger)
}

func watchDiffLabels(inCh <-chan []Labeled, quitCh <-chan struct{}, logger logging.Logger) <-chan *LabeledChanges {
	outCh := make(chan *LabeledChanges)

	go func() {
		defer close(outCh)
		oldLabels := make(map[string]Labeled)

		for {
			var results []Labeled
			select {
			case <-quitCh:
				return
			case val, ok := <-inCh:
				if !ok {
					// channel closed
					return
				}
				results = val
			}

			newLabels := make(map[string]Labeled)
			for _, labeled := range results {
				newLabels[labeled.ID] = labeled
			}

			outgoingChanges := &LabeledChanges{}
			for id, nodeLabel := range newLabels {
				if _, ok := oldLabels[id]; !ok {
					// If it was not observed, then it was created
					outgoingChanges.Created = append(outgoingChanges.Created, nodeLabel)
					oldLabels[id] = nodeLabel

				} else if oldLabels[id].Labels.String() != nodeLabel.Labels.String() {
					// If they are not equal, update them
					outgoingChanges.Updated = append(outgoingChanges.Updated, nodeLabel)
					oldLabels[id] = nodeLabel
				}
				// Otherwise no changes need to be made
			}

			for id, nodeLabel := range oldLabels {
				if _, ok := newLabels[id]; !ok {
					outgoingChanges.Deleted = append(outgoingChanges.Deleted, nodeLabel)
					delete(oldLabels, id)
				}
			}

			select {
			case <-quitCh:
				return
			case outCh <- outgoingChanges:
			}
		}
	}()

	return outCh
}

// these utility functions are used primarily while we exist in a mutable
// deployment world. We will need to figure out how to replace these with
// different datasources to allow RCs and DSs to continue to function correctly
// in the future.
func MakePodLabelKey(node types.NodeName, podID types.PodID) string {
	return node.String() + "/" + podID.String()
}

func NodeAndPodIDFromPodLabel(labeled Labeled) (types.NodeName, types.PodID, error) {
	if labeled.LabelType != POD {
		return "", "", util.Errorf("Label was not a pod label, was %s", labeled.LabelType)
	}

	parts := strings.SplitN(labeled.ID, "/", 2)
	if len(parts) < 2 {
		return "", "", util.Errorf("malformed pod label %s", labeled.ID)
	}

	return types.NodeName(parts[0]), types.PodID(parts[1]), nil
}

// confirm at compile time that consulApplicator is an implementation of the Applicator interface
var _ Applicator = &consulApplicator{}
