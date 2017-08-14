package labels

import (
	"context"
	"sync"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"

	"k8s.io/kubernetes/pkg/labels"
)

// This is a map of type -> id -> Set
// equivalently, of type -> id -> key -> value
type fakeApplicatorData map[Type]map[string]labels.Set

type fakeApplicator struct {
	// KV data that will be returned by queries
	data fakeApplicatorData
	// since entry() may mutate the map, every read can potentially trigger a
	// write. no point using rwmutex here
	mutex sync.Mutex
}

var _ Applicator = &fakeApplicator{}

func NewFakeApplicator() *fakeApplicator {
	return &fakeApplicator{data: make(fakeApplicatorData)}
}

func (app *fakeApplicator) entry(labelType Type, id string) map[string]string {
	if _, ok := app.data[labelType]; !ok {
		app.data[labelType] = make(map[string]labels.Set)
	}
	forType := app.data[labelType]
	if _, ok := forType[id]; !ok {
		forType[id] = make(labels.Set)
	}
	return forType[id]
}

func (app *fakeApplicator) SetLabel(labelType Type, id, name, value string) error {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	entry := app.entry(labelType, id)
	entry[name] = value
	return nil
}

func (app *fakeApplicator) SetLabels(labelType Type, id string, labels map[string]string) error {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	entry := app.entry(labelType, id)
	for k, v := range labels {
		entry[k] = v
	}
	return nil
}

func (app *fakeApplicator) SetLabelsTxn(ctx context.Context, labelType Type, id string, labels map[string]string) error {
	return util.Errorf("SetLabelsTxn not implemented in fake applicator. Use a real applicator")
}

func (app *fakeApplicator) RemoveAllLabels(labelType Type, id string) error {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	delete(app.data[labelType], id)
	return nil
}

// Implemented to satisfy interface constraints, but no test should be using
// this. Instead, use a real consul store using consulutil.NewFixture()
func (app *fakeApplicator) RemoveAllLabelsTxn(ctx context.Context, labelType Type, id string) error {
	panic("not implemented")
}

func (app *fakeApplicator) RemoveLabel(labelType Type, id, name string) error {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	entry := app.entry(labelType, id)
	delete(entry, name)
	return nil
}

func (app *fakeApplicator) RemoveLabelTxn(ctx context.Context, labelType Type, id string, name string) error {
	return util.Errorf("RemoveLabelTxn not implemented in fake applicator. use a real applicator")
}

func (app *fakeApplicator) RemoveLabelsTxn(ctx context.Context, labelType Type, id string, keysToRemove []string) error {
	panic("not implemented")
}

func (app *fakeApplicator) ListLabels(labelType Type) ([]Labeled, error) {
	res := []Labeled{}
	for id, set := range app.data[labelType] {
		res = append(res, Labeled{
			ID:        id,
			LabelType: labelType,
			Labels:    copySet(set),
		})
	}
	return res, nil
}

func (app *fakeApplicator) GetLabels(labelType Type, id string) (Labeled, error) {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	entry := app.entry(labelType, id)
	return Labeled{
		ID:        id,
		LabelType: labelType,
		Labels:    copySet(entry),
	}, nil
}

func (app *fakeApplicator) GetLabelsWithIndex(Type, string) (Labeled, uint64, error) {
	return Labeled{}, 0, util.Errorf("get labels with index not implemented in fake label store, use a real consul instance")
}

func (app *fakeApplicator) GetMatches(selector labels.Selector, labelType Type) ([]Labeled, error) {
	return app.getMatches(selector, labelType)
}

func (app *fakeApplicator) GetCachedMatches(selector labels.Selector, labelType Type, aggregationRate time.Duration) ([]Labeled, error) {
	return app.getMatches(selector, labelType)
}

func (app *fakeApplicator) getMatches(selector labels.Selector, labelType Type) ([]Labeled, error) {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	forType, ok := app.data[labelType]
	if !ok {
		return []Labeled{}, nil
	}

	results := []Labeled{}

	for id, set := range forType {
		if selector.Matches(set) {
			results = append(results, Labeled{
				ID:        id,
				LabelType: labelType,
				Labels:    copySet(set),
			})
		}
	}

	return results, nil
}

func (app *fakeApplicator) WatchMatches(selector labels.Selector, labelType Type, agregationRate time.Duration, quitCh <-chan struct{}) (chan []Labeled, error) {
	ch := make(chan []Labeled)
	go func() {
		for {
			select {
			case <-quitCh:
				return
			default:
			}

			res, _ := app.GetMatches(selector, labelType)

			select {
			case <-quitCh:
				return
			case ch <- res:
			}
		}
	}()
	return ch, nil
}

func (app *fakeApplicator) WatchMatchDiff(
	selector labels.Selector,
	labelType Type,
	aggregationRate time.Duration,
	quitCh <-chan struct{},
) <-chan *LabeledChanges {
	inCh, _ := app.WatchMatches(selector, labelType, aggregationRate, quitCh)
	return watchDiffLabels(inCh, quitCh, logging.DefaultLogger)
}

// avoid returning elements of the inner data map, otherwise concurrent callers
// may cause races when mutating them
func copySet(in labels.Set) labels.Set {
	ret := make(labels.Set, len(in))
	for k, v := range in {
		ret[k] = v
	}
	return ret
}
