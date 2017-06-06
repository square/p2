package labels

import (
	"fmt"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
	p2metrics "github.com/square/p2/pkg/metrics"
)

type labelLister interface {
	ListLabels(labelType Type) ([]Labeled, error)
}

type Batcher struct {
	createBatcherMux sync.Mutex
	lister           labelLister
	holdTime         time.Duration
	typeBatchers     map[Type]*TypeBatcher
}

func NewBatcher(lister labelLister, holdTime time.Duration) Batcher {
	return Batcher{
		lister:       lister,
		holdTime:     holdTime,
		typeBatchers: make(map[Type]*TypeBatcher),
	}
}

func (b *Batcher) ForType(labelType Type) *TypeBatcher {
	b.createBatcherMux.Lock()
	defer b.createBatcherMux.Unlock()
	batcher, ok := b.typeBatchers[labelType]
	if !ok {
		batcher = &TypeBatcher{
			lister:          b.lister,
			holdTime:        b.holdTime,
			labelType:       labelType,
			pending:         []chan batchResult{},
			queriesPerBatch: metrics.NewRegisteredHistogram(fmt.Sprintf("queries-per-%v-batch", labelType), p2metrics.Registry, metrics.NewUniformSample(1000)),
		}
		b.typeBatchers[labelType] = batcher
	}
	return batcher
}

// Batches a single label type
type TypeBatcher struct {
	lister    labelLister
	createMux sync.Mutex
	labelType Type

	holdTime        time.Duration
	pending         []chan batchResult
	batchInProgress bool
	queriesPerBatch metrics.Histogram

	// staleCache contains the full result set from the last time the
	// batcher was called. It can be used to serve "stale" queries when
	// quick responses are valued over consistent ones
	staleCache    batchResult
	staleCacheMux sync.Mutex
}

type batchResult struct {
	Matches []Labeled
	Err     error
}

func (b *TypeBatcher) handleBatch() {
	<-time.After(b.holdTime)
	b.createMux.Lock()
	handle := b.pending
	b.pending = []chan batchResult{}
	b.batchInProgress = false
	b.createMux.Unlock()

	allLabels, err := b.lister.ListLabels(b.labelType)
	res := batchResult{allLabels, err}
	b.staleCacheMux.Lock()
	b.staleCache = res
	b.staleCacheMux.Unlock()
	b.queriesPerBatch.Update(int64(len(handle)))
	for _, ch := range handle {
		ch <- res
		close(ch)
	}
}

func (b *TypeBatcher) Retrieve() ([]Labeled, error) {
	b.createMux.Lock()
	if !b.batchInProgress {
		b.batchInProgress = true
		go b.handleBatch()
	}
	respCh := make(chan batchResult)
	b.pending = append(b.pending, respCh)
	b.createMux.Unlock()
	res, ok := <-respCh
	if !ok {
		return nil, fmt.Errorf("Could not retrieve results, channel closed unexpectedly")
	}
	return res.Matches, res.Err
}

// RetrieveStale is like Retrieve() but returns the most recently completed
// query result rather than waiting for a new (consistent) query. This will
// return faster at the expense of getting stale results
func (b *TypeBatcher) RetrieveStale() ([]Labeled, error) {
	b.staleCacheMux.Lock()
	ret := b.staleCache
	b.staleCacheMux.Unlock()

	// Check that the cache has been populated, if not then let's wait for
	// one
	if ret.Matches == nil && ret.Err == nil {
		return b.Retrieve()
	}

	return ret.Matches, ret.Err
}

// RetrieveStaleByID is a convenience function for RetrieveStale() and then
// locating the Labeled with the specified ID in the []Labeled slice
func (b *TypeBatcher) RetrieveStaleByID(id string) (Labeled, error) {
	allLabeled, err := b.RetrieveStale()
	if err != nil {
		return Labeled{}, err
	}

	for _, labeled := range allLabeled {
		if labeled.ID == id {
			return labeled, nil
		}
	}

	return Labeled{}, fmt.Errorf("no record found for %s", id)
}
