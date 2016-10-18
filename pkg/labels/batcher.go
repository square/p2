package labels

import (
	"fmt"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
	p2metrics "github.com/square/p2/pkg/metrics"
)

type Batcher struct {
	createBatcherMux sync.Mutex
	applicator       Applicator
	holdTime         time.Duration
	typeBatchers     map[Type]*TypeBatcher
}

func NewBatcher(applicator Applicator, holdTime time.Duration) Batcher {
	return Batcher{
		applicator:   applicator,
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
			applicator:      b.applicator,
			holdTime:        b.holdTime,
			labelType:       labelType,
			pending:         []chan []Labeled{},
			queriesPerBatch: metrics.NewRegisteredHistogram(fmt.Sprintf("queries-per-%v-batch", labelType), p2metrics.Registry, metrics.NewUniformSample(1000)),
		}
		b.typeBatchers[labelType] = batcher
	}
	return batcher
}

// Batches a single label type
type TypeBatcher struct {
	applicator Applicator
	createMux  sync.Mutex
	labelType  Type

	holdTime        time.Duration
	pending         []chan []Labeled
	batchInProgress bool
	queriesPerBatch metrics.Histogram
}

func (b *TypeBatcher) handleBatch() {
	after := time.After(b.holdTime)
	allLabels, err := b.applicator.ListLabels(b.labelType)
	<-after
	b.createMux.Lock()
	defer b.createMux.Unlock()
	b.queriesPerBatch.Update(int64(len(b.pending)))
	for _, ch := range b.pending {
		if err != nil {
			close(ch)
		} else {
			ch <- allLabels
		}
	}
	b.pending = []chan []Labeled{}
	b.batchInProgress = false
}

func (b *TypeBatcher) Retrieve() ([]Labeled, error) {
	b.createMux.Lock()
	if !b.batchInProgress {
		b.batchInProgress = true
		go b.handleBatch()
	}
	respCh := make(chan []Labeled)
	b.pending = append(b.pending, respCh)
	b.createMux.Unlock()
	res, ok := <-respCh
	if !ok {
		return nil, fmt.Errorf("Could not retrieve results")
	}
	return res, nil
}
