package labels

import (
	"sync"
	"time"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/logging"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

// minimum required time between retrievals of label subtrees.
var AggregationRateCap = 10 * time.Second

// linked list of watches with control channels
type selectorWatch struct {
	selector labels.Selector
	resultCh chan *[]Labeled
	canceled chan struct{}
	next     *selectorWatch
}

func (s *selectorWatch) append(w *selectorWatch) {
	if w == nil {
		return
	}
	if s.next != nil {
		s.next.append(w)
	} else {
		s.next = w
	}
}

func (s *selectorWatch) delete(w *selectorWatch) {
	if w == nil {
		return
	}
	if s.next == w {
		s.next = w.next
	} else if s.next != nil {
		s.next.delete(w)
	}
}

type consulAggregator struct {
	logger         logging.Logger
	labelType      Type
	watcherLock    sync.Mutex
	path           string
	kv             consulutil.ConsulLister
	watchers       *selectorWatch
	labeledCache   []Labeled // cached contents of the label subtree
	aggregatorQuit chan struct{}
}

func NewConsulAggregator(labelType Type, kv consulutil.ConsulLister, logger logging.Logger) *consulAggregator {
	return &consulAggregator{
		kv:             kv,
		logger:         logger,
		labelType:      labelType,
		path:           typePath(labelType),
		aggregatorQuit: make(chan struct{}),
	}
}

// Add a new selector to the aggregator. New values on the output channel may not appear
// right away.
func (c *consulAggregator) Watch(selector labels.Selector, quitCh chan struct{}) chan *[]Labeled {
	resCh := make(chan *[]Labeled)
	select {
	case <-c.aggregatorQuit:
		c.logger.WithField("selector", selector.String()).Warnln("New selector added after aggregator was closed")
		close(resCh)
		return resCh
	default:
	}
	c.watcherLock.Lock()
	defer c.watcherLock.Unlock()
	watch := &selectorWatch{
		selector: selector,
		resultCh: resCh,
		canceled: make(chan struct{}),
	}
	if c.watchers == nil {
		c.watchers = watch
	} else {
		c.watchers.append(watch)
	}
	go func() {
		select {
		case <-quitCh:
		case <-c.aggregatorQuit:
		}
		c.removeWatch(watch)
	}()
	return watch.resultCh
}

func (c *consulAggregator) removeWatch(watch *selectorWatch) {
	c.watcherLock.Lock()
	defer c.watcherLock.Unlock()
	close(watch.canceled)
	close(watch.resultCh)
	if c.watchers == watch {
		c.watchers = c.watchers.next
	} else {
		c.watchers.delete(watch)
	}
}

func (c *consulAggregator) Quit() {
	close(c.aggregatorQuit)
}

// Aggregate does the labor of querying Consul for all labels under a given type,
// applying each watcher's label selector to the results and sending those results on each
// watcher's output channel respectively.
// Aggregate will loop forever, constantly sending matches to each watcher
// until Quit() has been invoked.
func (c *consulAggregator) Aggregate() {
	outPairs := make(chan api.KVPairs)
	done := make(chan struct{})
	outErrors := make(chan error)
	go consulutil.WatchPrefix(c.path, c.kv, outPairs, done, outErrors)
	for {
		loopTime := time.After(AggregationRateCap)
		select {
		case <-c.aggregatorQuit:
			return
		case pairs := <-outPairs:
			// Convert watch result to []Labeled
			c.labeledCache = make([]Labeled, len(pairs))
			for i, kvp := range pairs {
				val, err := convertKVPToLabeled(kvp)
				if err != nil {
					c.logger.WithErrorAndFields(err, logrus.Fields{
						"key":   kvp.Key,
						"value": string(kvp.Value),
					}).Errorln("Invalid key encountered, skipping this value")
					continue
				}
				c.labeledCache[i] = val
			}

			// Iterate over each watcher and send the []Labeled
			// that match the watcher's selector to the watcher's out channel.
			c.watcherLock.Lock()
			watcher := c.watchers
			for watcher != nil {
				matches := []Labeled{}
				for _, labeled := range c.labeledCache {
					if watcher.selector.Matches(labeled.Labels) {
						matches = append(matches, labeled)
					}
				}
				select {
				case watcher.resultCh <- &matches:
				case <-watcher.canceled:
				}
				watcher = watcher.next
			}
			c.watcherLock.Unlock()
		}
		select {
		case <-c.aggregatorQuit:
			return
		case <-loopTime:
		}

	}
}
