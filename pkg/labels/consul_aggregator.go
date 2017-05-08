package labels

import (
	"fmt"
	"sync"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/consulutil"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"k8s.io/kubernetes/pkg/labels"

	"github.com/rcrowley/go-metrics"
)

// minimum required time between retrievals of label subtrees.
var DefaultAggregationRate = 10 * time.Second

type MetricsRegistry interface {
	Register(metricName string, metric interface{}) error
}

// selectorWatch represents a watched label selector and its result channel
type selectorWatch struct {
	selector labels.Selector
	resultCh chan []Labeled
}

type consulAggregator struct {
	logger          logging.Logger
	labelType       Type
	watcherLock     sync.Mutex // watcherLock synchrnizes access to labeledCache and watchers
	path            string
	kv              consulutil.ConsulLister
	watchers        map[string]*selectorWatch
	labeledCache    []Labeled // cached contents of the label subtree
	aggregatorQuit  chan struct{}
	aggregationRate time.Duration

	metReg MetricsRegistry
	// how many watchers are currently using this aggregator?
	metWatchCount metrics.Gauge
	// count how many watcher channels are full when a send is attempted
	metWatchSendMiss metrics.Gauge
	// how big is the cache of labels?
	metCacheSize metrics.Gauge
}

func NewConsulAggregator(labelType Type, kv consulutil.ConsulLister, logger logging.Logger, metReg MetricsRegistry) *consulAggregator {
	if metReg == nil {
		metReg = metrics.NewRegistry()
	}
	watchCount := metrics.NewGauge()
	watchSendMiss := metrics.NewGauge()
	cacheSize := metrics.NewGauge()
	_ = metReg.Register(fmt.Sprintf("%v_aggregate_watches", labelType.String()), watchCount)
	_ = metReg.Register(fmt.Sprintf("%v_aggregate_send_miss", labelType.String()), watchSendMiss)
	_ = metReg.Register(fmt.Sprintf("%v_aggregate_cache_size", labelType.String()), cacheSize)

	return &consulAggregator{
		kv:               kv,
		logger:           logger,
		labelType:        labelType,
		path:             typePath(labelType),
		aggregatorQuit:   make(chan struct{}),
		aggregationRate:  DefaultAggregationRate,
		metReg:           metReg,
		metWatchCount:    watchCount,
		metWatchSendMiss: watchSendMiss,
		metCacheSize:     cacheSize,
		watchers:         make(map[string]*selectorWatch),
	}
}

// Add a new selector to the aggregator. New values on the output channel may not appear
// right away.
func (c *consulAggregator) Watch(selector labels.Selector, quitCh <-chan struct{}) chan []Labeled {
	resCh := make(chan []Labeled, 1) // this buffer is useful in sendMatches(), below
	select {
	case <-c.aggregatorQuit:
		c.logger.WithField("selector", selector.String()).Warnln("New selector added after aggregator was closed")
		close(resCh)
		return resCh
	default:
	}

	c.watcherLock.Lock()
	defer c.watcherLock.Unlock()
	watch, ok := c.watchers[selector.String()]
	if !ok {
		watch = &selectorWatch{
			selector: selector,
			resultCh: resCh,
		}

		c.watchers[selector.String()] = watch
	}

	if c.labeledCache != nil {
		c.sendMatches(watch)
	}
	go func() {
		select {
		case <-quitCh:
		case <-c.aggregatorQuit:
		}
		c.removeWatch(watch)
	}()
	c.metWatchCount.Update(int64(len(c.watchers)))
	return watch.resultCh
}

func (c *consulAggregator) removeWatch(watch *selectorWatch) {
	c.watcherLock.Lock()
	defer c.watcherLock.Unlock()
	close(watch.resultCh)
	delete(c.watchers, watch.selector.String())

	c.metWatchCount.Update(int64(len(c.watchers)))
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
	go consulutil.WatchPrefix(c.path+"/", c.kv, outPairs, done, outErrors, 0)
	for {
		missedSends := 0
		loopTime := time.After(c.aggregationRate)
		select {
		case err := <-outErrors:
			c.logger.WithError(err).Errorln("Error during watch")
		case <-c.aggregatorQuit:
			return
		case pairs := <-outPairs:
			if len(pairs) == 0 {
				// This protects us against spurious 404s from consul. It could
				// pose problems when the label type is something that might have
				// zero entries e.g. rolls, but for now there is no such use-case
				c.logger.WithError(NoLabelsFound).Errorf("No labels found for type %s", c.labelType)
				continue
			}
			c.watcherLock.Lock()

			// replace our current cache with the latest contents of the label tree.
			c.fillCache(pairs)

			// Iterate over each watcher and send the []Labeled
			// that match the watcher's selector to the watcher's out channel.
			var wg sync.WaitGroup
			missedSendsCh := make(chan struct{})

			missedSendsProcessed := make(chan struct{})
			go func() {
				defer close(missedSendsProcessed)
				for range missedSendsCh {
					missedSends++
				}
			}()

			for _, watcher := range c.watchers {
				wg.Add(1)
				go func(watch selectorWatch) {
					defer wg.Done()
					if !c.sendMatches(&watch) {
						missedSendsCh <- struct{}{}
					}
				}(*watcher)
			}
			wg.Wait()
			c.watcherLock.Unlock()

			close(missedSendsCh)
			<-missedSendsProcessed
			c.metWatchSendMiss.Update(int64(missedSends))
		}
		select {
		case <-c.aggregatorQuit:
			return
		case <-loopTime:
			// we purposely don't case outErrors here, since loopTime lets us
			// back off of Consul watches. If an error repeatedly were occurring,
			// we could end up in a nasty busy loop.
		}
	}
}

func (c *consulAggregator) getCache() ([]Labeled, error) {
	c.watcherLock.Lock()
	defer c.watcherLock.Unlock()
	if len(c.labeledCache) == 0 {
		return nil, fmt.Errorf("No cache available")
	}
	return c.labeledCache, nil
}

func (c *consulAggregator) fillCache(pairs api.KVPairs) {
	cache := make([]Labeled, len(pairs))
	for i, kvp := range pairs {
		labeled, err := convertKVPToLabeled(kvp)
		if err != nil {
			c.logger.WithErrorAndFields(err, logrus.Fields{
				"key":   kvp.Key,
				"value": string(kvp.Value),
			}).Errorln("Invalid key encountered, skipping this value")
			continue
		}
		cache[i] = labeled
	}
	c.labeledCache = cache
	c.metCacheSize.Update(int64(len(cache)))
}

// this must be called within the watcherLock mutex.
func (c *consulAggregator) sendMatches(watcher *selectorWatch) bool {
	matches := []Labeled{}
	for _, labeled := range c.labeledCache {
		if watcher.selector.Matches(labeled.Labels) {
			matches = append(matches, labeled)
		}
	}
	// Fast, lossy result broadcasting. We treat clients as unreliable
	// tenants of the aggregator by performing the following: the resulting
	// channel is a buffered channel of size 1. When sending an update to
	// watchers, we first see if we can _read_ a value off the buffered result
	// channel. This removes any stale values that have yet to be read by
	// watchers. We then subsequently put the newer value into the channel.
	//
	// This approach has the effect of a slower watcher potentially missing updates,
	// but also means one watcher can't cause a DoS of other watchers as the main
	// aggregation goroutine waits.

	// first drain the previous (stale) cached value if present...
	sendSuccess := true
	select {
	case <-watcher.resultCh:
		sendSuccess = false
	default:
	}
	// ... then send the newer value.
	select {
	case watcher.resultCh <- matches:
	default:
	}
	return sendSuccess
}

func selectorsEqual(sel1 labels.Selector, sel2 labels.Selector) bool {
	return sel1.String() == sel2.String()
}
