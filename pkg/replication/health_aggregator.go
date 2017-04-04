package replication

import (
	"sync"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/types"
)

type podHealth struct {
	podId   types.PodID
	checker checker.ConsulHealthChecker
	quit    chan struct{}

	cond      *sync.Cond // guards curHealth
	curHealth map[types.NodeName]health.Result
}

func AggregateHealth(id types.PodID, checker checker.ConsulHealthChecker, watchDelay time.Duration) *podHealth {
	p := &podHealth{
		podId:   id,
		checker: checker,
		cond:    sync.NewCond(&sync.Mutex{}),
		quit:    make(chan struct{}),
	}
	go p.beginWatch(watchDelay)

	// Wait for first update
	p.cond.L.Lock()
	for p.curHealth == nil {
		p.cond.Wait()
	}
	p.cond.L.Unlock()

	return p
}

func (p *podHealth) beginWatch(watchDelay time.Duration) {
	// TODO: hook up error reporting
	errCh := make(chan error)
	go func() {
		for range errCh {
		}
	}()

	resultCh := make(chan map[types.NodeName]health.Result)
	go p.checker.WatchService(p.podId.String(), resultCh, errCh, p.quit, watchDelay)

	// Always unblock AggregateHealth()
	defer func() {
		p.cond.L.Lock()
		defer p.cond.L.Unlock()
		if p.curHealth == nil {
			p.curHealth = make(map[types.NodeName]health.Result)
			p.cond.Broadcast()
		}
	}()

	for {
		select {
		case <-p.quit:
			return
		case res, ok := <-resultCh:
			if !ok {
				return
			}
			p.cond.L.Lock()
			p.cond.Broadcast()
			p.curHealth = res
			p.cond.L.Unlock()
		}
	}
}

func (p *podHealth) GetHealth(host types.NodeName) (health.Result, bool) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	h, ok := p.curHealth[host]
	return h, ok
}

func (p *podHealth) NumHealthyOf(hosts []types.NodeName) int {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	count := 0
	for _, host := range hosts {
		h, ok := p.curHealth[host]
		if ok && h.Status == health.Passing {
			count++
		}
	}
	return count
}

func (p *podHealth) Stop() {
	close(p.quit)
}
