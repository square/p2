package replication

import (
	"sync"

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

func AggregateHealth(id types.PodID, checker checker.ConsulHealthChecker) *podHealth {
	p := &podHealth{
		podId:   id,
		checker: checker,
		cond:    sync.NewCond(&sync.Mutex{}),
	}
	go p.beginWatch()

	// Wait for first update
	p.cond.L.Lock()
	for p.curHealth == nil {
		p.cond.Wait()
	}
	p.cond.L.Unlock()

	return p
}

func (p *podHealth) beginWatch() {
	// TODO: hook up error reporting
	errCh := make(chan error)
	go func() {
		for range errCh {
		}
	}()

	resultCh := make(chan map[types.NodeName]health.Result)
	go p.checker.WatchService(p.podId.String(), resultCh, errCh, p.quit)

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

func (p *podHealth) Stop() {
	close(p.quit)
}
