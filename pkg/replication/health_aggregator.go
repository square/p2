package replication

import (
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"sync"
)

type podHealth struct {
	podId        types.PodID
	health       checker.ConsulHealthChecker
	healthAccess sync.Mutex
	curHealth    map[types.NodeName]health.Result
	hasResults   *sync.Cond
	quit         chan struct{}
}

func AggregateHealth(id types.PodID, checker checker.ConsulHealthChecker) podHealth {
	p := podHealth{podId: id, health: checker, hasResults: sync.NewCond(&sync.Mutex{})}
	p.hasResults.L.Lock()
	go p.beginWatch()
	p.hasResults.Wait()
	return p
}

func (p *podHealth) beginWatch() {
	errCh := make(chan error)
	quitCh := make(chan struct{})
	resultCh := make(chan map[types.NodeName]health.Result)
	go p.health.WatchService(p.podId.String(), resultCh, errCh, quitCh)
	for {
		select {
		case <-p.quit:
			close(quitCh)
		case res := <-resultCh:
			p.healthAccess.Lock()
			p.hasResults.Broadcast()
			p.curHealth = res
			p.healthAccess.Unlock()
		}
	}
}

func (p *podHealth) GetHealth(host types.NodeName) (health.Result, error) {
	p.healthAccess.Lock()
	defer p.healthAccess.Unlock()
	res, ok := p.curHealth[host]
	if !ok {
		return health.Result{}, util.Errorf("%v does not have health currently", host)
	}
	return res, nil
}

func (p *podHealth) Stop() {
	p.healthAccess.Lock()
	defer p.healthAccess.Unlock()
	close(p.quit)
}
