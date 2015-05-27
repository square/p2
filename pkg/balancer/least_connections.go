package balancer

import (
	"math"
	"net"
	"sync"
	"time"

	"github.com/square/p2/pkg/util"
)

type lcBackend struct {
	conns int

	// disabling allows us to gracefully release connections without assigning
	// new connections to the backend.
	disabled bool
}

type LeastConnections struct {
	backends map[string]*lcBackend
	mapMux   sync.Mutex
}

func NewLeastConnectionsStrategy(initialAddresses []string) (*LeastConnections, error) {
	l := &LeastConnections{backends: make(map[string]*lcBackend)}
	for _, addr := range initialAddresses {
		err := l.AddAddress(addr)
		if err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *LeastConnections) Route(fn Handler) error {
	address, err := l.acquireAddress()
	if err != nil {
		return util.Errorf("Could not acquire %s: %s", address, err)
	}
	defer l.releaseAddress(address)
	outbound, err := net.Dial("tcp", address)
	if err != nil {
		return util.Errorf("Could not dial %s: %s", address, err)
	}
	defer outbound.Close()
	err = fn(outbound.(*net.TCPConn))
	if err != nil {
		return util.Errorf("Could not handle connection to %s: %s", address, err)
	}
	return nil
}

func (l *LeastConnections) RemoveAddress(address string) error {
	l.mapMux.Lock()
	defer l.mapMux.Unlock()
	backend, ok := l.backends[address]
	if !ok {
		return nil
	}
	backend.disabled = true
	return nil
}

func (l *LeastConnections) AddAddress(address string) error {
	l.mapMux.Lock()
	defer l.mapMux.Unlock()
	if backend, ok := l.backends[address]; ok {
		backend.disabled = false
		return nil
	}
	l.backends[address] = &lcBackend{
		0,
		false,
	}
	return nil
}

func (l *LeastConnections) acquireAddress() (string, error) {
	l.mapMux.Lock()
	defer l.mapMux.Unlock()
	minHost := ""
	minCount := math.MaxInt32
	for host, backend := range l.backends {
		if backend.conns < minCount && !backend.disabled {
			minCount = backend.conns
			minHost = host
		}
	}
	if minHost == "" {
		return "", util.Errorf("Could not find suitable backend. No backends were available")
	}
	l.backends[minHost].conns = minCount + 1
	return minHost, nil
}

func (l *LeastConnections) releaseAddress(address string) error {
	l.mapMux.Lock()
	defer l.mapMux.Unlock()

	backend, ok := l.backends[address]
	if !ok {
		return util.Errorf("Address %s not in set, cannot release", address)
	}
	backend.conns--
	return nil
}

// least connections is routable as long as there is at least one non-disabled
// backend
func (l *LeastConnections) WaitRoutable(duration time.Duration) error {
	after := time.After(duration)
	for {
		l.mapMux.Lock()
		for _, backend := range l.backends {
			if !backend.disabled {
				l.mapMux.Unlock()
				return nil
			}
		}
		l.mapMux.Unlock()
		select {
		case <-time.After(50 * time.Millisecond):
		case <-after:
			return util.Errorf("Exceeded timeout %v waiting for routability", duration)
		}
	}
	return nil
}
