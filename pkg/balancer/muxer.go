package balancer

import (
	"fmt"
	"net"
	"sync"
	"time"

	vhost "github.com/inconshreveable/go-vhost"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"
)

// VhostServer uses TLS virtual hosting using SNI to direct requests to
// an appropriate backend.
type VhostServer struct {
	muxed       map[string]Strategy
	monitor     Monitor
	logger      *logging.Logger
	toQuit      []chan struct{}
	strategyMux sync.Mutex
}

func NewVhostServer(monitor Monitor, logger *logging.Logger) *VhostServer {
	return &VhostServer{make(map[string]Strategy), monitor, logger, make([]chan struct{}, 0), sync.Mutex{}}
}

func (m *VhostServer) Serve(port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return util.Errorf("Could begin listening: %s", err)
	}
	if err != nil {
		return util.Errorf("Could not init muxer on port %d: %s", port, err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			m.logger.WithField("err", err).Warnln("Connection failed")
		}
		go m.handleConnection(conn)
	}
}

func (m *VhostServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	tlsConn, err := vhost.TLS(conn)
	if err != nil {
		m.logger.WithField("err", err).Errorln("Failed to recognize a TLS connection")
		return
	}
	defer tlsConn.Close()
	service := tlsConn.Host()
	strategy, err := m.determineStrategy(service)
	err = strategy.Route(DuplexConnection(tlsConn))
	if err != nil {
		m.logger.WithField("err", err).Errorln("Failed to route connection")
	}
}

func (m *VhostServer) determineStrategy(service string) (Strategy, error) {
	m.strategyMux.Lock()
	defer m.strategyMux.Unlock()
	strategy, ok := m.muxed[service]
	if !ok {
		var err error
		strategy, err = NewLeastConnectionsStrategy([]string{})
		if err != nil {
			return nil, err
		}
		monitorQuitCh := make(chan struct{})
		routableQuitCh := make(chan struct{})
		go m.monitor.MonitorHosts(service, strategy, monitorQuitCh)
		select {
		case <-time.After(time.Second * 15):
			monitorQuitCh <- struct{}{}
			routableQuitCh <- struct{}{}
			return nil, util.Errorf("Could not find routable backend to %s within timeout", service)
		case <-strategy.Routable(routableQuitCh):
			m.toQuit = append(m.toQuit, monitorQuitCh)
			m.muxed[service] = strategy
		}
	}

	return strategy, nil
}
