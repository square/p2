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

// vhostServer uses TLS virtual hosting using SNI to direct requests to
// an appropriate backend.
type vhostServer struct {
	muxed       map[string]Strategy
	monitor     Monitor
	logger      *logging.Logger
	toQuit      []chan struct{}
	strategyMux sync.Mutex
}

func NewVhostServer(monitor Monitor, logger *logging.Logger) *vhostServer {
	return &vhostServer{make(map[string]Strategy), monitor, logger, make([]chan struct{}, 0), sync.Mutex{}}
}

func (m *vhostServer) Serve(port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return util.Errorf("Could not init muxer on port %d: %s", port, err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			m.logger.WithField("err", err).Warnln("Connection failed")
			continue
		}
		go m.handleConnection(conn)
	}
}

func (m *vhostServer) Cleanup() {
	for _, q := range m.toQuit {
		close(q)
	}
}

func (m *vhostServer) handleConnection(conn net.Conn) {
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

// Find an existing strategy or initialize a new one for the given service
func (m *vhostServer) determineStrategy(service string) (Strategy, error) {
	m.strategyMux.Lock()
	defer m.strategyMux.Unlock()
	strategy, ok := m.muxed[service]
	if !ok {
		var err error
		strategy, err = NewLeastConnectionsStrategy([]string{})
		if err != nil {
			return nil, err
		}
		// Wait for 15 seconds for routability while starting the monitoring process
		monitorQuitCh := make(chan struct{})
		routableCh := make(chan struct{})
		routableErrCh := make(chan error)
		go func() {
			err := strategy.WaitRoutable(15 * time.Second)
			if err != nil {
				routableErrCh <- err
			} else {
				routableCh <- struct{}{}
			}
		}()
		go m.monitor.MonitorHosts(service, strategy, monitorQuitCh)
		select {
		case err = <-routableErrCh:
			monitorQuitCh <- struct{}{}
			return nil, util.Errorf("Could not find routable backend to %s: %s", service, err)
		case <-routableCh:
			m.toQuit = append(m.toQuit, monitorQuitCh)
			m.muxed[service] = strategy
		}
	}

	return strategy, nil
}
