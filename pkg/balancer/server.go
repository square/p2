package balancer

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"
)

type Handler func(outbound net.Conn) error

type Strategy interface {
	// Route is expected to synchronously call the given Handle with an outbound connection.
	Route(fn Handler) error
	AddAddress(addr string) error
	RemoveAddress(addr string) error
	WaitRoutable(wait time.Duration) error
}

type Monitor interface {
	MonitorHosts(service string, strategy Strategy, quitCh <-chan struct{})
}

type server struct {
	logger  *logging.Logger
	monitor Monitor
}

func DuplexConnection(inbound net.Conn) Handler {
	return func(outbound net.Conn) error {
		done := make(chan struct{})
		defer inbound.Close()
		defer outbound.Close()

		go func() {
			io.Copy(outbound, inbound)
			close(done)
		}()
		io.Copy(inbound, outbound)
		<-done
		return nil
	}
}

func NewServer(monitor Monitor, logger *logging.Logger) *server {
	return &server{
		logger:  logger,
		monitor: monitor,
	}
}

func (s *server) Serve(service string, port int, strategy Strategy) error {
	quitMonitoring := make(chan struct{})
	go s.monitor.MonitorHosts(service, strategy, quitMonitoring)
	defer func() {
		quitMonitoring <- struct{}{}
	}()
	inAddr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", inAddr)
	if err != nil {
		return util.Errorf("Couldn't listen on address %s: %s", inAddr, err)
	}

	for {
		inbound, err := listener.Accept()
		if err != nil {
			return util.Errorf("Failed to accept inbound connection: %s", err)
		}
		go s.handleConnection(inbound, strategy)
	}
}

func (s *server) handleConnection(inbound net.Conn, strategy Strategy) {
	defer inbound.Close()
	err := strategy.Route(DuplexConnection(inbound.(*net.TCPConn)))
	if err != nil {
		s.logger.WithField("err", err).Errorln("Error routing request: %s", err)
	}
}
