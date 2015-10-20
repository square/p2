package preparer

import (
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/square/p2/pkg/logging"
)

type StatusServer struct {
	listener net.Listener
	server   *http.Server
	logger   *logging.Logger
	Exit     chan error
}

func (s *StatusServer) Close() error {
	return s.listener.Close()
}

var NoServerConfigured = fmt.Errorf("No status server was configured")

func NewStatusServer(statusPort int, statusSocket string, logger *logging.Logger) (*StatusServer, error) {
	server := http.Server{}
	statusServer := &StatusServer{
		server: &server,
		logger: logger,
		Exit:   make(chan error),
	}
	var listener net.Listener
	var err error

	if statusPort != 0 {
		listener, err = statusServer.listenOnPort(statusPort)
		if err != nil {
			return nil, err
		}
	} else if statusSocket != "" {
		listener, err = statusServer.listenOnSocket(statusSocket)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, NoServerConfigured
	}

	statusServer.listener = listener

	return statusServer, nil
}

func (s *StatusServer) Serve() {
	defer s.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("/_status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "p2-preparer OK")
	})

	s.server.Handler = mux
	err := s.server.Serve(s.listener)
	s.logger.WithError(err).Warnln("Status server exited!")
	s.Exit <- err
	close(s.Exit)
}

func (s *StatusServer) listenOnPort(statusPort int) (net.Listener, error) {
	s.logger.WithField("port", statusPort).Infof("Reporting status on port %d", statusPort)
	return net.Listen("tcp", fmt.Sprintf(":%d", statusPort))
}

func (s *StatusServer) listenOnSocket(socket string) (net.Listener, error) {
	if _, err := os.Stat(socket); err == nil {
		s.logger.WithField("socket", socket).Warningln("Previous socket was not removed, removing")
		err = os.Remove(socket)
		if err != nil {
			s.logger.WithError(err).Fatalln("Could not remove existing socket!")
		}
	}
	s.logger.WithField("socket", socket).Infof("Reporting status on socket %s", socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	err = os.Chmod(socket, 0777)
	if err != nil {
		return nil, err
	}
	return listener, nil
}
