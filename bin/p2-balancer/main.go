package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/balancer"
	"github.com/square/p2/pkg/config"
	"github.com/square/p2/pkg/logging"
)

// serpro (service proxy) is a simple TCP proxy for services registered
// in the Consul registry.

type balancerConfig struct {
	serviceToProxy string `yaml:"service_to_proxy"`
	vhost          string `yaml:"vhost"`
	port           int    `yaml:"port"`
}

func main() {
	cfg := &balancerConfig{}
	err := config.LoadFromEnvInto(cfg)

	logger := logging.DefaultLogger

	if err != nil {
		logger.WithField("err", err).Fatalf("Could not load configuration for p2-balancer")
	}

	if cfg.serviceToProxy == "" && cfg.vhost == "" {
		logger.NoFields().Fatalln("Did not specify a service or vhost key in config")
	}

	if cfg.port == 0 {
		cfg.port = 443
	}

	monitor, err := balancer.NewConsulMonitor(api.DefaultConfig(), &logger)

	if err != nil {
		logger.NoFields().Fatalln(err)
	}

	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)

	if cfg.serviceToProxy != "" {
		err = doServer(cfg.serviceToProxy, cfg.port, signalCh, monitor)
	} else {
		err = doVhost(cfg.port, signalCh, monitor)
	}
	if err != nil {
		logger.WithField("err", err).Fatalln("Error running")
	}
}

func doServer(service string, port int, signalCh chan os.Signal, monitor balancer.Monitor) error {
	logger := logging.NewLogger(logrus.Fields{
		"service": service,
	})
	logger.WithField("port", port).Infoln("Serving TLS traffic for service")

	server := balancer.NewServer(monitor, &logger)

	errCh := make(chan error)
	go func() {
		leastConnections, _ := balancer.NewLeastConnectionsStrategy([]string{})
		err := server.Serve(service, port, leastConnections)
		errCh <- err
	}()
	select {
	// add cleanup here
	case err := <-errCh:
		return err
	case <-signalCh:
		return nil
	}
}

func doVhost(port int, signalCh chan os.Signal, monitor balancer.Monitor) error {
	logger := logging.NewLogger(logrus.Fields{
		"vhost": true,
	})
	logger.WithField("port", port).Infoln("Serving TLS traffic for all registered services")
	vhostServer := balancer.NewVhostServer(monitor, &logger)
	defer vhostServer.Cleanup()
	errCh := make(chan error)
	go func() {
		err := vhostServer.Serve(port)
		errCh <- err
	}()
	select {
	// add cleanup here
	case err := <-errCh:
		return err
	case <-signalCh:
		return nil
	}
}
