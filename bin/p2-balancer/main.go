package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/balancer"
	"github.com/square/p2/pkg/config"
	"github.com/square/p2/pkg/logging"
)

// serpro (service proxy) is a simple TCP proxy for services registered
// in the Consul registry.

func main() {
	cfg, err := config.LoadFromEnvironment()
	logger := logging.DefaultLogger

	if err != nil {
		logger.NoFields().Fatalf("Could not init serpro: %s", err)
	}

	service, err := cfg.ReadString("service_to_proxy")
	if err != nil {
		logger.WithField("err", err).Fatalln("Could not get the service to proxy")
	}

	vhost, err := cfg.ReadString("vhost")

	if service == "" && vhost == "" {
		logger.NoFields().Fatalln("Did not specify a service or vhost key in config")
	}

	port, err := cfg.ReadInt("port")
	if err != nil {
		logger.WithField("err", err).Fatalln("Could not get port number")
	}
	if port == 0 {
		port = 443
	}

	monitor, err := balancer.NewConsulMonitor(api.DefaultConfig(), &logger)

	if err != nil {
		logger.NoFields().Fatalln(err)
	}

	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)

	if service != "" {
		err = doServer(service, port, signalCh, monitor)
	} else {
		err = doVhost(port, signalCh, monitor)
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
