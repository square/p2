package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/util/param"
	"github.com/square/p2/pkg/version"
	"github.com/square/p2/pkg/watch"
)

func main() {
	// Other packages define flags, and they need parsing here.
	kingpin.Parse()

	logger := logging.NewLogger(logrus.Fields{})
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		logger.NoFields().Fatalln("No CONFIG_PATH variable was given")
	}
	preparerConfig, err := preparer.LoadConfig(configPath)
	if err != nil {
		logger.WithError(err).Fatalln("could not load preparer config")
	}
	err = param.Parse(preparerConfig.Params)
	if err != nil {
		logger.WithError(err).Fatalln("invalid parameter")
	}

	statusServer, err := preparer.NewStatusServer(preparerConfig.StatusPort, preparerConfig.StatusSocket, &logger)
	if err == preparer.NoServerConfigured {
		logger.NoFields().Warningln("No status port or socket provided, no status server configured")
	} else if err != nil {
		logger.WithError(err).Fatalln("Could not start status server")
	} else {
		go statusServer.Serve()
		defer statusServer.Close()
	}

	prep, err := preparer.New(preparerConfig, logger)
	if err != nil {
		logger.WithError(err).Fatalln("Could not initialize preparer")
	}
	defer prep.Close()

	logger.WithFields(logrus.Fields{
		"starting":    true,
		"node_name":   preparerConfig.NodeName,
		"consul":      preparerConfig.ConsulAddress,
		"hooks_dir":   preparerConfig.HooksDirectory,
		"status_port": preparerConfig.StatusPort,
		"auth_type":   preparerConfig.Auth["type"],
		"keyring":     preparerConfig.Auth["keyring"],
		"version":     version.VERSION,
	}).Infoln("Preparer started successfully")

	quitMainUpdate := make(chan struct{})
	quitHookUpdate := make(chan struct{})
	quitChans := []chan struct{}{quitHookUpdate}

	// Guarantee that hooks are synced before any other pods are processed
	err = prep.SyncHooksOnce()
	if err != nil {
		logger.WithError(err).Fatalln("Could not do initial sync of hooks")
	}
	go prep.WatchForPodManifestsForNode(quitMainUpdate)
	go prep.WatchForHooks(quitHookUpdate)

	if prep.PodProcessReporter != nil {
		quitPodProcessReporter := make(chan struct{})
		quitChans = append(quitChans, quitPodProcessReporter)
		go prep.PodProcessReporter.Run(quitPodProcessReporter)
	}

	// Launch health checking watch. This watch tracks health of
	// all pods on this host and writes the information to consul
	quitMonitorPodHealth := make(chan struct{})
	var wgHealth sync.WaitGroup
	wgHealth.Add(1)
	go func() {
		defer wgHealth.Done()
		watch.MonitorPodHealth(preparerConfig, &logger, quitMonitorPodHealth)
	}()

	waitForTermination(logger, quitMainUpdate, quitChans)

	// The preparer should continue to report app health during a shutdown, so terminate
	// the health monitor last.
	close(quitMonitorPodHealth)
	wgHealth.Wait()

	logger.NoFields().Infoln("Terminating")
}

func waitForTermination(logger logging.Logger, quitMainUpdate chan struct{}, quitChans []chan struct{}) {
	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)
	received := <-signalCh
	logger.WithField("signal", received.String()).Infoln("Stopping work")
	for _, quitCh := range quitChans {
		quitCh <- struct{}{}
	}
	quitMainUpdate <- struct{}{}
	<-quitMainUpdate // acknowledgement
}
