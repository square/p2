package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/version"
	"github.com/square/p2/pkg/watch"
)

func main() {
	var token string
	logger := logging.NewLogger(logrus.Fields{})
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		logger.NoFields().Fatalln("No CONFIG_PATH variable was given")
	}
	preparerConfig, err := preparer.LoadPreparerConfig(configPath)
	if err != nil {
		logger.WithField("inner_err", err).Fatalln("could not load preparer config")
	}

	prep, err := preparer.New(preparerConfig, logger)
	if err != nil {
		logger.WithField("inner_err", err).Fatalln("Could not initialize preparer")
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
	quitWatchHealth := make(chan struct{})
	if preparerConfig.ConsulTokenPath != "" {
		token, err = preparer.LoadConsulToken(preparerConfig.ConsulTokenPath)
		if err != nil {
			logger.WithField("inner_err", err).Fatalln("Could not load consul token")
		}
	} else {
		token = ""
	}

	go prep.WatchForPodManifestsForNode(quitMainUpdate)
	go prep.WatchForHooks(quitHookUpdate)

	// Launch health checking watch. This watch tracks health of
	// all pods on this host and writes the information to consul
	go watch.WatchHealth("localhost:8500",
		preparerConfig.ConsulAddress,
		token,
		&logger,
		quitWatchHealth)

	if preparerConfig.StatusPort != 0 {
		http.HandleFunc("/_status",
			func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, "p2-preparer OK")
			})
		go http.ListenAndServe(fmt.Sprintf(":%d", preparerConfig.StatusPort), nil)
	}

	waitForTermination(logger, quitMainUpdate, quitHookUpdate, quitWatchHealth)

	logger.NoFields().Infoln("Terminating")
}

func waitForTermination(logger logging.Logger, quitMainUpdate, quitHookUpdate chan struct{}, quitWatchHealth chan struct{}) {
	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)
	received := <-signalCh
	logger.WithField("signal", received.String()).Infoln("Stopping work")
	quitHookUpdate <- struct{}{}
	quitMainUpdate <- struct{}{}
	quitWatchHealth <- struct{}{}
	<-quitMainUpdate // acknowledgement
}
