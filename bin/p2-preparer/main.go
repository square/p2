package main

import (
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/version"
	"golang.org/x/crypto/openpgp"
	"gopkg.in/yaml.v2"
)

type AppConfig struct {
	P2PreparerConfig PreparerConfig `yaml:"preparer"`
}

type LogDestination struct {
	Type logging.OutType `yaml:"type"`
	Path string          `yaml:"path"`
}

type PreparerConfig struct {
	NodeName             string           `yaml:"node_name"`
	ConsulAddress        string           `yaml:"consul_address"`
	HooksDirectory       string           `yaml:"hooks_directory"`
	KeyringPath          string           `yaml:"keyring,omitempty"`
	ExtraLogDestinations []LogDestination `yaml:"extra_log_destinations,omitempty"`
}

func main() {
	logger := logging.NewLogger(logrus.Fields{})
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		logger.NoFields().Errorln("No CONFIG_PATH variable was given")
		os.Exit(1)
		return
	}
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"inner_err": err,
		}).Errorln("Could not read the config file")
		os.Exit(1)
		return
	}
	appConfig := AppConfig{}
	err = yaml.Unmarshal(configBytes, &appConfig)
	preparerConfig := appConfig.P2PreparerConfig
	if err != nil {
		logger.WithFields(logrus.Fields{
			"inner_err": err,
		}).Errorln("The config file was malformatted")
		os.Exit(1)
		return
	}
	for _, dest := range preparerConfig.ExtraLogDestinations {
		logger.WithFields(logrus.Fields{
			"type": dest.Type,
			"path": dest.Path,
		}).Infoln("Adding log destination")
		logger.AddHook(dest.Type, dest.Path)
	}
	if preparerConfig.NodeName == "" {
		preparerConfig.NodeName, _ = os.Hostname()
	}
	if preparerConfig.ConsulAddress == "" {
		preparerConfig.ConsulAddress = "127.0.0.1:8500"
	}
	if preparerConfig.HooksDirectory == "" {
		preparerConfig.HooksDirectory = hooks.DEFAULT_PATH
	}

	keyring, err := loadKeyring(preparerConfig.KeyringPath)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"inner_err":    err,
			"keyring_path": preparerConfig.KeyringPath,
		}).Errorln("Could not load preparer keyring")
		os.Exit(1)
	}

	prep, err := preparer.New(preparerConfig.NodeName, preparerConfig.ConsulAddress, preparerConfig.HooksDirectory, logger, keyring)
	if err != nil {
		logger.WithField("inner_err", err).Errorln("Could not initialize preparer")
		os.Exit(1)
	}

	err = os.MkdirAll(pods.DEFAULT_PATH, 0755)
	if err != nil {
		logger.WithField("inner_err", err).Errorln("Could not create preparer pod directory")
		os.Exit(1)
	}

	logger.WithFields(logrus.Fields{
		"starting":  true,
		"node_name": preparerConfig.NodeName,
		"consul":    preparerConfig.ConsulAddress,
		"hooks_dir": preparerConfig.HooksDirectory,
		"keyring":   preparerConfig.KeyringPath,
		"version":   version.VERSION,
	}).Infoln("Preparer started successfully")

	quitMainUpdate := make(chan struct{})
	quitHookUpdate := make(chan struct{})
	go prep.WatchForPodManifestsForNode(quitMainUpdate)
	go prep.WatchForHooks(quitHookUpdate)

	waitForTermination(logger, quitMainUpdate, quitHookUpdate)

	logger.NoFields().Infoln("Terminating")
}

func waitForTermination(logger logging.Logger, quitMainUpdate, quitHookUpdate chan struct{}) {
	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)
	received := <-signalCh
	logger.WithField("signal", received.String()).Infoln("Stopping work")
	quitHookUpdate <- struct{}{}
	quitMainUpdate <- struct{}{}
	<-quitMainUpdate // acknowledgement
}

func loadKeyring(path string) (openpgp.KeyRing, error) {
	if path == "" {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return openpgp.ReadKeyRing(f)
}
