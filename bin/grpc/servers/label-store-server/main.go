package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/square/p2/pkg/grpc/labelstore"
	label_protos "github.com/square/p2/pkg/grpc/labelstore/protos"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/flags"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
)

var (
	verbose = kingpin.Flag("verbose", "Enable verbose logging for the server").Bool()

	logger = log.New(os.Stderr, "", 0)
)

type config struct {
	Port int `yaml:"port"`
}

const defaultPort = 3000

func main() {
	// Parse custom flags + standard Consul routing options
	_, opts, _ := flags.ParseWithConsulOptions()

	logrusLogger := logging.DefaultLogger
	if *verbose {
		logrusLogger.Logger.Level = logrus.DebugLevel
	}
	client := consul.NewConsulClient(opts)
	applicator := labels.NewConsulApplicator(client, 1, 0)

	port := getPort()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}

	logrusLogger.Infof("Listening tcp on port %d", port)
	s := grpc.NewServer()
	label_protos.RegisterP2LabelStoreServer(s, labelstore.NewServer(applicator, logrusLogger))
	if err := s.Serve(lis); err != nil {
		logger.Fatalf("failed to serve: %v", err)
	}
}

func getPort() int {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		return defaultPort
	}

	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		logger.Fatal(err)
	}

	var config config
	err = yaml.Unmarshal(configBytes, &config)
	if err != nil {
		logger.Fatal(err)
	}

	if config.Port == 0 {
		return defaultPort
	}

	return config.Port
}
