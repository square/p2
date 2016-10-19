package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/store_server"
	store_protos "github.com/square/p2/pkg/store_server/protos"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

type config struct {
	Port int `yaml:"port"`
}

const defaultPort = 3000

func main() {
	// Parse custom flags + standard Consul routing options
	_, opts := flags.ParseWithConsulOptions()

	client := kp.NewConsulClient(opts)
	kpStore := kp.NewConsulStore(client)

	logger := log.New(os.Stderr, "", 0)
	port := getPort(logger)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	store_protos.RegisterStoreServer(s, store_server.NewServer(kpStore))
	if err := s.Serve(lis); err != nil {
		logger.Fatalf("failed to serve: %v", err)
	}
}

func getPort(logger *log.Logger) int {
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
		logger.Fatal("Port must be set")
	}

	return config.Port
}
