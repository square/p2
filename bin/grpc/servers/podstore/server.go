package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/square/p2/pkg/grpc/podstore"
	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	kp_podstore "github.com/square/p2/pkg/kp/podstore"
	"github.com/square/p2/pkg/kp/statusstore"
	"github.com/square/p2/pkg/kp/statusstore/podstatus"

	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

type config struct {
	Port int `yaml:"port"`
}

const defaultPort = 3000

func main() {
	// Parse custom flags + standard Consul routing options
	_, opts, _ := flags.ParseWithConsulOptions()

	client := kp.NewConsulClient(opts)
	podStore := kp_podstore.NewConsul(client.KV())
	podStatusStore := podstatus.NewConsul(statusstore.NewConsul(client), kp.PreparerPodStatusNamespace)

	logger := log.New(os.Stderr, "", 0)
	port := getPort(logger)

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	podstore_protos.RegisterP2PodStoreServer(s, podstore.NewServer(podStore, podStatusStore))
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
