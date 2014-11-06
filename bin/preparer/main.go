package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

type PreparerConfig struct {
	NodeName      string `yaml:"node_name"`
	ConsulAddress string `yaml:"consul_address"`
}

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		fmt.Println("No CONFIG_PATH variable was given")
		os.Exit(1)
		return
	}
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		fmt.Println(fmt.Sprintf("Could not read the config file at %s", err))
		os.Exit(1)
		return
	}
	preparerConfig := PreparerConfig{}
	err = yaml.Unmarshal(configBytes, &preparerConfig)
	if err != nil {
		fmt.Println(fmt.Sprintf("The config file was malformatted: %s", err))
		os.Exit(1)
		return
	}
	if preparerConfig.NodeName == "" {
		fmt.Println("`node_name` was not set in the file at CONFIG_PATH")
		os.Exit(1)
		return
	}
	if preparerConfig.ConsulAddress == "" {
		preparerConfig.ConsulAddress = "127.0.0.1:8500"
	}
	logFile, err := os.OpenFile("/tmp/platypus", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		os.Exit(1)
	}
	defer logFile.Close()

	watchForPodManifestsForNode(preparerConfig.NodeName, preparerConfig.ConsulAddress, logFile)
}
