package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/square/p2/pkg/kv-consul"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	consulManifestPath      = kingpin.Flag("consul-pod", "A path to the manifest that will be used to boot Consul.").ExistingFile()
	existingConsul          = kingpin.Flag("existing-consul-url", "A URL to an existing Consul server that will be supplied to the base agent's configuration").String()
	agentManifestPath       = kingpin.Flag("agent-pod", "A path to the manifest that will used to boot the base agent.").ExistingFile()
	additionalManifestsPath = kingpin.Flag("additional-pods", "(Optional) a directory of additional pods that will be launched and added to the intent store immediately").ExistingDir()
)

func main() {
	kingpin.Version("0.0.1")
	kingpin.Parse()
	log.Println("Starting bootstrap")
	consulManifest, err := pods.PodManifestFromPath(*consulManifestPath)
	if err != nil {
		log.Fatalf("Could not get consul manifest: %s", err)
	}
	consulPod := pods.NewPod(consulManifest)
	agentManifest, err := pods.PodManifestFromPath(*agentManifestPath)
	if err != nil {
		log.Fatalln("Could not get agent manifest: %s", err)
	}
	log.Println("Installing and launching consul")
	err = InstallConsul(consulPod)
	if err != nil {
		log.Fatalf("Could not install consul: %s", err)
	}
	log.Println("Registering base agent in consul")
	err = RegisterBaseAgentInConsul(agentManifest)
	if err != nil {
		log.Fatalf("Could not register base agent with consul: %s", err)
	}
	log.Println("Installing and launching base agent")
	err = InstallBaseAgent(agentManifest)
	if err != nil {
		log.Fatalf("Could not install base agent: %s", err)
	}
	log.Println("Bootstrapping complete")
}

func InstallConsul(consulPod *pods.Pod) error {
	// Inject servicebuilder?
	err := consulPod.Install()
	if err != nil {
		return util.Errorf("Can't install Consul, aborting: %s", err)
	}
	ok, err := consulPod.Launch()
	if err != nil || !ok {
		return util.Errorf("Can't launch Consul, aborting: %s", err)
	}
	time.Sleep(time.Second * 10)
	return nil
}

func RegisterBaseAgentInConsul(agentManifest *pods.PodManifest) error {
	client, err := ppkv.NewClient()
	if err != nil {
		return err
	}
	b := bytes.Buffer{}
	// TODO: pass consul URI as value in agentManifest config.
	// agentManifest.Config["consul_url"] = "localhost:8300"
	agentManifest.Write(&b)
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("nodes/%s/%s", hostname, agentManifest.Id)
	err = client.Put(endpoint, b.String())
	if err != nil {
		return util.Errorf("Could not PUT %s into %s: %s", b.String(), endpoint, err)
	}
	return nil
}

func InstallBaseAgent(agentManifest *pods.PodManifest) error {
	agentPod := pods.NewPod(agentManifest)
	err := agentPod.Install()
	if err != nil {
		return err
	}
	_, err = agentPod.Launch()
	return err
}
