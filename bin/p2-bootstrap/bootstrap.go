package main

import (
	"log"
	"os"
	"time"

	"github.com/square/p2/pkg/intent"
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
	agentManifest, err := pods.PodManifestFromPath(*agentManifestPath)
	if err != nil {
		log.Fatalln("Could not get agent manifest: %s", err)
	}
	log.Println("Installing and launching consul")

	if *existingConsul == "" {
		consulManifest, err := pods.PodManifestFromPath(*consulManifestPath)
		if err != nil {
			log.Fatalf("Could not get consul manifest: %s", err)
		}
		consulPod := pods.PodFromManifestId(consulManifest.ID())
		consulPod.RunAs = "root"
		err = InstallConsul(consulPod, consulManifest)
		if err != nil {
			log.Fatalf("Could not install consul: %s", err)
		}
	} else {
		log.Printf("Using existing Consul at %s\n", *existingConsul)
		agentManifest.Config["consul_address"] = *existingConsul
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

func InstallConsul(consulPod *pods.Pod, consulManifest *pods.PodManifest) error {
	// Inject servicebuilder?
	err := consulPod.Install(consulManifest)
	if err != nil {
		return util.Errorf("Can't install Consul, aborting: %s", err)
	}
	ok, err := consulPod.Launch(consulManifest)
	if err != nil || !ok {
		return util.Errorf("Can't launch Consul, aborting: %s", err)
	}
	time.Sleep(time.Second * 10)
	return nil
}

func RegisterBaseAgentInConsul(agentManifest *pods.PodManifest) error {
	store, err := intent.LookupStore(intent.Options{})
	if err != nil {
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	_, err = store.SetPod(hostname, *agentManifest)
	return err
}

func InstallBaseAgent(agentManifest *pods.PodManifest) error {
	agentPod := pods.PodFromManifestId(agentManifest.ID())
	agentPod.RunAs = "root"
	err := agentPod.Install(agentManifest)
	if err != nil {
		return err
	}
	_, err = agentPod.Launch(agentManifest)
	return err
}
