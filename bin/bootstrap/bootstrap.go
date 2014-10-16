package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/square/p2/pkg/kv-consul"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	consulManifestPath      = kingpin.Flag("consul-pod", "A path to the manifest that will be used to boot Consul.").ExistingFile()
	existingConsul          = kingpin.Flag("existing-consul-url", "A URL to an existing Consul server that will be supplied to the base agent's configuration")
	agentManifestPath       = kingpin.Flag("agent-pod", "A path to the manifest that will used to boot the base agent.").ExistingFile()
	additionalManifestsPath = kingpin.Flag("additional-pods", "(Optional) a directory of additional pods that will be launched and added to the intent store immediately").ExistingDir()
)

func main() {
	kingpin.Version("0.0.1")
	kingpin.Parse()

	consulManifest, err := pods.PodManifestFromPath(*consulManifestPath)
	consulPod := pods.PodFromManifest(consulManifest)
	agentManifest, err := pods.PodManifestFromPath(*agentManifestPath)

	InstallConsul(consulPod)
	RegisterBaseAgentInConsul(agentManifest)
	InstallBaseAgent(agentManifest)
	AdditionalPods(*additionalManifests)
}

func InstallConsul(consulPod *pods.Pod) error {
	// Inject servicebuilder?
	err := consulPod.Install()
	if err != nil {
		return util.Errorf("Can't install Consul, aborting: %s", err)
	}
	err = consulPod.Launch()
	if err != nil {
		return util.Errorf("Can't launch Consul, aborting: %s", err)
	}
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
	client.Put(fmt.Sprintf("/nodes/%s/%s", hostname, agentManifest.Id), b.String())
	return nil
}

func InstallBaseAgent(agentManifest *pods.PodManifest) error {
	agentPod := pods.PodFromManifest(agentManifest)
	err := agentPod.Install()
	if err != nil {
		return err
	}
	err = agentPod.Launch()
	if err != nil {
		return err
	}
	return nil
}
