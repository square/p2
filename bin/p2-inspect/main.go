package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/intent"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/reality"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	consulUrl      = kingpin.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	filterNodeName = kingpin.Flag("node", "The node to inspect. By default, all nodes are shown.").String()
	filterPodId    = kingpin.Flag("pod", "The pod manifest ID to inspect. By default, all pods are shown.").String()
)

const (
	INTENT_SOURCE = iota
	REALITY_SOURCE
)

type NodePodStatus struct {
	NodeName           string      `json:"node,omitempty"`
	PodId              string      `json:"pod,omitempty"`
	IntentManifestSHA  string      `json:"intent_manifest_sha"`
	RealityManifestSHA string      `json:"reality_manifest_sha"`
	Health             *NodeHealth `json:"health_check,omitempty"`
}

type NodeHealth struct {
	Status string `json:"status"`
	Output string `json:"output"`
}

func main() {
	kingpin.Version("0.0.1")
	kingpin.Parse()

	client, err := consulapi.NewClient(&consulapi.Config{
		Address: *consulUrl,
	})
	if err != nil {
		log.Fatalf("Could not open consul client: %s", err)
	}

	intents, _, err := client.KV().List(intent.INTENT_TREE, nil)
	if err != nil {
		log.Fatalf("Could not list intent kvpairs: %s", err)
	}
	realities, _, err := client.KV().List(reality.REALITY_TREE, nil)
	if err != nil {
		log.Fatalf("Could not list reality kvpairs: %s", err)
	}

	statusMap := make(map[string]map[string]NodePodStatus)

	for _, kvp := range intents {
		if addKVPToMap(kvp, INTENT_SOURCE, *filterNodeName, *filterPodId, statusMap) != nil {
			log.Fatal(err)
		}
	}

	for _, kvp := range realities {
		if addKVPToMap(kvp, REALITY_SOURCE, *filterNodeName, *filterPodId, statusMap) != nil {
			log.Fatal(err)
		}
	}

	for podId := range statusMap {
		checks, _, err := client.Health().Checks(podId, nil)
		if err != nil {
			log.Fatalf("Could not retrieve health checks for pod %s: %s", podId, err)
		}

		for _, check := range checks {
			if *filterNodeName != "" && check.Node != *filterNodeName {
				continue
			}

			old := statusMap[podId][check.Node]
			old.Health = &NodeHealth{
				Status: check.Status,
				Output: check.Output,
			}
			statusMap[podId][check.Node] = old
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.Encode(statusMap)
}

func addKVPToMap(kvp *consulapi.KVPair, source int, filterNode, filterPod string, statuses map[string]map[string]NodePodStatus) error {
	keySegs := strings.Split(kvp.Key, "/")
	nodeName := keySegs[1]
	podId := keySegs[2]

	if filterNode != "" && nodeName != filterNode {
		return nil
	}
	if filterPod != "" && podId != filterPod {
		return nil
	}

	if statuses[podId] == nil {
		statuses[podId] = make(map[string]NodePodStatus)
	}
	old := statuses[podId][nodeName]

	manifest, err := pods.PodManifestFromBytes(kvp.Value)
	if err != nil {
		return err
	}
	manifestSHA, err := manifest.SHA()
	if err != nil {
		return err
	}

	switch source {
	case INTENT_SOURCE:
		if old.IntentManifestSHA != "" {
			return fmt.Errorf("Two intent manifests for node %s pod %s", nodeName, podId)
		}
		old.IntentManifestSHA = manifestSHA
	case REALITY_SOURCE:
		if old.RealityManifestSHA != "" {
			return fmt.Errorf("Two reality manifests for node %s pod %s", nodeName, podId)
		}
		old.RealityManifestSHA = manifestSHA
	}

	statuses[podId][nodeName] = old
	return nil
}
