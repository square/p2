package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util/net"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	consulUrl      = kingpin.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	filterNodeName = kingpin.Flag("node", "The node to inspect. By default, all nodes are shown.").String()
	filterPodId    = kingpin.Flag("pod", "The pod manifest ID to inspect. By default, all pods are shown.").String()
	consulToken    = kingpin.Flag("token", "The consul ACL token to use. Empty by default.").String()
	headers        = kingpin.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
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
	IntentLocations    []string    `json:"intent_locations"`
	RealityLocations   []string    `json:"reality_locations"`
	Health             *NodeHealth `json:"health_check,omitempty"`
}

type NodeHealth struct {
	Status string `json:"status"`
	Output string `json:"output"`
}

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	httpc := net.NewHeaderClient(*headers)
	store := kp.NewStore(kp.Options{
		Address: *consulUrl,
		Token:   *consulToken,
		Client:  httpc,
	})

	intents, _, err := store.ListPods(kp.INTENT_TREE)
	if err != nil {
		log.Fatalf("Could not list intent kvpairs: %s", err)
	}
	realities, _, err := store.ListPods(kp.REALITY_TREE)
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

	// error is always nil
	client, _ := api.NewClient(&api.Config{
		Address:    *consulUrl,
		Token:      *consulToken, // this is not actually needed because /health endpoints are unACLed
		HttpClient: httpc,
	})
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

func addKVPToMap(result kp.ManifestResult, source int, filterNode, filterPod string, statuses map[string]map[string]NodePodStatus) error {
	keySegs := strings.Split(result.Path, "/")
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

	manifestSHA, err := result.Manifest.SHA()
	if err != nil {
		return err
	}

	switch source {
	case INTENT_SOURCE:
		if old.IntentManifestSHA != "" {
			return fmt.Errorf("Two intent manifests for node %s pod %s", nodeName, podId)
		}
		old.IntentManifestSHA = manifestSHA
		for _, launchable := range result.Manifest.LaunchableStanzas {
			old.IntentLocations = append(old.IntentLocations, launchable.Location)
		}
	case REALITY_SOURCE:
		if old.RealityManifestSHA != "" {
			return fmt.Errorf("Two reality manifests for node %s pod %s", nodeName, podId)
		}
		old.RealityManifestSHA = manifestSHA
		for _, launchable := range result.Manifest.LaunchableStanzas {
			old.RealityLocations = append(old.RealityLocations, launchable.Location)
		}
	}

	statuses[podId][nodeName] = old
	return nil
}
