package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/inspect"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
)

var (
	nodeArg = kingpin.Flag("node", "The node to inspect. By default, all nodes are shown.").String()
	podArg  = kingpin.Flag("pod", "The pod manifest ID to inspect. By default, all pods are shown.").String()
	format  = kingpin.Flag("format", "Display format").Default("tree").Enum("tree", "list")
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(opts)
	store := kp.NewConsulStore(client)

	var intents []kp.ManifestResult
	var realities []kp.ManifestResult
	var err error
	filterNodeName := types.NodeName(*nodeArg)
	filterPodID := types.PodID(*podArg)

	if filterNodeName != "" {
		intents, _, err = store.ListPods(kp.INTENT_TREE, filterNodeName)
	} else {
		intents, _, err = store.AllPods(kp.INTENT_TREE)
	}
	if err != nil {
		message := "Could not list intent kvpairs: %s"
		if kvErr, ok := err.(consulutil.KVError); ok {
			log.Fatalf(message, kvErr.UnsafeError)
		} else {
			log.Fatalf(message, err)
		}
	}

	if filterNodeName != "" {
		realities, _, err = store.ListPods(kp.REALITY_TREE, filterNodeName)
	} else {
		realities, _, err = store.AllPods(kp.REALITY_TREE)
	}

	if err != nil {
		message := "Could not list reality kvpairs: %s"
		if kvErr, ok := err.(consulutil.KVError); ok {
			log.Fatalf(message, kvErr.UnsafeError)
		} else {
			log.Fatalf(message, err)
		}
	}

	statusMap := make(map[types.PodID]map[types.NodeName]inspect.NodePodStatus)

	for _, kvp := range intents {
		if inspect.AddKVPToMap(kvp, inspect.INTENT_SOURCE, filterNodeName, filterPodID, statusMap) != nil {
			log.Fatal(err)
		}
	}

	for _, kvp := range realities {
		if inspect.AddKVPToMap(kvp, inspect.REALITY_SOURCE, filterNodeName, filterPodID, statusMap) != nil {
			log.Fatal(err)
		}
	}

	hchecker := checker.NewConsulHealthChecker(client)
	for podID := range statusMap {
		resultMap, err := hchecker.Service(podID.String())
		if err != nil {
			log.Fatalf("Could not retrieve health checks for pod %s: %s", podID, err)
		}

		for node, result := range resultMap {
			if filterNodeName != "" && node != filterNodeName {
				continue
			}

			old := statusMap[podID][node]
			old.Health = result.Status
			statusMap[podID][node] = old
		}
	}

	// Keep this switch in sync with the enum options for the "format" flag. Rethink this
	// design once there are many different formats.
	switch *format {
	case "tree":
		// Native data format is already a "tree"
		enc := json.NewEncoder(os.Stdout)
		err = enc.Encode(statusMap)
	case "list":
		// "List" format is a flattened version of "tree"
		output := make([]inspect.NodePodStatus, 0)
		for podID, nodes := range statusMap {
			for node, status := range nodes {
				status.PodId = podID
				status.NodeName = node
				output = append(output, status)
			}
		}
		enc := json.NewEncoder(os.Stdout)
		err = enc.Encode(output)
	default:
		err = fmt.Errorf("unrecognized format: %s", *format)
	}
	if err != nil {
		log.Fatal(err)
	}
}
