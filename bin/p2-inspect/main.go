package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/inspect"
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
	https          = kingpin.Flag("https", "Use HTTPS").Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	opts := kp.Options{
		Address: *consulUrl,
		Token:   *consulToken,
		Client:  net.NewHeaderClient(*headers, http.DefaultTransport),
		HTTPS:   *https,
	}
	store := kp.NewConsulStore(opts)

	intents, _, err := store.ListPods(kp.INTENT_TREE)
	if err != nil {
		message := "Could not list intent kvpairs: %s"
		if kvErr, ok := err.(kp.KVError); ok {
			log.Fatalf(message, kvErr.UnsafeError)
		} else {
			log.Fatalf(message, err)
		}
	}
	realities, _, err := store.ListPods(kp.REALITY_TREE)
	if err != nil {
		message := "Could not list reality kvpairs: %s"
		if kvErr, ok := err.(kp.KVError); ok {
			log.Fatalf(message, kvErr.UnsafeError)
		} else {
			log.Fatalf(message, err)
		}
	}

	statusMap := make(map[string]map[string]inspect.NodePodStatus)

	for _, kvp := range intents {
		if inspect.AddKVPToMap(kvp, inspect.INTENT_SOURCE, *filterNodeName, *filterPodId, statusMap) != nil {
			log.Fatal(err)
		}
	}

	for _, kvp := range realities {
		if inspect.AddKVPToMap(kvp, inspect.REALITY_SOURCE, *filterNodeName, *filterPodId, statusMap) != nil {
			log.Fatal(err)
		}
	}

	hchecker := health.NewConsulHealthChecker(opts)
	for podId := range statusMap {
		resultMap, err := hchecker.Service(podId)
		if err != nil {
			log.Fatalf("Could not retrieve health checks for pod %s: %s", podId, err)
		}

		for node, results := range resultMap {
			if *filterNodeName != "" && node != *filterNodeName {
				continue
			}

			old := statusMap[podId][node]
			_, old.Health = health.FindWorst(results)
			statusMap[podId][node] = old
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.Encode(statusMap)
}
