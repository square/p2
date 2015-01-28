package main

import (
	"log"

	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/allocation"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	manifestUri = kingpin.Arg("manifest", "a path or url to a pod manifest that will be replicated.").String()
	hosts       = kingpin.Arg("hosts", "Hosts to replicate to").Strings()
	consulUrl   = kingpin.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	store := kp.NewStore(kp.Options{Address: *consulUrl})

	conf := consulapi.DefaultConfig()
	conf.Address = *consulUrl

	// the error is always nil
	client, _ := consulapi.NewClient(conf)

	healthChecker := health.NewConsulHealthChecker(*store, client.Health())

	manifest, err := pods.PodManifestFromPath(*manifestUri)
	if err != nil {
		log.Fatalf("Invalid manifest: %s", err)
	}

	allocated := allocation.NewAllocation(*hosts...)

	replicator := replication.NewReplicator(*manifest, allocated)

	stopChan := make(chan struct{})
	replicator.Enact(store, healthChecker, stopChan)
}
