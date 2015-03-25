package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/allocation"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util/net"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	replicate = kingpin.New("p2-replicate", `p2-replicate uses the replication package to schedule deployment of a pod across multiple nodes. See the replication package's README and godoc for more information.

	Example invocation: p2-replicate --min-nodes 2 helloworld.yaml aws{1,2,3}.example.com

	This will take the pod whose manifest is located at helloworld.yaml and
	deploy it to the three nodes aws1.example.com, aws2.example.com, and
	aws3.example.com

	Because of --min-nodes 2, the replicator will ensure that at least two healthy
	nodes remain up at all times, according to p2's health checks.

	`)
	manifestUri = replicate.Arg("manifest", "a path or url to a pod manifest that will be replicated.").Required().String()
	hosts       = replicate.Arg("hosts", "Hosts to replicate to").Required().Strings()
	minNodes    = replicate.Flag("min-nodes", "The minimum number of healthy nodes that must remain up while replicating.").Default("1").Short('m').Int()
	consulUrl   = replicate.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	consulToken = replicate.Flag("token", "The ACL token to use for consul").String()
	threshold   = replicate.Flag("threshold", "The minimum health level to treat as healthy. One of (in order) passing, warning, unknown, critical.").String()
	headers     = replicate.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	https       = replicate.Flag("https", "Use HTTPS").Bool()
)

func main() {
	replicate.Version(version.VERSION)
	replicate.Parse(os.Args[1:])

	opts := kp.Options{
		Address: *consulUrl,
		Token:   *consulToken,
		Client:  net.NewHeaderClient(*headers),
		HTTPS:   *https,
	}
	store := kp.NewStore(opts)

	inner := health.NewConsulHealthChecker(opts)
	var healthChecker replication.ServiceChecker = inner
	if *threshold != "" {
		healthChecker = replication.ServiceUpgrader{
			Inner:     inner,
			Threshold: health.ToHealthState(*threshold),
		}
	}

	// Fetch manifest (could be URI) into temp file
	localMan, err := ioutil.TempFile("", "tempmanifest")
	defer os.Remove(localMan.Name())
	if err != nil {
		log.Fatalln("Couldn't create tempfile")
	}
	err = uri.URICopy(*manifestUri, localMan.Name())
	if err != nil {
		log.Fatalf("Could not fetch manifest: %s", err)
	}

	manifest, err := pods.ManifestFromPath(localMan.Name())
	if err != nil {
		log.Fatalf("Invalid manifest: %s", err)
	}

	for _, host := range *hosts {
		_, _, err := store.Pod(kp.RealityPath(host, "p2-preparer"))
		if err != nil {
			log.Fatalf("p2 is not running on host %s: %s", host, err)
		}
	}

	allocated := allocation.NewAllocation(*hosts...)

	replicator := replication.NewReplicator(*manifest, allocated)
	replicator.Logger.Logger.Formatter = new(logrus.TextFormatter)
	replicator.MinimumNodes = *minNodes

	stopChan := make(chan struct{})
	replicator.Enact(store, healthChecker, stopChan)
}
