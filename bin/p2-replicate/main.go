package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"sort"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
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
	manifestUri  = replicate.Arg("manifest", "a path or url to a pod manifest that will be replicated.").Required().String()
	hosts        = replicate.Arg("hosts", "Hosts to replicate to").Required().Strings()
	minNodes     = replicate.Flag("min-nodes", "The minimum number of healthy nodes that must remain up while replicating.").Default("1").Short('m').Int()
	consulUrl    = replicate.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	consulToken  = replicate.Flag("token", "The ACL token to use for consul").String()
	threshold    = replicate.Flag("threshold", "The minimum health level to treat as healthy. One of (in order) passing, warning, unknown, critical.").String()
	headers      = replicate.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	https        = replicate.Flag("https", "Use HTTPS").Bool()
	overrideLock = replicate.Flag("override-lock", "Override any lock holders").Bool()
)

func main() {
	replicate.Version(version.VERSION)
	replicate.Parse(os.Args[1:])

	opts := kp.Options{
		Address: *consulUrl,
		Token:   *consulToken,
		Client:  net.NewHeaderClient(*headers, http.DefaultTransport),
		HTTPS:   *https,
	}
	store := kp.NewStore(opts)
	healthChecker := health.NewConsulHealthChecker(opts)

	// Fetch manifest (could be URI) into temp file
	localMan, err := ioutil.TempFile("", "tempmanifest")
	defer os.Remove(localMan.Name())
	if err != nil {
		log.Fatalln("Couldn't create tempfile")
	}
	if err := uri.URICopy(*manifestUri, localMan.Name()); err != nil {
		log.Fatalf("Could not fetch manifest: %s", err)
	}

	manifest, err := pods.ManifestFromPath(localMan.Name())
	if err != nil {
		log.Fatalf("Invalid manifest: %s", err)
	}

	healthResults, err := healthChecker.Service(manifest.ID())
	if err != nil {
		log.Fatalf("Could not get initial health results: %s", err)
	}
	order := health.SortOrder{
		Nodes:  *hosts,
		Health: healthResults,
	}
	sort.Sort(order)

	repl := replication.Replicator{
		Manifest: *manifest,
		Store:    store,
		Health:   healthChecker,
		Nodes:    *hosts, // sorted by the health.SortOrder
		Active:   len(*hosts) - *minNodes,
		Logger: logging.NewLogger(logrus.Fields{
			"pod": manifest.ID(),
		}),
		Threshold: health.HealthState(*threshold),
	}
	repl.Logger.Logger.Formatter = &logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
		TimestampFormat:  "15:04:05.000",
	}

	if err := repl.CheckPreparers(); err != nil {
		log.Fatalf("Preparer check failed: %s", err)
	}

	// create a lock with a meaningful name and set up a renewal loop for it
	thisHost, err := os.Hostname()
	if err != nil {
		log.Fatalf("Could not retrieve hostname: %s", err)
	}
	thisUser, err := user.Current()
	if err != nil {
		log.Fatalf("Could not retrieve user: %s", err)
	}
	lock, err := store.NewLock(fmt.Sprintf("%q from %q at %q", thisUser.Username, thisHost, time.Now()))
	if err != nil {
		log.Fatalf("Could not generate lock: %s", err)
	}
	// deferring on main is not particularly useful, since os.Exit will skip
	// the defer, so we have to manually destroy the lock at the right exit
	// paths
	go func() {
		for range time.Tick(10 * time.Second) {
			if err := lock.Renew(); err != nil {
				// if the renewal failed, then either the lock is already dead
				// or the consul agent cannot be reached
				log.Fatalf("Lock could not be renewed: %s", err)
			}
		}
	}()
	if err := repl.LockHosts(lock, *overrideLock); err != nil {
		lock.Destroy()
		log.Fatalf("Could not lock all hosts: %s", err)
	}

	// auto-drain this channel
	errs := make(chan error)
	go func() {
		for range errs {
		}
	}()

	quitch := make(chan struct{})
	go func() {
		// clear lock immediately on ctrl-C
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		<-signals
		close(quitch)
		lock.Destroy()
		os.Exit(1)
	}()

	repl.Enact(errs, quitch)
	lock.Destroy()
}
