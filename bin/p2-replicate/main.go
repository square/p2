package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"os/user"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/version"
)

var (
	manifestUri  = kingpin.Arg("manifest", "a path or url to a pod manifest that will be replicated.").Required().String()
	hosts        = kingpin.Arg("hosts", "Hosts to replicate to").Required().Strings()
	minNodes     = kingpin.Flag("min-nodes", "The minimum number of healthy nodes that must remain up while replicating.").Default("1").Short('m').Int()
	threshold    = kingpin.Flag("threshold", "The minimum health level to treat as healthy. One of (in order) passing, warning, unknown, critical.").String()
	overrideLock = kingpin.Flag("override-lock", "Override any lock holders").Bool()
)

func main() {
	kingpin.CommandLine.Name = "p2-replicate"
	kingpin.CommandLine.Help = `p2-replicate uses the replication package to schedule deployment of a pod across multiple nodes. See the replication package's README and godoc for more information.

	Example invocation: p2-replicate --min-nodes 2 helloworld.yaml aws{1,2,3}.example.com

	This will take the pod whose manifest is located at helloworld.yaml and
	deploy it to the three nodes aws1.example.com, aws2.example.com, and
	aws3.example.com

	Because of --min-nodes 2, the replicator will ensure that at least two healthy
	nodes remain up at all times, according to p2's health checks.
`

	kingpin.Version(version.VERSION)
	_, opts := flags.ParseWithConsulOptions()
	store := kp.NewConsulStore(opts)
	healthChecker := checker.NewConsulHealthChecker(opts)

	manifest, err := pods.ManifestFromURI(*manifestUri)
	if err != nil {
		log.Fatalf("%s", err)
	}

	logger := logging.NewLogger(logrus.Fields{
		"pod": manifest.ID(),
	})
	logger.Logger.Formatter = &logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
		TimestampFormat:  "15:04:05.000",
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
	lockMessage := fmt.Sprintf("%q from %q at %q", thisUser.Username, thisHost, time.Now())
	repl, err := replication.NewReplicator(
		manifest,
		logger,
		*hosts,
		len(*hosts)-*minNodes,
		store,
		healthChecker,
		health.HealthState(*threshold),
		lockMessage,
	)
	if err != nil {
		log.Fatalf("Could not initialize replicator: %s", err)
	}

	replication, errCh, err := repl.InitializeReplication(*overrideLock)
	if err != nil {
		log.Fatalf("Unable to initialize replication: %s", err)
	}

	// auto-drain this channel
	go func() {
		for range errCh {
		}
	}()

	go func() {
		// clear lock immediately on ctrl-C
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		<-signals
		replication.Cancel()
		os.Exit(1)
	}()

	replication.Enact()
}
