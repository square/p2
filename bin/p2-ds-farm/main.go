package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"

	ds_farm "github.com/square/p2/pkg/ds"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	useCachePodMatches = kingpin.Flag("use-cached-pod-matches", "If enabled, create a local cache of the pod label tree and match against that instead of querying on all pod selector queries").Bool()
)

// SessionName returns a node identifier for use when creating Consul sessions.
func SessionName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown hostname"
	}
	// Current time of "Jan 2, 2006, 15:04:05" turns into "2006-01-02-15-04-05"
	timeStr := time.Now().Format("2006-01-02-15-04-05")
	return fmt.Sprintf("p2-ds-farm:%s:%s", hostname, timeStr)
}

func main() {
	quitCh := make(chan struct{})

	_, consulOpts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(consulOpts)
	logger := logging.NewLogger(logrus.Fields{})
	dsStore := dsstore.NewConsul(client, 3, &logger)
	kpStore := kp.NewConsulStore(client)
	applicator := labels.NewConsulApplicator(client, 3)
	healthChecker := checker.NewConsulHealthChecker(client)

	sessions := make(chan string)
	go consulutil.SessionManager(api.SessionEntry{
		Name:      SessionName(),
		LockDelay: 5 * time.Second,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "15s",
	}, client, sessions, quitCh, logger)

	dsf := ds_farm.NewFarm(kpStore, dsStore, applicator, sessions, logger, nil, &healthChecker, 1*time.Second, *useCachePodMatches)

	go func() {
		// clear lock immediately on ctrl-C
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		<-signals
		close(quitCh)
	}()

	dsf.Start(quitCh)
}
