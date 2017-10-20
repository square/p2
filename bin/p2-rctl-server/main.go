// p2-rctl-server contains the server code for running Farms for resource controllers and
// rolling updates.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/hashicorp/go-cleanhttp"
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/roll"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/flags"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/rollstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/util/stream"
	"github.com/square/p2/pkg/version"
)

// Command arguments
var (
	logLevel            = kingpin.Flag("log", "Logging level to display").String()
	pagerdutyServiceKey = kingpin.Flag("pagerduty-service-key", "Pagerduty Service Key to use for alerting if provided").String()
)

// RetryCount defines the number of retries to attempt when accessing some storage
// components.
const RetryCount = 3

// SessionName returns a node identifier for use when creating Consul sessions.
func SessionName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown hostname"
	}
	// Current time of "Jan 2, 2006, 15:04:05" turns into "2006-01-02-15-04-05"
	timeStr := time.Now().Format("2006-01-02-15-04-05")
	return fmt.Sprintf("p2-rctl-server:%s:%s", hostname, timeStr)
}

func main() {
	// Parse custom flags + standard Consul routing options
	kingpin.Version(version.VERSION)
	_, opts, labeler := flags.ParseWithConsulOptions()

	// Set up the logger
	logger := logging.NewLogger(logrus.Fields{})
	logger.Logger.Formatter = new(logrus.TextFormatter)
	if *logLevel != "" {
		lv, err := logrus.ParseLevel(*logLevel)
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{"level": *logLevel}).
				Fatalln("Could not parse log level")
		}
		logger.Logger.Level = lv
	}

	// Initialize the myriad of different storage components
	httpClient := cleanhttp.DefaultClient()
	client := consul.NewConsulClient(opts)
	statusStoreClient := statusstore.NewConsul(client)
	consulStore := consul.NewConsulStore(client)
	rcStore := rcstore.NewConsul(client, labeler, RetryCount)
	rcStatusStore := rcstatus.NewConsul(statusStoreClient, consul.RCStatusNamespace)

	rollStore := rollstore.NewConsul(client, labeler, nil)
	healthChecker := checker.NewConsulHealthChecker(client)
	sched := scheduler.NewApplicatorScheduler(labeler)

	// Start acquiring sessions
	sessions := make(chan string)
	go consulutil.SessionManager(api.SessionEntry{
		Name:      SessionName(),
		LockDelay: 5 * time.Second,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "15s",
	}, client, sessions, nil, logger)
	pub := stream.NewStringValuePublisher(sessions, "")

	alerter := alerting.NewNop()
	if *pagerdutyServiceKey != "" {
		var err error
		// just use the same key for high and low urgency
		// TODO: support both high and low urgency keys
		alerter, err = alerting.NewPagerduty(*pagerdutyServiceKey, *pagerdutyServiceKey, httpClient)
		if err != nil {
			logger.WithError(err).Fatalln(
				"Unable to initialize pagerduty alerter",
			)
		}
	}

	auditLogStore := auditlogstore.NewConsulStore(client.KV())

	// Run the farms!
	go rc.NewFarm(
		consulStore,
		client,
		rcStatusStore,
		auditLogStore,
		rcStore,
		rcStore,
		rcStore,
		client.KV(),
		healthChecker,
		sched,
		labeler,
		pub.Subscribe().Chan(),
		logger,
		klabels.Everything(),
		alerter,
		1*time.Second,
	).Start(nil)
	roll.NewFarm(
		roll.UpdateFactory{
			Store:         consulStore,
			RCStore:       rcStore,
			HealthChecker: healthChecker,
			Labeler:       labeler,
		},
		consulStore,
		rollStore,
		rcStore,
		pub.Subscribe().Chan(),
		logger,
		labeler,
		klabels.Everything(),
		client.KV(),
		roll.FarmConfig{},
		alerter,
	).Start(nil)
}
