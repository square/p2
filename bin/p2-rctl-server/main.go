// p2-rctl-server contains the server code for running Farms for resource controllers and
// rolling updates.
package main

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/hashicorp/go-cleanhttp"
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/alerting"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/kp/rollstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/roll"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/util/stream"
	"github.com/square/p2/pkg/version"
)

// Command arguments
var (
	labelEndpoint       = kingpin.Flag("labels", "An HTTP endpoint to use for labels, instead of using Consul").String()
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
	_, opts := flags.ParseWithConsulOptions()

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
	client := kp.NewConsulClient(opts)
	kpStore := kp.NewConsulStore(client)
	rcStore := rcstore.NewConsul(client, RetryCount)
	rollStore := rollstore.NewConsul(client, nil)
	healthChecker := checker.NewConsulHealthChecker(client)
	labeler := labels.NewConsulApplicator(client, RetryCount)
	var sched scheduler.Scheduler
	if *labelEndpoint != "" {
		endpoint, err := url.Parse(*labelEndpoint)
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{
				"url": *labelEndpoint,
			}).Fatalln("Could not parse URL from label endpoint")
		}
		httpLabeler, err := labels.NewHttpApplicator(opts.Client, endpoint)
		if err != nil {
			logger.WithError(err).Fatalln("Could not create label applicator from endpoint")
		}
		sched = scheduler.NewApplicatorScheduler(httpLabeler)
	} else {
		sched = scheduler.NewApplicatorScheduler(labeler)
	}

	// Start acquiring sessions
	sessions := make(chan string)
	go consulutil.SessionManager(api.SessionEntry{
		Name:      SessionName(),
		LockDelay: 5 * time.Second,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "15s",
	}, client, sessions, nil, logger)
	pub := stream.NewStringValuePublisher(sessions, "")
	rcSub := pub.Subscribe(nil)
	rlSub := pub.Subscribe(nil)

	alerter := alerting.NewNop()
	if *pagerdutyServiceKey != "" {
		var err error
		alerter, err = alerting.NewPagerduty(*pagerdutyServiceKey, httpClient)
		if err != nil {
			logger.WithError(err).Fatalln(
				"Unable to initialize pagerduty alerter",
			)
		}
	}

	// Run the farms!
	go rc.NewFarm(kpStore, rcStore, sched, labeler, rcSub.Chan(), logger, klabels.Everything(), alerter, nil).Start(nil)
	roll.NewFarm(roll.UpdateFactory{
		KPStore:       kpStore,
		RCStore:       rcStore,
		HealthChecker: healthChecker,
		Labeler:       labeler,
		Scheduler:     sched,
	}, kpStore, rollStore, rcStore, rlSub.Chan(), logger, labeler, klabels.Everything(), alerter).Start(nil)
}
