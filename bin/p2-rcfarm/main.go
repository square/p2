package main

import (
	"net/http"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/util/net"
	"github.com/square/p2/pkg/version"
)

var (
	consulUrl   = kingpin.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	consulToken = kingpin.Flag("token", "The consul ACL token to use. Empty by default.").String()
	headers     = kingpin.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	https       = kingpin.Flag("https", "Use HTTPS").Bool()
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
	kpStore := kp.NewConsulStore(opts)
	client := kp.NewConsulClient(opts)
	rcStore := rcstore.NewConsul(client, 3, logging.DefaultLogger)
	labeler := labels.NewConsulApplicator(client, 3)
	sched := rc.NewApplicatorScheduler(labeler)

	sessionCh := make(chan string)
	quitCh := make(chan struct{})
	logging.DefaultLogger.Logger.Level = logrus.DebugLevel

	go kp.ConsulSessionManager(api.SessionEntry{
		LockDelay: 1 * time.Nanosecond,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "15s",
	}, client, sessionCh, quitCh, logging.DefaultLogger)

	rcm := rc.NewFarm(kpStore, rcStore, sched, labeler, sessionCh, logging.DefaultLogger)
	rcm.Start(quitCh)
}
