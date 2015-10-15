package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"

	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/kp/rollstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll"
	roll_fields "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/util/net"
	"github.com/square/p2/pkg/util/stream"
	"github.com/square/p2/pkg/version"
)

const (
	CMD_CREATE   = "create"
	CMD_DELETE   = "delete"
	CMD_REPLICAS = "set-replicas"
	CMD_LIST     = "list"
	CMD_GET      = "get"
	CMD_ENABLE   = "enable"
	CMD_DISABLE  = "disable"
	CMD_ROLL     = "rolling-update"
	CMD_FARM     = "farm"
	CMD_SCHEDUP  = "schedule-update"
)

var (
	consulUrl     = kingpin.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	consulToken   = kingpin.Flag("token", "The consul ACL token to use. Empty by default.").String()
	headers       = kingpin.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	https         = kingpin.Flag("https", "Use HTTPS").Bool()
	labelEndpoint = kingpin.Flag("labels", "An HTTP endpoint to use for labels, instead of using Consul.").String()
	logLevel      = kingpin.Flag("log", "Logging level to display.").String()
	waitTime      = kingpin.Flag("wait", "Maximum duration for Consul watches, before resetting and starting again.").Default("30s").Duration()

	cmdCreate       = kingpin.Command(CMD_CREATE, "Create a new replication controller")
	createManifest  = cmdCreate.Flag("manifest", "manifest file to use for this replication controller").Short('m').Required().String()
	createNodeSel   = cmdCreate.Flag("node-selector", "node selector that this replication controller should target").Short('n').Required().String()
	createPodLabels = cmdCreate.Flag("pod-label", "a pod label, in LABEL=VALUE form, to add to this replication controller. Can be specified multiple times.").Short('p').StringMap()

	cmdDelete   = kingpin.Command(CMD_DELETE, "Delete a replication controller")
	deleteID    = cmdDelete.Arg("id", "replication controller uuid to delete").Required().String()
	deleteForce = cmdDelete.Flag("force", "delete even if desired replicas > 0").Short('f').Bool()

	cmdReplicas = kingpin.Command(CMD_REPLICAS, "Set desired replica count of a replication controller")
	replicasID  = cmdReplicas.Arg("id", "replication controller uuid to modify").Required().String()
	replicasNum = cmdReplicas.Arg("replicas", "number of replicas desired").Required().Int()

	cmdList  = kingpin.Command(CMD_LIST, "List replication controllers")
	listJSON = cmdList.Flag("json", "output the entire JSON object of each replication controller").Short('j').Bool()

	cmdGet      = kingpin.Command(CMD_GET, "Get replication controller")
	getID       = cmdGet.Arg("id", "replication controller uuid to get").Required().String()
	getManifest = cmdGet.Flag("manifest", "print just the manifest of the replication controller").Short('m').Bool()

	cmdEnable = kingpin.Command(CMD_ENABLE, "Enable replication controller")
	enableID  = cmdEnable.Arg("id", "replication controller uuid to enable").Required().String()

	cmdDisable = kingpin.Command(CMD_DISABLE, "Disable replication controller")
	disableID  = cmdDisable.Arg("id", "replication controller uuid to disable").Required().String()

	cmdRoll    = kingpin.Command(CMD_ROLL, "Rolling update from one replication controller to another")
	rollOldID  = cmdRoll.Flag("old", "old replication controller uuid").Required().Short('o').String()
	rollNewID  = cmdRoll.Flag("new", "new replication controller uuid").Required().Short('n').String()
	rollWant   = cmdRoll.Flag("desired", "number of replicas desired").Required().Short('d').Int()
	rollNeed   = cmdRoll.Flag("minimum", "minimum number of healthy replicas during update").Required().Short('m').Int()
	rollDelete = cmdRoll.Flag("delete", "delete pods during update").Bool()

	cmdFarm = kingpin.Command(CMD_FARM, "Start farms for replication controllers and rolling updates")

	cmdSchedup    = kingpin.Command(CMD_SCHEDUP, "Schedule new rolling update (will be run by farm)")
	schedupOldID  = cmdSchedup.Flag("old", "old replication controller uuid").Required().Short('o').String()
	schedupNewID  = cmdSchedup.Flag("new", "new replication controller uuid").Required().Short('n').String()
	schedupWant   = cmdSchedup.Flag("desired", "number of replicas desired").Required().Short('d').Int()
	schedupNeed   = cmdSchedup.Flag("minimum", "minimum number of healthy replicas during update").Required().Short('m').Int()
	schedupDelete = cmdSchedup.Flag("delete", "delete pods during update").Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	cmd := kingpin.Parse()

	logger := logging.NewLogger(logrus.Fields{})
	logger.Logger.Formatter = &logrus.TextFormatter{}
	if *logLevel != "" {
		lv, err := logrus.ParseLevel(*logLevel)
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{"level": *logLevel}).Fatalln("Could not parse log level")
		}
		logger.Logger.Level = lv
	}

	opts := kp.Options{
		Address:  *consulUrl,
		Token:    *consulToken,
		Client:   net.NewHeaderClient(*headers, http.DefaultTransport),
		HTTPS:    *https,
		WaitTime: *waitTime,
	}
	client := kp.NewConsulClient(opts)
	labeler := labels.NewConsulApplicator(client, 3)
	sched := rc.NewApplicatorScheduler(labeler)
	if *labelEndpoint != "" {
		endpoint, err := url.Parse(*labelEndpoint)
		if err != nil {
			logging.DefaultLogger.WithErrorAndFields(err, logrus.Fields{
				"url": *labelEndpoint,
			}).Fatalln("Could not parse URL from label endpoint")
		}
		httpLabeler, err := labels.NewHttpApplicator(opts.Client, endpoint)
		if err != nil {
			logging.DefaultLogger.WithError(err).Fatalln("Could not create label applicator from endpoint")
		}
		sched = rc.NewApplicatorScheduler(httpLabeler)
	}
	rctl := RCtl{
		baseClient: client,
		rcs:        rcstore.NewConsul(client, 3),
		rls:        rollstore.NewConsul(client),
		kps:        kp.NewConsulStore(opts),
		labeler:    labeler,
		sched:      sched,
		hcheck:     checker.NewConsulHealthChecker(opts),
		logger:     logger,
	}

	switch cmd {
	case CMD_CREATE:
		rctl.Create(*createManifest, *createNodeSel, *createPodLabels)
	case CMD_DELETE:
		rctl.Delete(*deleteID, *deleteForce)
	case CMD_REPLICAS:
		rctl.SetReplicas(*replicasID, *replicasNum)
	case CMD_LIST:
		rctl.List(*listJSON)
	case CMD_GET:
		rctl.Get(*getID, *getManifest)
	case CMD_ENABLE:
		rctl.Enable(*enableID)
	case CMD_DISABLE:
		rctl.Disable(*disableID)
	case CMD_ROLL:
		rctl.RollingUpdate(*rollOldID, *rollNewID, *rollWant, *rollNeed, *rollDelete)
	case CMD_FARM:
		rctl.Farm()
	case CMD_SCHEDUP:
		rctl.ScheduleUpdate(*schedupOldID, *schedupNewID, *schedupWant, *schedupNeed, *schedupDelete)
	}
}

// rctl is a struct for the data structures shared between commands
// each member function represents a single command that takes over from main
// and terminates the program on failure
type RCtl struct {
	baseClient *api.Client
	rcs        rcstore.Store
	rls        rollstore.Store
	sched      rc.Scheduler
	labeler    labels.Applicator
	kps        kp.Store
	hcheck     checker.ConsulHealthChecker
	logger     logging.Logger
}

func (r RCtl) Create(manifestPath, nodeSelector string, podLabels map[string]string) {
	manifest, err := pods.ManifestFromPath(manifestPath)
	if err != nil {
		r.logger.WithErrorAndFields(err, logrus.Fields{
			"manifest": manifestPath,
		}).Fatalln("Could not read pod manifest")
	}

	nodeSel, err := labels.Parse(nodeSelector)
	if err != nil {
		r.logger.WithErrorAndFields(err, logrus.Fields{
			"selector": nodeSelector,
		}).Fatalln("Could not parse node selector")
	}

	newRC, err := r.rcs.Create(manifest, nodeSel, labels.Set(podLabels))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not create replication controller in Consul")
	}
	r.logger.WithField("id", newRC.ID).Infoln("Created new replication controller")
}

func (r RCtl) Delete(id string, force bool) {
	err := r.rcs.Delete(rc_fields.ID(id), force)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not delete replication controller in Consul")
	}
	r.logger.WithField("id", id).Infoln("Deleted replication controller")
}

func (r RCtl) SetReplicas(id string, replicas int) {
	if replicas < 0 {
		r.logger.NoFields().Fatalln("Cannot set negative replica count")
	}

	err := r.rcs.SetDesiredReplicas(rc_fields.ID(id), replicas)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not set desired replica count in Consul")
	}
	r.logger.WithFields(logrus.Fields{
		"id":       id,
		"replicas": replicas,
	}).Infoln("Set desired replica count of replication controller")
}

func (r RCtl) List(asJSON bool) {
	list, err := r.rcs.List()
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not list replication controllers in Consul")
	}

	for _, listRC := range list {
		if asJSON {
			out, err := json.MarshalIndent(listRC, "", "    ")
			if err != nil {
				r.logger.WithError(err).Fatalln("Could not marshal replication controller to JSON")
			}
			fmt.Printf("%s\n", out)
		} else {
			fmt.Println(listRC.ID)
		}
	}
}

func (r RCtl) Get(id string, manifest bool) {
	getRC, err := r.rcs.Get(rc_fields.ID(id))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not get replication controller in Consul")
	}

	if manifest {
		out, err := getRC.Manifest.Marshal()
		if err != nil {
			r.logger.WithError(err).Fatalln("Could not marshal replication controller manifest")
		}
		fmt.Printf("%s", out)
	} else {
		out, err := json.MarshalIndent(getRC, "", "    ")
		if err != nil {
			r.logger.WithError(err).Fatalln("Could not marshal replication controller to JSON")
		}
		fmt.Printf("%s\n", out)
	}
}

func (r RCtl) Enable(id string) {
	err := r.rcs.Enable(rc_fields.ID(id))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not enable replication controller in Consul")
	}
	r.logger.WithField("id", id).Infoln("Enabled replication controller")
}

func (r RCtl) Disable(id string) {
	err := r.rcs.Disable(rc_fields.ID(id))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not disable replication controller in Consul")
	}
	r.logger.WithField("id", id).Infoln("Disabled replication controller")
}

func (r RCtl) RollingUpdate(oldID, newID string, want, need int, deletes bool) {
	sessions := make(chan string)
	go kp.ConsulSessionManager(api.SessionEntry{
		LockDelay: 1 * time.Nanosecond,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "15s",
	}, r.baseClient, sessions, nil, r.logger)

	session := <-sessions
	if session == "" {
		r.logger.NoFields().Fatalln("Could not acquire session")
	}
	lock := r.kps.NewUnmanagedLock(session, "")

	u := roll.NewUpdate(roll_fields.Update{
		OldRC:           rc_fields.ID(oldID),
		NewRC:           rc_fields.ID(newID),
		DesiredReplicas: want,
		MinimumReplicas: need,
		DeletePods:      deletes,
	}, r.kps, r.rcs, r.hcheck, r.labeler, r.sched, r.logger, lock)

	if err := u.Prepare(); err != nil {
		r.logger.WithError(err).Fatalln("Could not prepare update")
	}

	doneRun := make(chan struct{})
	go func() {
		err := u.Run(nil)
		if err != nil {
			r.logger.WithError(err).Errorln("Could not run update")
		}
		close(doneRun)
	}()

	select {
	case <-sessions:
		r.logger.NoFields().Fatalln("Lost session")
	case <-doneRun:
		r.logger.NoFields().Infoln("Done")
	}
}

func (r RCtl) Farm() {
	sessions := make(chan string)
	go kp.ConsulSessionManager(api.SessionEntry{
		LockDelay: 1 * time.Nanosecond,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "15s",
	}, r.baseClient, sessions, nil, r.logger)
	firstSession := <-sessions

	pub := stream.NewStringValuePublisher(sessions, firstSession)
	rcSub := pub.Subscribe(nil)
	rlSub := pub.Subscribe(nil)

	go rc.NewFarm(r.kps, r.rcs, r.sched, r.labeler, rcSub.Chan(), r.logger).Start(nil)
	roll.NewFarm(roll.UpdateFactory{
		KPStore:       r.kps,
		RCStore:       r.rcs,
		HealthChecker: r.hcheck,
		Labeler:       r.labeler,
		Scheduler:     r.sched,
	}, r.kps, r.rls, rlSub.Chan(), r.logger).Start(nil)
}

func (r RCtl) ScheduleUpdate(oldID, newID string, want, need int, deletes bool) {
	err := r.rls.Put(roll_fields.Update{
		OldRC:           rc_fields.ID(oldID),
		NewRC:           rc_fields.ID(newID),
		DesiredReplicas: want,
		MinimumReplicas: need,
		DeletePods:      deletes,
	})
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not create rolling update")
	} else {
		r.logger.WithField("id", newID).Infoln("Created new rolling update")
	}
}
