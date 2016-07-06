package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-cleanhttp"
	"gopkg.in/alecthomas/kingpin.v2"
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
	"github.com/square/p2/pkg/manifest"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll"
	roll_fields "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/version"
)

const (
	cmdCreateText   = "create"
	cmdDeleteText   = "delete"
	cmdReplicasText = "set-replicas"
	cmdListText     = "list"
	cmdGetText      = "get"
	cmdEnableText   = "enable"
	cmdDisableText  = "disable"
	cmdRollText     = "rolling-update"
	cmdSchedupText  = "schedule-update"
)

var (
	labelEndpoint = kingpin.Flag("labels", "An HTTP endpoint to use for labels, instead of using Consul.").String()
	logLevel      = kingpin.Flag("log", "Logging level to display.").String()
	logJSON       = kingpin.Flag("log-json", "Log messages will be JSON formatted").Bool()

	cmdCreate       = kingpin.Command(cmdCreateText, "Create a new replication controller")
	createManifest  = cmdCreate.Flag("manifest", "manifest file to use for this replication controller").Short('m').Required().String()
	createNodeSel   = cmdCreate.Flag("node-selector", "node selector that this replication controller should target").Short('n').Required().String()
	createPodLabels = cmdCreate.Flag("pod-label", "a pod label, in LABEL=VALUE form, to add to this replication controller. Can be specified multiple times.").Short('p').StringMap()
	createRCLabels  = cmdCreate.Flag("rc-label", "an RC label, in LABEL=VALUE form, to be applied to this replication controller. Can be specified multiple times.").Short('r').StringMap()

	cmdDelete   = kingpin.Command(cmdDeleteText, "Delete a replication controller")
	deleteID    = cmdDelete.Arg("id", "replication controller uuid to delete").Required().String()
	deleteForce = cmdDelete.Flag("force", "delete even if desired replicas > 0").Short('f').Bool()

	cmdReplicas = kingpin.Command(cmdReplicasText, "Set desired replica count of a replication controller")
	replicasID  = cmdReplicas.Arg("id", "replication controller uuid to modify").Required().String()
	replicasNum = cmdReplicas.Arg("replicas", "number of replicas desired").Required().Int()

	cmdList  = kingpin.Command(cmdListText, "List replication controllers")
	listJSON = cmdList.Flag("json", "output the entire JSON object of each replication controller").Short('j').Bool()

	cmdGet      = kingpin.Command(cmdGetText, "Get replication controller")
	getID       = cmdGet.Arg("id", "replication controller uuid to get").Required().String()
	getManifest = cmdGet.Flag("manifest", "print just the manifest of the replication controller").Short('m').Bool()

	cmdEnable = kingpin.Command(cmdEnableText, "Enable replication controller")
	enableID  = cmdEnable.Arg("id", "replication controller uuid to enable").Required().String()

	cmdDisable = kingpin.Command(cmdDisableText, "Disable replication controller")
	disableID  = cmdDisable.Arg("id", "replication controller uuid to disable").Required().String()

	cmdRoll                 = kingpin.Command(cmdRollText, "Rolling update from one replication controller to another")
	rollOldID               = cmdRoll.Flag("old", "old replication controller uuid").Required().Short('o').String()
	rollNewID               = cmdRoll.Flag("new", "new replication controller uuid").Required().Short('n').String()
	rollWant                = cmdRoll.Flag("desired", "number of replicas desired").Required().Short('d').Int()
	rollNeed                = cmdRoll.Flag("minimum", "minimum number of healthy replicas during update").Required().Short('m').Int()
	rollPagerdutyServiceKey = cmdRoll.Flag("pagerduty-service-key", "Pagerduty Service Key to use for alerting if provided").String()

	cmdSchedup   = kingpin.Command(cmdSchedupText, "Schedule new rolling update (will be run by farm)")
	schedupOldID = cmdSchedup.Flag("old", "old replication controller uuid").Required().Short('o').String()
	schedupNewID = cmdSchedup.Flag("new", "new replication controller uuid").Required().Short('n').String()
	schedupWant  = cmdSchedup.Flag("desired", "number of replicas desired").Required().Short('d').Int()
	schedupNeed  = cmdSchedup.Flag("minimum", "minimum number of healthy replicas during update").Required().Short('m').Int()
)

func main() {
	kingpin.Version(version.VERSION)
	cmd, opts := flags.ParseWithConsulOptions()

	logger := logging.NewLogger(logrus.Fields{})
	if *logJSON {
		logger.Logger.Formatter = &logrus.JSONFormatter{}
	} else {
		logger.Logger.Formatter = &logrus.TextFormatter{}
	}
	if *logLevel != "" {
		lv, err := logrus.ParseLevel(*logLevel)
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{"level": *logLevel}).Fatalln("Could not parse log level")
		}
		logger.Logger.Level = lv
	}

	httpClient := cleanhttp.DefaultClient()
	client := kp.NewConsulClient(opts)
	labeler := labels.NewConsulApplicator(client, 3)
	sched := scheduler.NewApplicatorScheduler(labeler)
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
		sched = scheduler.NewApplicatorScheduler(httpLabeler)
	}
	rctl := rctlParams{
		httpClient: httpClient,
		baseClient: client,
		rcs:        rcstore.NewConsul(client, 3),
		rls:        rollstore.NewConsul(client, nil),
		kps:        kp.NewConsulStore(client),
		labeler:    labeler,
		sched:      sched,
		hcheck:     checker.NewConsulHealthChecker(client),
		logger:     logger,
	}

	switch cmd {
	case cmdCreateText:
		rctl.Create(*createManifest, *createNodeSel, *createPodLabels, *createRCLabels)
	case cmdDeleteText:
		rctl.Delete(*deleteID, *deleteForce)
	case cmdReplicasText:
		rctl.SetReplicas(*replicasID, *replicasNum)
	case cmdListText:
		rctl.List(*listJSON)
	case cmdGetText:
		rctl.Get(*getID, *getManifest)
	case cmdEnableText:
		rctl.Enable(*enableID)
	case cmdDisableText:
		rctl.Disable(*disableID)
	case cmdRollText:
		rctl.RollingUpdate(*rollOldID, *rollNewID, *rollWant, *rollNeed, *rollPagerdutyServiceKey)
	case cmdSchedupText:
		rctl.ScheduleUpdate(*schedupOldID, *schedupNewID, *schedupWant, *schedupNeed)
	}
}

// SessionName returns a node identifier for use when creating Consul sessions.
func SessionName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown hostname"
	}
	// Current time of "Jan 2, 2006, 15:04:05" turns into "2006-01-02-15-04-05"
	timeStr := time.Now().Format("2006-01-02-15-04-05")
	return fmt.Sprintf("p2-rctl:%s:%s", hostname, timeStr)
}

// rctl is a struct for the data structures shared between commands
// each member function represents a single command that takes over from main
// and terminates the program on failure
type rctlParams struct {
	httpClient *http.Client
	baseClient *api.Client
	rcs        rcstore.Store
	rls        rollstore.Store
	sched      scheduler.Scheduler
	labeler    labels.Applicator
	kps        kp.Store
	hcheck     checker.ConsulHealthChecker
	logger     logging.Logger
}

func (r rctlParams) Create(manifestPath, nodeSelector string, podLabels map[string]string, rcLabels map[string]string) {
	manifest, err := manifest.ManifestFromPath(manifestPath)
	if err != nil {
		r.logger.WithErrorAndFields(err, logrus.Fields{
			"manifest": manifestPath,
		}).Fatalln("Could not read pod manifest")
	}

	nodeSel, err := klabels.Parse(nodeSelector)
	if err != nil {
		r.logger.WithErrorAndFields(err, logrus.Fields{
			"selector": nodeSelector,
		}).Fatalln("Could not parse node selector")
	}

	newRC, err := r.rcs.Create(manifest, nodeSel, klabels.Set(podLabels))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not create replication controller in Consul")
	}
	r.logger.WithField("id", newRC.ID).Infoln("Created new replication controller")

	err = r.labeler.SetLabels(labels.RC, newRC.ID.String(), rcLabels)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not label replication controller")
	}
}

func (r rctlParams) Delete(id string, force bool) {
	err := r.rcs.Delete(rc_fields.ID(id), force)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not delete replication controller in Consul")
	}
	r.logger.WithField("id", id).Infoln("Deleted replication controller")
}

func (r rctlParams) SetReplicas(id string, replicas int) {
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

func (r rctlParams) List(asJSON bool) {
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

func (r rctlParams) Get(id string, manifest bool) {
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

func (r rctlParams) Enable(id string) {
	err := r.rcs.Enable(rc_fields.ID(id))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not enable replication controller in Consul")
	}
	r.logger.WithField("id", id).Infoln("Enabled replication controller")
}

func (r rctlParams) Disable(id string) {
	err := r.rcs.Disable(rc_fields.ID(id))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not disable replication controller in Consul")
	}
	r.logger.WithField("id", id).Infoln("Disabled replication controller")
}

func (r rctlParams) RollingUpdate(oldID, newID string, want, need int, pagerdutyServiceKey string) {
	if want < need {
		r.logger.WithFields(logrus.Fields{
			"want": want,
			"need": need,
		}).Fatalln("Cannot run update with desired replicas less than minimum replicas")
	}
	sessions := make(chan string)
	quit := make(chan struct{})

	go consulutil.SessionManager(api.SessionEntry{
		Name:      SessionName(),
		LockDelay: 5 * time.Second,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "15s",
	}, r.baseClient, sessions, quit, r.logger)

	sessionID := <-sessions
	if sessionID == "" {
		r.logger.NoFields().Fatalln("Could not acquire session")
	}
	session := r.kps.NewUnmanagedSession(sessionID, "")

	alerter := alerting.NewNop()
	if pagerdutyServiceKey != "" {
		var err error
		alerter, err = alerting.NewPagerduty(pagerdutyServiceKey, r.httpClient)
		if err != nil {
			r.logger.WithError(err).Fatalln("Could not initialize pagerduty alerter")
		}
	}

	result := make(chan bool, 1)
	go func() {
		result <- roll.NewUpdate(roll_fields.Update{
			OldRC:           rc_fields.ID(oldID),
			NewRC:           rc_fields.ID(newID),
			DesiredReplicas: want,
			MinimumReplicas: need,
		}, r.kps, r.rcs, r.hcheck, r.labeler, r.sched, r.logger, session, alerter).Run(quit)
		close(result)
	}()

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)

LOOP:
	for {
		select {
		case <-signals:
			// try to clean up locks on ^C
			close(quit)
			// do not exit right away - the session and result channels will be
			// closed after the quit is requested, ensuring that the locks held
			// by the farm were released.
			r.logger.NoFields().Errorln("Got signal, exiting")
		case <-sessions:
			r.logger.NoFields().Fatalln("Lost session")
		case res := <-result:
			// done, either due to ^C (already printed message above) or
			// clean finish
			if res {
				r.logger.NoFields().Infoln("Done")
			}
			break LOOP
		}
	}
}

func (r rctlParams) ScheduleUpdate(oldID, newID string, want, need int) {
	_, err := r.rls.CreateRollingUpdateFromExistingRCs(roll_fields.Update{
		OldRC:           rc_fields.ID(oldID),
		NewRC:           rc_fields.ID(newID),
		DesiredReplicas: want,
		MinimumReplicas: need,
	}, nil)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not create rolling update")
	} else {
		r.logger.WithField("id", newID).Infoln("Created new rolling update")
	}
}
