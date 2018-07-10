package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	"github.com/square/p2/pkg/cli"
	"github.com/square/p2/pkg/health/checker"
	hclient "github.com/square/p2/pkg/health/client"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/roll"
	roll_fields "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/flags"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/rollstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/rcstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/version"
)

const (
	cmdCreateText         = "create"
	cmdDeleteText         = "delete"
	cmdReplicasText       = "set-replicas"
	cmdListText           = "list"
	cmdGetText            = "get"
	cmdGetStatusText      = "get-status"
	cmdEnableText         = "enable"
	cmdDisableText        = "disable"
	cmdRollText           = "rolling-update"
	cmdDeleteRollText     = "delete-rolling-update"
	cmdSchedupText        = "schedule-update"
	cmdUpdateManifestText = "update-manifest"
	cmdUpdateStrategyText = "update-strategy"
)

var (
	logLevel = kingpin.Flag("log", "Logging level to display.").String()
	logJSON  = kingpin.Flag("log-json", "Log messages will be JSON formatted").Bool()

	cmdCreate                = kingpin.Command(cmdCreateText, "Create a new replication controller")
	createManifest           = cmdCreate.Flag("manifest", "manifest file to use for this replication controller").Short('m').Required().String()
	createNodeSel            = cmdCreate.Flag("node-selector", "node selector that this replication controller should target").Short('n').Required().String()
	createPodLabels          = cmdCreate.Flag("pod-label", "a pod label, in LABEL=VALUE form, to add to this replication controller. Can be specified multiple times.").Short('p').StringMap()
	createRCLabels           = cmdCreate.Flag("rc-label", "an RC label, in LABEL=VALUE form, to be applied to this replication controller. Can be specified multiple times.").Short('r').StringMap()
	createAvailabilityZone   = cmdCreate.Flag("availability-zone", "availability zone that RC should belong to").Short('a').Required().String()
	createClusterName        = cmdCreate.Flag("cluster-name", "availability zone that RC should belong to").Short('c').Required().String()
	createAllocationStrategy = cmdCreate.Flag("allocation-strategy", "determines how RC will allocate new nodes").Short('s').Required().String()

	cmdDelete   = kingpin.Command(cmdDeleteText, "Delete a replication controller")
	deleteID    = cmdDelete.Arg("id", "replication controller uuid to delete").Required().String()
	deleteForce = cmdDelete.Flag("force", "delete even if desired replicas > 0").Short('f').Bool()

	cmdReplicas = kingpin.Command(cmdReplicasText, "Set desired replica count of a replication controller")
	replicasID  = cmdReplicas.Arg("id", "replication controller uuid to modify").Required().String()
	replicasNum = cmdReplicas.Arg("replicas", "number of replicas desired").Required().Int()
	yes         = cmdReplicas.Flag("yes", "auto confirm the replica change (i.e. no confirmation prompt)").Short('y').Bool()

	cmdList  = kingpin.Command(cmdListText, "List replication controllers")
	listJSON = cmdList.Flag("json", "output the entire JSON object of each replication controller").Short('j').Bool()

	cmdGet      = kingpin.Command(cmdGetText, "Get replication controller")
	getID       = cmdGet.Arg("id", "replication controller uuid to get").Required().String()
	getManifest = cmdGet.Flag("manifest", "print just the manifest of the replication controller").Short('m').Bool()

	cmdGetStatus = kingpin.Command(cmdGetStatusText, "Get the status entry for a replication controller")
	getStatusID  = cmdGetStatus.Arg("id", "uuid of replication controller whose status should be fetched").Required().String()

	cmdEnable = kingpin.Command(cmdEnableText, "Enable replication controller")
	enableID  = cmdEnable.Arg("id", "replication controller uuid to enable").Required().String()

	cmdDisable = kingpin.Command(cmdDisableText, "Disable replication controller")
	disableID  = cmdDisable.Arg("id", "replication controller uuid to disable").Required().String()

	cmdRoll   = kingpin.Command(cmdRollText, "Rolling update from one replication controller to another")
	rollOldID = cmdRoll.Flag("old", "old replication controller uuid").Required().Short('o').String()
	rollNewID = cmdRoll.Flag("new", "new replication controller uuid").Required().Short('n').String()
	rollWant  = cmdRoll.Flag("desired", "number of replicas desired").Required().Short('d').Int()
	rollNeed  = cmdRoll.Flag("minimum", "minimum number of healthy replicas during update").Required().Short('m').Int()

	cmdDeleteRoll = kingpin.Command(cmdDeleteRollText, "Delete a rolling update.")
	deleteRollID  = cmdDeleteRoll.Flag("id", "rolling update uuid").Required().Short('i').String()

	cmdSchedup   = kingpin.Command(cmdSchedupText, "Schedule new rolling update (will be run by farm)")
	schedupOldID = cmdSchedup.Flag("old", "old replication controller uuid").Required().Short('o').String()
	schedupNewID = cmdSchedup.Flag("new", "new replication controller uuid").Required().Short('n').String()
	schedupWant  = cmdSchedup.Flag("desired", "number of replicas desired").Required().Short('d').Int()
	schedupNeed  = cmdSchedup.Flag("minimum", "minimum number of healthy replicas during update").Required().Short('m').Int()

	cmdUpdateManifest  = kingpin.Command(cmdUpdateManifestText, "DANGEROUS. Forcefully update the manifest for the given RC. Consider disabling the RC before invoking this command.")
	updateManifestRCID = cmdUpdateManifest.Arg("id", "replication controller uuid to update").Required().String()
	updateManifestPath = cmdUpdateManifest.Arg("manifest-path", "Path to a signed manifest").Required().String()

	cmdUpdateStrategy  = kingpin.Command(cmdUpdateStrategyText, "Forcefully update the allocation strategy in the manifest.")
	updateStrategyRCID = cmdUpdateStrategy.Flag("id", "replication controller uuid to update").Required().String()
	updateStrategy     = cmdUpdateStrategy.Flag("strategy", "allocation strategy to use for the replication controller").Required().String()
)

func main() {
	kingpin.Version(version.VERSION)
	cmd, opts, _ := flags.ParseWithConsulOptions()

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
	client := consul.NewConsulClient(opts)

	// we ignore the labels.ApplicatorWithoutWatches that
	// flags.ParseWithConsulOptions() gives you because that interface
	// doesn't support transactions which is required by the rc store. so
	// we just set up a labeler that directly accesses consul
	labeler := labels.NewConsulApplicator(client, 0, 0)

	rcStore := rcstore.NewConsul(client, labeler, 3)
	rcStatusStore := rcstatus.NewConsul(statusstore.NewConsul(client), consul.RCStatusNamespace)

	// The roll labeler CANT be an http applicator because it uses consul
	// transactions, so this might be different from labeler returned by
	// flags.ParseWithConsulOptions()
	rollLabeler := labels.NewConsulApplicator(client, 0, 0)
	rctl := rctlParams{
		httpClient: httpClient,
		baseClient: client,
		// rcStore copied so many times right now but might not all be
		// the same implementation of these various interfaces in
		// the future.
		rcs:               rcStore,
		rcStatusStore:     rcStatusStore,
		rollRCStore:       rcStore,
		rollRCStatusStore: rcStatusStore,
		rcLocker:          rcStore,
		rls:               rollstore.NewConsul(client, rollLabeler, nil),
		consuls:           consul.NewConsulStore(client),
		labeler:           labeler,
		hcheck:            checker.NewShadowTrafficHealthChecker(nil, nil, client, nil, nil, false, false),
		hclient:           nil,
		logger:            logger,
	}

	switch cmd {
	case cmdCreateText:
		rctl.Create(
			*createManifest,
			*createNodeSel,
			pc_fields.AvailabilityZone(*createAvailabilityZone),
			pc_fields.ClusterName(*createClusterName),
			*createPodLabels,
			*createRCLabels,
			rc_fields.Strategy(*createAllocationStrategy),
		)
	case cmdDeleteText:
		rctl.Delete(*deleteID, *deleteForce)
	case cmdReplicasText:
		rctl.SetReplicas(*replicasID, *replicasNum)
	case cmdListText:
		rctl.List(*listJSON)
	case cmdGetText:
		rctl.Get(*getID, *getManifest)
	case cmdGetStatusText:
		rctl.GetStatus(*getStatusID)
	case cmdEnableText:
		rctl.Enable(*enableID)
	case cmdDisableText:
		rctl.Disable(*disableID)
	case cmdRollText:
		rctl.RollingUpdate(*rollOldID, *rollNewID, *rollWant, *rollNeed)
	case cmdSchedupText:
		rctl.ScheduleUpdate(*schedupOldID, *schedupNewID, *schedupWant, *schedupNeed, client.KV())
	case cmdDeleteRollText:
		rctl.DeleteRollingUpdate(*deleteRollID, client.KV())
	case cmdUpdateManifestText:
		rctl.UpdateManifest(fields.ID(*updateManifestRCID), *updateManifestPath)
	case cmdUpdateStrategyText:
		rctl.UpdateStrategy(fields.ID(*updateStrategyRCID), fields.Strategy(*updateStrategy))
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

type Store interface {
	// for passing into a roll farm
	roll.Store
}

type ReplicationControllerStore interface {
	Create(
		manifest manifest.Manifest,
		nodeSelector klabels.Selector,
		availabilityZone pc_fields.AvailabilityZone,
		clusterName pc_fields.ClusterName,
		podLabels klabels.Set,
		additionalLabels klabels.Set,
		allocationStrategy rc_fields.Strategy,
	) (fields.RC, error)
	SetDesiredReplicas(id fields.ID, n int) error
	List() ([]fields.RC, error)
	Enable(id fields.ID) error
	Disable(id fields.ID) error
	Delete(id fields.ID, force bool) error
	Get(id fields.ID) (fields.RC, error)
	UpdateManifest(id fields.ID, man manifest.Manifest) error
	UpdateStrategy(id fields.ID, strategy fields.Strategy) error
}

type RollingUpdateStore interface {
	Delete(ctx context.Context, id roll_fields.ID) error
	CreateRollingUpdateFromExistingRCs(ctx context.Context, u roll_fields.Update, newRCLabels klabels.Set, rollLabels klabels.Set) (roll_fields.Update, error)
	Watch(quit <-chan struct{}, jitterWindow time.Duration) (<-chan []roll_fields.Update, <-chan error)
}

type RCStatusStore interface {
	Get(rcID rc_fields.ID) (rcstatus.Status, *api.QueryMeta, error)
}

// rctl is a struct for the data structures shared between commands
// each member function represents a single command that takes over from main
// and terminates the program on failure
type rctlParams struct {
	httpClient        *http.Client
	baseClient        consulutil.ConsulClient
	rcs               ReplicationControllerStore
	rcStatusStore     RCStatusStore
	rollRCStore       roll.ReplicationControllerStore
	rollRCStatusStore roll.RCStatusStore
	rcLocker          roll.ReplicationControllerLocker
	rcWatcher         rc.ReplicationControllerWatcher
	rls               RollingUpdateStore
	labeler           labels.ApplicatorWithoutWatches
	consuls           Store
	hcheck            checker.ShadowTrafficHealthChecker
	hclient           hclient.HealthServiceClient
	logger            logging.Logger
}

func (r rctlParams) Create(
	manifestPath string,
	nodeSelector string,
	availabilityZone pc_fields.AvailabilityZone,
	clusterName pc_fields.ClusterName,
	podLabels map[string]string,
	rcLabels map[string]string,
	allocationStrategy rc_fields.Strategy,
) {
	manifest, err := manifest.FromPath(manifestPath)
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

	newRC, err := r.rcs.Create(manifest, nodeSel, availabilityZone, clusterName, klabels.Set(podLabels), rcLabels, allocationStrategy)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not create replication controller in Consul")
	}
	r.logger.WithField("id", newRC.ID).Infoln("Created new replication controller")
}

func (r rctlParams) Delete(id string, force bool) {
	err := r.rcs.Delete(rc_fields.ID(id), force)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not delete replication controller in Consul")
	}
	r.logger.WithField("id", id).Infoln("Deleted replication controller")
}

func (r rctlParams) DeleteRollingUpdate(id string, txner transaction.Txner) {
	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err := r.rls.Delete(ctx, roll_fields.ID(id))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not delete RU. Consider a retry.")
	}

	err = transaction.MustCommit(ctx, txner)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not delete RU. Consider a retry.")
	}
}

func (r rctlParams) SetReplicas(id string, replicas int) {
	if replicas < 0 {
		r.logger.NoFields().Fatalln("Cannot set negative replica count")
	}

	fmt.Printf("setting the replica count to %d\n", replicas)
	if !*yes && !cli.Confirm() {
		r.logger.Fatal("user aborted")
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

func (r rctlParams) GetStatus(id string) {
	status, _, err := r.rcStatusStore.Get(rc_fields.ID(id))
	switch {
	case statusstore.IsNoStatus(err):
		fmt.Printf("no status found for %s\n", id)
		return
	case err != nil:
		r.logger.WithError(err).Fatalln("could not fetch RC status")
	}

	out, err := json.MarshalIndent(status, "", "    ")
	if err != nil {
		r.logger.WithError(err).Fatalln("could not print rc status as JSON")
	}
	fmt.Printf("%s\n", out)
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

func (r rctlParams) RollingUpdate(oldID, newID string, want, need int) {
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
	session := r.consuls.NewUnmanagedSession(sessionID, "")

	result := make(chan bool, 1)

	go func() {
		ctx, cancel := transaction.New(context.Background())
		defer cancel()
		watchDelay := 1 * time.Second
		result <- roll.NewUpdate(
			roll_fields.Update{
				OldRC:           rc_fields.ID(oldID),
				NewRC:           rc_fields.ID(newID),
				DesiredReplicas: want,
				MinimumReplicas: need,
			},
			r.consuls,
			r.baseClient,
			r.rcLocker,
			r.rollRCStore,
			r.rollRCStatusStore,
			r.rls,
			r.baseClient.KV(),
			r.hcheck,
			r.hclient,
			r.labeler,
			r.logger,
			session,
			watchDelay,
			alerting.NewNop(),
			false, // no audit logging
			auditlogstore.ConsulStore{}, // no audit logging
		).Run(ctx)
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

func (r rctlParams) ScheduleUpdate(oldID, newID string, want, need int, txner transaction.Txner) {
	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	_, err := r.rls.CreateRollingUpdateFromExistingRCs(
		ctx,
		roll_fields.Update{
			OldRC:           rc_fields.ID(oldID),
			NewRC:           rc_fields.ID(newID),
			DesiredReplicas: want,
			MinimumReplicas: need,
		}, nil, nil)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not create rolling update")
	}

	err = transaction.MustCommit(ctx, txner)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not create rolling update")
	}

	r.logger.WithField("id", newID).Infoln("Created new rolling update")
}

func (r rctlParams) UpdateManifest(id fields.ID, manifestPath string) {
	man, err := manifest.FromPath(manifestPath)

	err = r.rcs.UpdateManifest(id, man)
	if err != nil {
		r.logger.WithError(err).Fatalln("Manifest update failed! Please retry after checking the database")
	}
}

func (r rctlParams) UpdateStrategy(id fields.ID, strategy fields.Strategy) {
	err := r.rcs.UpdateStrategy(id, strategy)
	if err != nil {
		r.logger.WithError(err).Fatalln("Strategy update failed")
	}
}
