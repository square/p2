package ds

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	p2metrics "github.com/square/p2/pkg/metrics"
	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/daemonsetstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
	"github.com/rcrowley/go-metrics"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	// This label is applied to pods owned by a DS.
	DSIDLabel                = "daemon_set_id"
	DaemonSetStatusNamespace = statusstore.Namespace("daemon_set_farm")
)

var (
	DefaultRetryInterval = 5 * time.Minute
)

type DaemonSet interface {
	ID() fields.ID

	IsDisabled() bool

	// Returns the daemon set's pod id
	PodID() types.PodID

	ClusterName() fields.ClusterName

	GetNodeSelector() klabels.Selector

	// Returns a list of all nodes that are selected by this daemon set's selector
	EligibleNodes() ([]types.NodeName, error)

	MetricNames(suffix string) []string

	// WatchDesires watches for changes to its daemon set, then schedule/unschedule
	// pods to to the nodes that it is responsible for
	//
	// Whatever calls WatchDesires is responsible for sending signals for whether
	// the daemon set updated or deleted
	//
	// When this is first called, it assumes that the daemon set is created
	//
	// The caller is responsible for sending signals when something has been changed
	WatchDesires(
		ctx context.Context,
		updatedCh <-chan fields.DaemonSet,
		deletedCh <-chan fields.DaemonSet,
	) <-chan error

	// CurrentPods() returns all nodes that are scheduled by this daemon set
	CurrentPods() (types.PodLocations, error)

	Replicate(context.Context, <-chan []types.NodeName, <-chan struct{}, <-chan struct{}, <-chan manifest.Manifest)
}

type Labeler interface {
	SetLabelsTxn(ctx context.Context, labelType labels.Type, id string, labels map[string]string) error
	RemoveLabelTxn(
		ctx context.Context,
		labelType labels.Type,
		id string,
		name string,
	) error
	GetMatches(selector klabels.Selector, labelType labels.Type) ([]labels.Labeled, error)
	GetCachedMatches(selector klabels.Selector, labelType labels.Type, aggregationRate time.Duration) ([]labels.Labeled, error)
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
}

type LabelWatcher interface {
	WatchMatchDiff(
		selector klabels.Selector,
		labelType labels.Type,
		aggregationRate time.Duration,
		quitCh <-chan struct{},
	) <-chan *labels.LabeledChanges
}

type store interface {
	DeletePodTxn(ctx context.Context, podPrefix consul.PodPrefix, nodename types.NodeName, podID types.PodID) error
	NewUnmanagedSession(session, name string) consul.Session

	// For passing to the replication package:
	replication.Store
}

type DaemonSetStore interface {
	List() ([]fields.DaemonSet, error)
	Watch(quitCh <-chan struct{}) <-chan dsstore.WatchedDaemonSets
	Disable(id fields.ID) (fields.DaemonSet, error)
}

type StatusStore interface {
	Get(dsID fields.ID) (daemonsetstatus.Status, *api.QueryMeta, error)
	SetTxn(ctx context.Context, dsID fields.ID, status daemonsetstatus.Status) error
}

type Scheduler interface {
	EligibleNodes(manifest.Manifest, klabels.Selector) ([]types.NodeName, error)
}

// daemonSet wraps a daemon set struct with information required to manage it.
// Note: the inner fields.DaemonSet set may be modified during the lifetime of
// this struct, necessitating access synchronization with a mute
type daemonSet struct {
	fields.DaemonSet
	mu sync.Mutex

	logger                logging.Logger
	store                 store
	statusStore           StatusStore
	scheduler             Scheduler
	dsStore               DaemonSetStore
	txner                 transaction.Txner
	applicator            Labeler
	watcher               LabelWatcher
	healthChecker         *checker.ConsulHealthChecker
	healthWatchDelay      time.Duration
	statusWritingInterval time.Duration

	// unlocker is useful to ensure that certain operations only succeed if
	// the farm that spawned this daemon set still holds the lock
	unlocker consul.TxnUnlocker

	// This is the replication that the enact go routine that is running.
	// Access to it is protected by dsReplicationMu
	dsReplication   *dsReplication
	dsReplicationMu sync.Mutex

	// Indicates how long to wait between updating each node during a replication
	rateLimitInterval time.Duration

	// allow stale reads of matching pods.  We allow stale matches for daemon
	// set queries because the consequent operations are idempotent.
	cachedPodMatch bool
	// labelsAggregationRate determines the polling rate the labeler should use
	// on the database when using cached matches. If zero, it will default
	// to a sane value (see labels.DefaultAggregationRate)
	labelsAggregationRate time.Duration

	retryInterval time.Duration
}

type dsReplication struct {
	replication replication.Replication
	nodeQueue   chan<- types.NodeName
}

func New(
	fields fields.DaemonSet,
	dsStore DaemonSetStore,
	store store,
	txner transaction.Txner,
	applicator Labeler,
	watcher LabelWatcher,
	labelsAggregationRate time.Duration,
	logger logging.Logger,
	healthChecker *checker.ConsulHealthChecker,
	rateLimitInterval time.Duration,
	cachedPodMatch bool,
	healthWatchDelay time.Duration,
	retryInterval time.Duration,
	unlocker consul.TxnUnlocker,
	statusStore StatusStore,
	statusWritingInterval time.Duration,
) DaemonSet {

	if retryInterval == 0 {
		retryInterval = DefaultRetryInterval
	}

	return &daemonSet{
		DaemonSet: fields,

		dsStore:               dsStore,
		store:                 store,
		txner:                 txner,
		logger:                logger,
		applicator:            applicator,
		watcher:               watcher,
		scheduler:             scheduler.NewApplicatorScheduler(applicator),
		healthChecker:         healthChecker,
		healthWatchDelay:      healthWatchDelay,
		dsReplication:         nil,
		rateLimitInterval:     rateLimitInterval,
		cachedPodMatch:        cachedPodMatch,
		labelsAggregationRate: labelsAggregationRate,
		retryInterval:         retryInterval,
		unlocker:              unlocker,
		statusWritingInterval: statusWritingInterval,
		statusStore:           statusStore,
	}
}

func (ds *daemonSet) ID() fields.ID {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.DaemonSet.ID
}

func (ds *daemonSet) IsDisabled() bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.DaemonSet.Disabled
}

func (ds *daemonSet) PodID() types.PodID {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.DaemonSet.PodID
}

func (ds *daemonSet) ClusterName() fields.ClusterName {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.DaemonSet.Name
}

func (ds *daemonSet) GetNodeSelector() klabels.Selector {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.DaemonSet.NodeSelector
}

func (ds *daemonSet) Manifest() manifest.Manifest {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.DaemonSet.Manifest
}

func (ds *daemonSet) MinHealth() int {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.DaemonSet.MinHealth
}

func (ds *daemonSet) EligibleNodes() ([]types.NodeName, error) {
	ds.mu.Lock()
	m := ds.DaemonSet.Manifest
	nodeSelector := ds.DaemonSet.NodeSelector
	ds.mu.Unlock()
	return ds.scheduler.EligibleNodes(m, nodeSelector)
}

func (ds *daemonSet) MetricNames(suffix string) []string {
	prefix := "daemonset"
	middles := []string{
		ds.PodID().String() + "." + ds.ClusterName().String(),
		ds.ID().String(),
	}
	var ret []string
	for _, middle := range middles {
		ret = append(ret, fmt.Sprintf("%s.%s.%s", prefix, middle, suffix))
	}
	return ret
}

func (ds *daemonSet) WatchDesires(
	ctx context.Context,
	updatedCh <-chan fields.DaemonSet,
	deletedCh <-chan fields.DaemonSet,
) <-chan error {
	errCh := make(chan error)
	// TODO: make WatchMatchDiff take a context instead of a quit channel
	watchMatchQuitCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(watchMatchQuitCh)
	}()

	go func() {
		ds.publishStatus(ctx)
	}()

	// buffer this channel so we don't block the WatchDesires() loop on
	// replications being slow. In steady state we expect the rate of nodes
	// being added to a daemon set to be much smaller than the speed at
	// which they're deployed, but there might be temporary bursts when the
	// daemon set's manifest is changed which causes all nodes to be
	// deployed (i.e. it might take hours to process just a single slice
	// off this channel). Buffering with a size of 1000 should be plenty
	// big to ride out storms of full deploys without blocking
	// responsiveness of WatchDesires()
	nodesToAdd := make(chan []types.NodeName, 1000)
	pauseReplication := make(chan struct{})
	unpauseReplication := make(chan struct{})
	manifestChange := make(chan manifest.Manifest)
	go ds.Replicate(ctx, nodesToAdd, pauseReplication, unpauseReplication, manifestChange)

	nodesChangedCh := ds.watcher.WatchMatchDiff(ds.NodeSelector, labels.NODE, ds.labelsAggregationRate, watchMatchQuitCh)
	// Do something whenever something is changed
	go func() {
		var err error
		var addedNodes []types.NodeName
		var eligibleNodes []types.NodeName
		defer close(errCh)
		defer func() {
			close(nodesToAdd)
			close(pauseReplication)
			close(unpauseReplication)
			close(manifestChange)
		}()

		// Make a timer and stop it so the receieve from channel does not occur
		// until a reset happens
		timer := time.NewTimer(time.Duration(0))

		paused := true

		// Schedule all the pods when we first start watching
		if !ds.IsDisabled() {
			ds.logger.NoFields().Infof("Received new daemon set: %s", ds.ID)
			eligibleNodes, err = ds.EligibleNodes()
			if err != nil {
				err = util.Errorf("Unable to compute eligible nodes: %v", err)
			}
			nodesToAdd <- eligibleNodes
			paused = false
		}

		for {
			if err != nil {
				ds.logger.Errorf("An error has occurred in the daemon set, retrying if no changes are made in %d. %v", ds.retryInterval, err)
				select {
				case errCh <- err:
					// Retry the replication in the RetryInterval's duration
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(ds.retryInterval)
					// This is required in case the user disables the daemon set
					// so that the timer would be stopped after
					err = nil
				case <-ctx.Done():
					return
				}
			} else {
				// If err == nil, stop the timer because there is no need to retry
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}

			// Precondition: err == nil
			// err should be assigned a value in this select statement unless
			// it returns or the daemon set is disabled
			select {
			case newDS, ok := <-updatedCh:
				if !ok {
					// channel closed
					return
				}
				if ds.ID() != newDS.ID {
					err = util.Errorf("Expected uuid to be the same, expected '%v', got '%v'", ds.ID(), newDS.ID)
					continue
				}

				var oldManifestSHA, newManifestSHA string
				oldManifestSHA, err = ds.Manifest().SHA()
				if err != nil {
					err = util.Errorf("could not compute sha of old manifest: %s", err)
					continue
				}

				newManifestSHA, err = newDS.Manifest.SHA()
				if err != nil {
					err = util.Errorf("could not compute sha of new manifest: %s", err)
					continue
				}

				manifestChanged := newManifestSHA != oldManifestSHA
				if manifestChanged {
					ds.logger.Infoln("manifest changed")
				}

				ds.mu.Lock()
				ds.DaemonSet = newDS
				ds.mu.Unlock()

				if reportErr := ds.reportEligible(); reportErr != nil {
					// An error in sending the metrics shouldn't stop us from doing updates.
					// Report it, and move on.
					ds.logger.WithError(reportErr).Warnf("Error reporting number of eligible nodes")
				}

				if ds.Disabled {
					ds.logger.Infoln("daemon set disabled, pausing replication")
					pauseReplication <- struct{}{}
					paused = true
					continue
				}

				if paused || manifestChanged {
					if paused {
						ds.logger.Infoln("daemon set enabled, unpausing replication")
					}

					unpauseReplication <- struct{}{}
					manifestChange <- ds.Manifest()

					ds.logger.Infoln("kicking off replication for all nodes")
					// schedule all the nodes again cuz the manifest changed or we unpaused
					eligibleNodes, err = ds.EligibleNodes()
					if err != nil {
						err = util.Errorf("Unable to compute eligible nodes: %v", err)
						continue
					}

					// only mark as unpaused after we've successfully computed the full node set
					paused = false

					nodesToAdd <- eligibleNodes
				}

				err = ds.removePods()
				if err != nil {
					err = util.Errorf("Unable to remove pods from intent tree: %v", err)
					continue
				}
				addedNodes, err = ds.computeNodesToAdd()
				if err != nil {
					err = util.Errorf("Unable to add pods to intent tree: %v", err)
					continue
				}

				nodesToAdd <- addedNodes

			case deleteDS, ok := <-deletedCh:
				if !ok {
					return
				}
				// Deleting a daemon sets has no effect
				ds.logger.WithFields(logrus.Fields{"id": deleteDS, "node_selector": ds.NodeSelector.String()}).Infof("Daemon Set Deletion is disabled and has no effect. You may want to clean this up manually.")
				return

			case _, ok := <-nodesChangedCh:
				if !ok {
					// channel closed
					return
				}
				if reportErr := ds.reportEligible(); reportErr != nil {
					// An error in sending the metrics shouldn't stop us from doing updates.
					// Report it, and move on.
					ds.logger.WithError(reportErr).Warnf("Error reporting number of eligible nodes")
				}
				if ds.Disabled {
					continue
				}

				err = ds.removePods()
				if err != nil {
					err = util.Errorf("Unable to remove pods from intent tree: %v", err)
					continue
				}
				addedNodes, err = ds.computeNodesToAdd()
				if err != nil {
					err = util.Errorf("Unable to add pods to intent tree: %v", err)
					continue
				}
				nodesToAdd <- addedNodes

			case <-timer.C:
				if ds.Disabled {
					paused = true
					continue
				}

				if paused {
					unpauseReplication <- struct{}{}
					// schedule all the nodes again because we might have been paused for a while
					eligibleNodes, err = ds.EligibleNodes()
					if err != nil {
						err = util.Errorf("Unable to compute eligible nodes: %v", err)
						continue
					}

					// only mark as unpaused after we've successfully computed the full node set
					paused = false

					nodesToAdd <- eligibleNodes
				}

				ds.logger.Infoln("retrying after error timeout")
				// Account for any operations that could have failed and retry the replication
				err = ds.removePods()
				if err != nil {
					err = util.Errorf("Unable to remove pods from intent tree: %v", err)
					continue
				}
				addedNodes, err = ds.computeNodesToAdd()
				if err != nil {
					err = util.Errorf("Unable to add pods to intent tree: %v", err)
					continue
				}
				nodesToAdd <- addedNodes

			case <-ctx.Done():
				return
			}
		}
	}()

	return errCh
}

func (ds *daemonSet) reportEligible() error {
	eligible, err := ds.EligibleNodes()
	if err != nil {
		return util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}
	for _, name := range ds.MetricNames("total") {
		gauge := metrics.GetOrRegisterGauge(name, p2metrics.Registry)
		gauge.Update(int64(len(eligible)))
	}
	return nil
}

// computeNodesToAdd schedules pods for all unscheduled nodes selected by ds.nodeSelector
func (ds *daemonSet) computeNodesToAdd() ([]types.NodeName, error) {
	podLocations, err := ds.CurrentPods()
	if err != nil {
		return nil, util.Errorf("Error retrieving pod locations from daemon set: %v", err)
	}
	currentNodes := podLocations.Nodes()

	eligible, err := ds.EligibleNodes()
	if err != nil {
		return nil, util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}
	// TODO: Grab a lock here for the pod_id before adding something to check
	// contention and then disable

	// Get the difference in nodes that we need to schedule on and then sort them
	// for deterministic ordering
	toScheduleSorted := types.NewNodeSet(eligible...).Difference(types.NewNodeSet(currentNodes...)).ListNodes()

	if len(toScheduleSorted) > 0 {
		ds.logger.Infof("Need to schedule %d nodes: %s", len(toScheduleSorted), toScheduleSorted)
	}

	return toScheduleSorted, nil
}

// removePods unschedules pods for all scheduled nodes not selected
// by ds.nodeSelector
func (ds *daemonSet) removePods() error {
	podLocations, err := ds.CurrentPods()
	if err != nil {
		return util.Errorf("Error retrieving pod locations from daemon set: %v", err)
	}
	currentNodes := podLocations.Nodes()

	eligible, err := ds.EligibleNodes()
	if err != nil {
		return util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}

	if len(eligible) == 0 {
		return util.Errorf("No nodes eligible; daemon set refuses to unschedule everything.")
	}

	// Get the difference in nodes that we need to unschedule on and then sort them
	// for deterministic ordering
	toUnscheduleSorted := types.NewNodeSet(currentNodes...).Difference(types.NewNodeSet(eligible...)).ListNodes()
	if len(toUnscheduleSorted) == 0 {
		return nil
	}

	ds.logger.NoFields().Infof("Need to unschedule %d nodes, remaining on %d nodes", len(toUnscheduleSorted), len(eligible))

	// NOTE: there's it's possible that this node is in the replication's
	// nodeQueue still and therefore it will be scheduled again, but for
	// now we're willing to deal with that tradeoff
	for _, node := range toUnscheduleSorted {
		err := ds.unschedule(node)
		if err != nil {
			return util.Errorf("Error unscheduling node: %v", err)
		}
	}

	return nil
}

func (ds *daemonSet) unschedule(node types.NodeName) error {
	ds.logger.NoFields().Infof("Unscheduling '%v' in node '%v' with daemon set uuid '%v'", ds.Manifest().ID(), node, ds.ID())

	ctx, cancel := transaction.New(context.Background())
	defer cancel()

	// Will remove the following key:
	// <consul.INTENT_TREE>/<node>/<ds.Manifest.ID()>
	err := ds.store.DeletePodTxn(ctx, consul.INTENT_TREE, node, ds.Manifest().ID())
	if err != nil {
		return util.Errorf("unable to form pod deletion transaction for pod id '%v' from node '%v': %v", ds.Manifest().ID(), node, err)
	}

	// Will remove the following label on the key <labels.POD>/<node>/<ds.Manifest.ID()>: DSIDLabel
	// This is for indicating that this pod path no longer belongs to this daemon set
	id := labels.MakePodLabelKey(node, ds.Manifest().ID())
	err = ds.applicator.RemoveLabelTxn(ctx, labels.POD, id, DSIDLabel)
	if err != nil {
		return util.Errorf("error adding label removal to transaction: %v", err)
	}

	err = transaction.MustCommit(ctx, ds.txner)
	if err != nil {
		return util.Errorf("error unscheduling %s from %s: %s", ds.Manifest().ID(), node, err)
	}

	return nil
}

func (ds *daemonSet) Replicate(
	ctx context.Context,
	nodesToAdd <-chan []types.NodeName,
	pauseReplication <-chan struct{},
	unpauseReplication <-chan struct{},
	manifestChange <-chan manifest.Manifest,
) {
	nodeQueue := make(chan types.NodeName)

	// retry starting the replication until there are no errors
	for {
		nodes, err := ds.EligibleNodes()
		if err != nil {
			ds.logger.WithError(err).Errorln("error retrieving eligible nodes for daemon set")
			continue
		}

		ds.logger.Infof("Preparing to publish the following nodes: %v", nodes)

		thisHost, err := os.Hostname()
		if err != nil {
			ds.logger.Errorf("Could not retrieve hostname: %s", err)
			thisHost = ""
		}
		thisUser, err := user.Current()
		if err != nil {
			ds.logger.Errorf("Could not retrieve user: %s", err)
			thisUser = &user.User{}
		}

		lockMessage := fmt.Sprintf("%q from %q at %q", thisUser.Username, thisHost, time.Now())
		repl, err := replication.NewReplicator(
			ds.Manifest(),
			ds.logger,
			nodes,
			50, // always use max parallelism for daemon set deploys
			ds.store,
			ds.txner,
			ds.applicator,
			*ds.healthChecker,
			health.HealthState(health.Passing),
			lockMessage,
			ds.Timeout,
			ds.healthWatchDelay,
		)
		if err != nil {
			ds.logger.WithError(err).Errorln("Could not initialize replicator")
			continue
		}

		ds.logger.Info("New replicator was made")

		podLabels := map[string]string{
			DSIDLabel: ds.ID().String(),
		}

		// Replication locks are designed to make sure that two replications to
		// the same nodes cannot occur at the same time. The granularity is
		// pod-wide as an optimization for consul performance (only need to
		// lock a single key) with limited downside when human operators are
		// executing deploys, because the likelihood of a lock collision is
		// low. With daemon sets, locking is not necessary because the node
		// sets should not overlap when they are managed properly. Even when
		// there is a node overlap between two daemon sets, a simple mutual
		// exclusion lock around replication will not prevent the pod manifest
		// on an overlapped node from thrashing. Therefore, it makes sense for
		// daemon sets to ignore this locking mechanism and always try to
		// converge nodes to the specified manifest
		replication, errCh, err := repl.InitializeDaemonSetReplication(
			nodeQueue,
			replication.DefaultConcurrentReality,
			ds.rateLimitInterval,
			podLabels,
		)
		if err != nil {
			ds.logger.WithError(err).Errorf("Unable to initialize replication: %s", err)
			continue
		}

		ds.logger.Info("Replication initialized")

		// auto-drain this channel
		go func() {
			for err := range errCh {
				ds.logger.Errorf("Error occurred in replication: '%v'", err)
			}
		}()

		// Set a new replication
		dsReplication := &dsReplication{
			replication: replication,
			nodeQueue:   nodeQueue,
		}
		ds.setDSReplication(dsReplication)

		go replication.Enact()

		break
	}

	ds.logger.Info("Replication enacted")

	paused := false
	addMoreNodes := func(moreNodes []types.NodeName) {
		if paused {
			return
		}

		for _, node := range moreNodes {
			// prioritize pauses and manifest changes
			select {
			case <-pauseReplication:
				paused = true
				// throw away the rest of the nodes and wait
				// for unpause
				return
			case man := <-manifestChange:
				if man != nil {
					ds.getDSReplication().replication.SetManifest(man)
				}
			default:
			}

			select {
			case nodeQueue <- node:
			case <-ctx.Done():
				return
			case <-pauseReplication:
				paused = true
				return
			case <-unpauseReplication:
				paused = false
			}
		}
	}

	for {
		select {
		case moreNodes, ok := <-nodesToAdd:
			if !ok {
				// upstream closed the channel
				return
			}

			addMoreNodes(moreNodes)
		case man := <-manifestChange:
			if man != nil {
				ds.getDSReplication().replication.SetManifest(man)
			}
		case <-pauseReplication:
			paused = true
		case <-unpauseReplication:
			paused = false
		case <-ctx.Done():
			return
		}
	}
}

func (ds *daemonSet) setDSReplication(rep *dsReplication) {
	ds.dsReplicationMu.Lock()
	defer ds.dsReplicationMu.Unlock()
	ds.dsReplication = rep
}

func (ds *daemonSet) getDSReplication() *dsReplication {
	ds.dsReplicationMu.Lock()
	defer ds.dsReplicationMu.Unlock()
	return ds.dsReplication
}

func (ds *daemonSet) CurrentPods() (types.PodLocations, error) {
	// Changing DaemonSet.ID is not permitted, so as long as there is no uuid
	// collision, this will always get the current pod path that this daemon set
	// had scheduled on
	dsID := ds.ID()
	selector := klabels.Everything().Add(DSIDLabel, klabels.EqualsOperator, []string{dsID.String()})

	var podMatches []labels.Labeled
	var err error
	if ds.cachedPodMatch {
		podMatches, err = ds.applicator.GetCachedMatches(selector, labels.POD, ds.labelsAggregationRate)
	} else {
		podMatches, err = ds.applicator.GetMatches(selector, labels.POD)
	}
	if err != nil {
		return nil, util.Errorf("Unable to get matches on pod tree: %v", err)
	}

	result := make(types.PodLocations, len(podMatches))
	for i, podMatch := range podMatches {
		// ID will be something like <node>/<PodID>
		node, podID, err := labels.NodeAndPodIDFromPodLabel(podMatch)
		if err != nil {
			return nil, err
		}
		result[i].Node = node
		result[i].PodID = podID
	}

	return result, nil
}

func (ds *daemonSet) publishStatus(ctx context.Context) {
	var lastStatus daemonsetstatus.Status

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(ds.statusWritingInterval):
		}

		// seed lastStatus if it's still the zero value
		if lastStatus.ManifestSHA == "" {
			var err error
			lastStatus, _, err = ds.statusStore.Get(ds.ID())
			switch {
			case statusstore.IsNoStatus(err):
			case err != nil:
				ds.logger.WithError(err).Errorln("could not write daemon set status")
				continue
			}
		}

		written, err := ds.writeNewestStatus(ctx, lastStatus)
		if err != nil {
			ds.logger.WithError(err).Errorln("could not write daemon set status")
			continue
		}

		lastStatus = written
	}
}

// writeNewestStatus writes the latest status for the daemon set to consul if
// it differs from the most recently written one. It handles the case where
// lastStatus is the zero status which might be the case the first time the
// daemon set's status is written
func (ds *daemonSet) writeNewestStatus(ctx context.Context, lastStatus daemonsetstatus.Status) (daemonsetstatus.Status, error) {
	var toWrite daemonsetstatus.Status
	manifestSHA, err := ds.Manifest().SHA()
	if err != nil {
		return daemonsetstatus.Status{}, err
	}

	toWrite.ManifestSHA = manifestSHA
	toWrite.NodesDeployed = lastStatus.NodesDeployed
	if toWrite.ManifestSHA != lastStatus.ManifestSHA {
		// reset the deployed count if the manifest has changed
		toWrite.NodesDeployed = 0
	}

	dsReplication := ds.getDSReplication()
	if dsReplication == nil {
		toWrite.ReplicationInProgress = false
	} else {
		// NOTE: it's always in progress now so we should probably get rid of this key
		toWrite.ReplicationInProgress = dsReplication.replication.InProgress()

		// ensure that NodesDeployed doesn't backtrack (which it might if the replication was restarted)
		if int(dsReplication.replication.CompletedCount()) > toWrite.NodesDeployed {
			toWrite.NodesDeployed = int(dsReplication.replication.CompletedCount())
		}
	}

	if toWrite == lastStatus {
		// nothing to do
		return lastStatus, nil
	}

	writeCtx, cancel := transaction.New(ctx)
	defer cancel()

	err = ds.unlocker.CheckLockedTxn(writeCtx)
	if err != nil {
		return daemonsetstatus.Status{}, err
	}

	err = ds.statusStore.SetTxn(writeCtx, ds.ID(), toWrite)
	if err != nil {
		return daemonsetstatus.Status{}, err
	}

	err = transaction.MustCommit(writeCtx, ds.txner)
	if err != nil {
		return daemonsetstatus.Status{}, err
	}

	return toWrite, nil
}
