package pcstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	"github.com/rcrowley/go-metrics"
	klabels "k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/util/sets"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pc/fields"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

const podClusterTree string = "pod_clusters"

var (
	NoPodCluster            error = errors.New("No pod cluster found")
	PodClusterAlreadyExists error = errors.New("Pod cluster already exists")
)

type Session interface {
	Lock(key string) (consul.Unlocker, error)
}

// WatchedPodCluster is an Either type: it will have 1 one of pc xor err
type WatchedPodCluster struct {
	PodCluster *fields.PodCluster
	Err        error
}

type WatchedPodClusters struct {
	Clusters []*fields.PodCluster
	Err      error
}

// Subset of metrics.Registry interface
type MetricsRegistry interface {
	Get(metricName string) interface{}
	Register(metricName string, metric interface{}) error
}

func IsNotExist(err error) bool {
	return err == NoPodCluster
}

func IsAlreadyExists(err error) bool {
	return err == PodClusterAlreadyExists
}

type consulKV interface {
	Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

type ConsulStore struct {
	kv      consulKV
	labeler pcLabeler
	// labelAggregationRate controls how often the store will query for
	// labels when servicing selector watches. A zero-value will result in
	// a sane default (see labels.DefaultAggregationRate)
	labelAggregationRate time.Duration
	watcher              pcWatcher

	logger logging.Logger

	metricsRegistry MetricsRegistry
}

type pcLabeler interface {
	SetLabel(labelType labels.Type, id, name, value string) error
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
	SetLabels(labelType labels.Type, id string, labels map[string]string) error
	RemoveAllLabels(labelType labels.Type, id string) error
	GetMatches(selector klabels.Selector, labelType labels.Type) ([]labels.Labeled, error)
}

type pcWatcher interface {
	WatchMatches(
		selector klabels.Selector,
		labelType labels.Type,
		aggregationRate time.Duration,
		quitCh <-chan struct{},
	) (chan []labels.Labeled, error)
}

func NewConsul(
	client consulutil.ConsulClient,
	labeler pcLabeler,
	labelAggregationRate time.Duration,
	watcher pcWatcher,
	logger *logging.Logger,
) *ConsulStore {
	return &ConsulStore{
		kv:      client.KV(),
		logger:  *logger,
		labeler: labeler,
		watcher: watcher,
	}
}

func (s *ConsulStore) SetMetricsRegistry(reg MetricsRegistry) {
	s.metricsRegistry = reg
}

func (s *ConsulStore) Create(
	podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName,
	podSelector klabels.Selector,
	annotations fields.Annotations,
	allocationStrategy rc_fields.Strategy,
	session Session,
) (fields.PodCluster, error) {
	id := fields.ID(uuid.New())

	unlocker, err := s.lockForCreation(podID, availabilityZone, clusterName, session)
	if err != nil {
		return fields.PodCluster{}, err
	}
	defer unlocker.Unlock()

	existing, err := s.FindWhereLabeled(podID, availabilityZone, clusterName)
	if err != nil {
		return fields.PodCluster{}, util.Errorf("Couldn't determine if pod cluster exists already: %v", err)
	}
	if len(existing) > 0 {
		return existing[0], PodClusterAlreadyExists
	}

	pc := fields.PodCluster{
		ID:                 id,
		PodID:              podID,
		AvailabilityZone:   availabilityZone,
		Name:               clusterName,
		PodSelector:        podSelector,
		Annotations:        annotations,
		AllocationStrategy: allocationStrategy,
	}

	key, err := pcPath(id)
	if err != nil {
		return fields.PodCluster{}, err
	}

	jsonPC, err := json.Marshal(pc)
	if err != nil {
		// Probably the annotations don't marshal to JSON
		return fields.PodCluster{}, util.Errorf("Unable to marshal pod cluster as JSON: %s", err)
	}

	// the chance of the UUID already existing is vanishingly small, but
	// technically not impossible, so we should use the CAS index to guard
	// against duplicate UUIDs
	success, _, err := s.kv.CAS(&api.KVPair{
		Key:         key,
		Value:       jsonPC,
		ModifyIndex: 0,
	}, nil)
	if err != nil {
		return fields.PodCluster{}, consulutil.NewKVError("cas", key, err)
	}

	if !success {
		return fields.PodCluster{}, util.Errorf("Could not set pod cluster at path '%s'", key)
	}

	err = s.setLabelsForPC(pc)
	if err != nil {
		// TODO: what if this delete fails?
		deleteErr := s.Delete(pc.ID)
		if deleteErr != nil {
			err = util.Errorf("%s\n%s", err, deleteErr)
		}
		return fields.PodCluster{}, err
	}

	return pc, nil
}

func (s *ConsulStore) setLabelsForPC(pc fields.PodCluster) error {
	pcLabels := klabels.Set{}
	pcLabels[fields.PodIDLabel] = pc.PodID.String()
	pcLabels[fields.AvailabilityZoneLabel] = pc.AvailabilityZone.String()
	pcLabels[fields.ClusterNameLabel] = pc.Name.String()

	return s.labeler.SetLabels(labels.PC, pc.ID.String(), pcLabels)
}

func (s *ConsulStore) Get(id fields.ID) (fields.PodCluster, error) {
	key, err := pcPath(id)
	if err != nil {
		return fields.PodCluster{}, err
	}

	kvp, _, err := s.kv.Get(key, nil)
	if err != nil {
		return fields.PodCluster{}, consulutil.NewKVError("get", key, err)
	}
	if kvp == nil {
		// ID didn't exist
		return fields.PodCluster{}, NoPodCluster
	}

	return kvpToPC(kvp)
}

func (s *ConsulStore) Delete(id fields.ID) error {
	key, err := pcPath(id)
	if err != nil {
		return err
	}

	_, err = s.kv.Delete(key, nil)
	if err != nil {
		return consulutil.NewKVError("delete", key, err)
	}

	return s.labeler.RemoveAllLabels(labels.PC, id.String())
}

func (s *ConsulStore) List() ([]fields.PodCluster, error) {
	pairs, _, err := s.kv.List(podClusterTree+"/", nil)
	if err != nil {
		return nil, err
	}

	return kvpsToPC(pairs)
}

// performs a safe (ie check-and-set) mutation of the pc with the given id,
// using the given function
// if the mutator returns an error, it will be propagated out
// if the returned PC has id="", then it will be deleted
func (s *ConsulStore) MutatePC(
	id fields.ID,
	mutator func(fields.PodCluster) (fields.PodCluster, error),
) (fields.PodCluster, error) {
	pcp, err := pcPath(id)
	if err != nil {
		return fields.PodCluster{}, err
	}

	kvp, meta, err := s.kv.Get(pcp, nil)
	if err != nil {
		return fields.PodCluster{}, consulutil.NewKVError("get", pcp, err)
	}

	if kvp == nil {
		return fields.PodCluster{}, NoPodCluster
	}

	pc, err := kvpToPC(kvp)
	if err != nil {
		return fields.PodCluster{}, err
	}

	pc, err = mutator(pc)
	if err != nil {
		return fields.PodCluster{}, err
	}

	jsonPC, err := json.Marshal(pc)
	if err != nil {
		// Probably the annotations don't marshal to JSON
		return fields.PodCluster{}, util.Errorf("Unable to marshal pod cluster as JSON: %s", err)
	}

	// the chance of the UUID already existing is vanishingly small, but
	// technically not impossible, so we should use the CAS index to guard
	// against duplicate UUIDs
	var success bool
	success, _, err = s.kv.CAS(&api.KVPair{
		Key:         pcp,
		Value:       jsonPC,
		ModifyIndex: meta.LastIndex,
	}, nil)
	if err != nil {
		return fields.PodCluster{}, consulutil.NewKVError("cas", pcp, err)
	}

	if !success {
		return fields.PodCluster{}, util.Errorf("Could not set pod cluster at path '%s'", pcp)
	}

	err = s.setLabelsForPC(pc)
	if err != nil {
		// TODO: what if this delete fails?
		deleteErr := s.Delete(pc.ID)
		if deleteErr != nil {
			err = util.Errorf("%s\n%s", err, deleteErr)
		}
		return fields.PodCluster{}, err
	}

	return pc, nil
}

func pcPath(pcID fields.ID) (string, error) {
	if pcID == "" {
		return "", util.Errorf("Path requested for empty pod cluster ID")
	}

	return path.Join(podClusterTree, pcID.String()), nil
}

func (s *ConsulStore) lockForCreation(podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName,
	session Session) (consul.Unlocker, error) {
	return session.Lock(pcCreateLockPath(podID, availabilityZone, clusterName))
}

func pcCreateLockPath(podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName) string {
	return path.Join(consul.LOCK_TREE, podID.String(), availabilityZone.String(), clusterName.String())
}

func pcSyncLockPath(id fields.ID, syncerType ConcreteSyncerType) string {
	return path.Join(consul.LOCK_TREE, podClusterTree, id.String(), syncerType.String())
}

// FindWhereLabeled returns a slice of pod clusters that are labeled
// with the passed information. Although pod clusters should always be
// unique for this 3-ple, this method will return a slice in cases
// where duplicates are discovered. It is up to clients to decide how
// to respond to such situations.
func (s *ConsulStore) FindWhereLabeled(podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName) ([]fields.PodCluster, error) {

	sel := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{availabilityZone.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	podClusters, err := s.labeler.GetMatches(sel, labels.PC)
	if err != nil {
		return nil, err
	}
	ret := make([]fields.PodCluster, len(podClusters))
	for i, pc := range podClusters {
		ret[i], err = s.Get(fields.ID(pc.ID))
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

// Watch watches the entire podClusterTree for changes.
// It will return a blocking channel on which the client can read
// WatchedPodCluster objects. The goroutine maintaining the watch will block on
// writing to this channel so it's up to the caller to read it with haste.
func (s *ConsulStore) Watch(quit <-chan struct{}) <-chan WatchedPodClusters {
	inCh := make(chan api.KVPairs)
	outCh := make(chan WatchedPodClusters)
	errChan := make(chan error, 1)

	go consulutil.WatchPrefix(podClusterTree, s.kv, inCh, quit, errChan, 5*time.Second, 1*time.Minute)

	go func() {
		var kvp api.KVPairs
		for {
			select {
			case <-quit:
				return
			case err := <-errChan:
				s.logger.WithError(err).Errorf("WatchPrefix returned error, recovered.")
			case kvp = <-inCh:
				if kvp == nil {
					// nothing to do
					continue
				}
			}

			clusters := WatchedPodClusters{}

			pcs, err := kvpsToPC(kvp)
			if err != nil {
				clusters.Err = err
				select {
				case <-quit:
					return
				case outCh <- clusters:
					continue
				}
			}

			for _, pc := range pcs {
				// We can't just use &pc because that would be a pointer to
				// the iteration variable
				pcPtr := pc
				clusters.Clusters = append(clusters.Clusters, &pcPtr)
			}

			select {
			case outCh <- clusters:
			case <-quit:
				return
			}
		}
	}()

	return outCh
}

// WatchPodCluster implements a watch for the Pod cluster at _id_
// It will return a blocking channel on which the client can read
// WatchedPodCluster objects. The goroutine maintaining the watch will block on
// writing to this channel so it's up to the caller to read it with haste.
// This function will return ErrNoPodCluster if the podCluster goes away. In
// this case, the caller should close the quit chan.
// The caller may shutdown this watch by sending a sentinel on the quitChan.
func (s *ConsulStore) WatchPodCluster(id fields.ID, quit <-chan struct{}) <-chan WatchedPodCluster {
	inCh := make(chan *api.KVPair)
	outCh := make(chan WatchedPodCluster)
	errChan := make(chan error, 1)
	quitWatch := make(chan struct{})
	key := path.Join(podClusterTree, string(id))

	go consulutil.WatchSingle(key, s.kv, inCh, quitWatch, errChan)

	go func() {
		var kvp *api.KVPair
		for {
			select {
			case <-quit:
				return
			case err := <-errChan:
				s.logger.WithError(err).Errorf("WatchSingle returned error, recovered.")
			case kvp = <-inCh:
				if kvp == nil { // PodCluster at _id_ has been deleted
					select {
					case <-quit:
						return
					case outCh <- WatchedPodCluster{PodCluster: nil, Err: NoPodCluster}:
					}

					return
				}
			}

			pc, err := kvpToPC(kvp)
			var wpc WatchedPodCluster
			if err != nil {
				wpc.Err = err
			} else {
				wpc.PodCluster = &pc
			}

			select {
			case <-quit:
				return
			case outCh <- wpc:
			}
		}
	}()

	return outCh
}

type podClusterChange struct {
	previous *fields.PodCluster
	current  *fields.PodCluster
}

func (p podClusterChange) different() bool {
	return !p.previous.Equals(p.current)
}

// There may be multiple implementations of ConcreteSyncer that are interested
// in pod cluster updates, and wish to acquire a pod cluster lock to guarantee
// exclusive right to sync an update. ConcreteSyncerType is used to namespace a
// lock by implementation type so that two different concrete syncer
// implementations may sync the same pod cluster at the same time
type ConcreteSyncerType string

func (t ConcreteSyncerType) String() string { return string(t) }

type ConcreteSyncer interface {
	// SyncCluster implements a concrete synchronization of the pod cluster to
	// some real world implementation of a load balancer, credential management
	// system, or service discovery implementation.
	//
	// When a ConcreteSyncer is passed to WatchAndSync, SyncCluster is called
	// every time the set of labeled pods change, the pod cluster's metadata
	// changes, or when the long-lived watch on the pod cluster store returns.
	// This function is expected to be idempotent.
	//
	// SyncCluster will be called for every pod cluster present in the store.
	// If this function returns an error, SyncCluster will be called again later
	// with the same cluster and pods, assuming that no changes occur in the intervening
	// period.
	//
	// ConcreteSyncers will be called concurrently and must operate safely.
	SyncCluster(pc *fields.PodCluster, pods []labels.Labeled) error

	// DeleteCluster is called when a pod cluster is observed to have been removed
	// from the store. DeleteCluster can be invoked in two circumstances: first,
	// if the cluster was present in a first watch, then absent in a subsequent watch,
	// then it is assumed it was deleted from the pcstore. Second, if the call
	// to GetInitialClusters() returns a pod cluster ID that is not present in the very
	// first watch result, DeleteCluster will be invoked with that pod cluster.
	//
	// If the passed ID is used to retrieve the pod cluster via store.Get(), it will
	// return ErrNoPodCluster. Clients should track any relevant metadata to the pod
	// cluster ID in the status store or in vendor-specific code.
	DeleteCluster(pc fields.ID) error

	// GetInitialClusters is called at the beginning of the WatchAndSync
	// routine. See DeleteCluster() for an explanation of how its results are
	// used. If the function results in an error, the WatchAndSync function will
	// terminate immediately, forwarding the error.
	GetInitialClusters() ([]fields.ID, error)

	// Used to derive the syncer type from a syncer instance for things
	// like namespacing of metrics
	Type() ConcreteSyncerType
}

// WatchAndSync registers a ConcreteSyncer which will have its
// functions invoked on certain pod cluster changes. See the
// ConcreteSyncer interface for details on how to use this function
func (s *ConsulStore) WatchAndSync(syncer ConcreteSyncer, quit <-chan struct{}) error {
	watchedRes := s.Watch(quit)

	clusterUpdaters := map[fields.ID]chan podClusterChange{}
	defer func() {
		for _, handler := range clusterUpdaters {
			close(handler)
		}
	}()

	// populate the initial clusters, if any provided
	prevResults, err := s.getInitialClusters(syncer)
	if err != nil {
		return err
	}

	// Typical exp decay sample
	histogram := metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))
	if s.metricsRegistry != nil {
		_ = s.metricsRegistry.Register(fmt.Sprintf("%s_pc_processing_time", syncer.Type()), histogram)
	}

	for {
		select {
		case curResults := <-watchedRes:
			if curResults.Err != nil {
				s.logger.WithError(curResults.Err).Errorln("Could not sync pod clusters")
				continue
			}

			if len(curResults.Clusters) == 0 {
				s.logger.Warnln("could not sync pod clusters because watch returned 0 pod clusters")
				continue
			}

			// zip up the previous and current results, act based on the difference.
			zipped := s.zipResults(curResults, prevResults)
			for id, change := range zipped {
				updater, ok := clusterUpdaters[id]
				if !ok {
					clusterUpdaters[id] = make(chan podClusterChange)
					go s.handlePCUpdates(syncer, clusterUpdaters[id], histogram)
					updater = clusterUpdaters[id]
				}
				// only notify about a change if the new cluster does not match the old one
				if change.different() {
					select {
					case updater <- change:
						if change.previous != nil && change.current == nil {
							close(clusterUpdaters[id])
							delete(clusterUpdaters, id)
						}
					case <-quit:
						return nil
					}
				} else {
					select {
					case <-quit:
						return nil
					default:
					}
				}
			}
			prevResults = curResults
		case <-quit:
			return nil
		}
	}
}

func (s *ConsulStore) getInitialClusters(syncer ConcreteSyncer) (WatchedPodClusters, error) {
	var prevResults WatchedPodClusters

	initial, err := syncer.GetInitialClusters()
	if err != nil {
		s.logger.Errorf("Error retrieving initial clusters: %v", err)
		return prevResults, err
	}

	for _, id := range initial {
		// We only populate the existing cluster with the ID. It's a little strange
		// but this means that there will definitely be a "change" for the cluster
		// when the watch is started, resulting in the label watch being started
		// as well
		prevResults.Clusters = append(prevResults.Clusters, &fields.PodCluster{
			ID: id,
		})
	}
	return prevResults, nil
}

// zipResults takes two sets of watched pod clusters and joins them such that they
// are paired together in a map of pc ID -> change objects. Each change will be sent
// to the respective sync channels of each pod cluster later on.
func (s *ConsulStore) zipResults(current, previous WatchedPodClusters) map[fields.ID]podClusterChange {
	allPrevious := make(map[fields.ID]*fields.PodCluster)
	for _, prev := range previous.Clusters {
		allPrevious[prev.ID] = prev
	}
	ret := map[fields.ID]podClusterChange{}
	for _, cur := range current.Clusters {
		prev, ok := allPrevious[cur.ID]
		ret[cur.ID] = podClusterChange{
			previous: prev,
			current:  cur,
		}
		if ok {
			delete(allPrevious, cur.ID)
		}
	}
	for _, prev := range allPrevious {
		ret[prev.ID] = podClusterChange{
			previous: prev,
		}
	}
	return ret
}

// try forever to match the expectations as defined in the provided change channel.
// If a change fails to take, this function will retry that change forever until
// it works as expected or a newer change appears on the channel. This routine also
// executes and monitors the label watch for the pod's label selector.
func (s *ConsulStore) handlePCUpdates(concrete ConcreteSyncer, changes chan podClusterChange, histogram metrics.Histogram) {
	var change podClusterChange
	podWatch := make(chan []labels.Labeled)
	watching := false
	podWatchQuit := make(chan struct{})
	defer func() {
		close(podWatchQuit)
	}()

	var ok bool
	var pcChangePending bool = false
	var prevLabeledPods []labels.Labeled
	var startTime time.Time

	for {
		select {
		case labeledPods := <-podWatch:
			if !pcChangePending {
				// This means that there was no change to the pod
				// cluster itself, just the nodes. Track the time we
				// noticed that so we can measure the time it takes
				// to sync the pod cluster
				startTime = time.Now()
			}

			if pcChangePending || !labeledEqual(labeledPods, prevLabeledPods) {
				s.logger.Debugf("Calling SyncCluster with %v / %v", change.current, labeledPods)
				err := concrete.SyncCluster(change.current, labeledPods)
				if err != nil {
					s.logger.WithError(err).Errorf("Failed to SyncCluster on %v / %v", change.current, labeledPods)
				} else {
					pcChangePending = false
					prevLabeledPods = labeledPods
				}

				timeElapsed := time.Now().Sub(startTime)
				histogram.Update(timeElapsed.Nanoseconds())
				s.logger.Debugf("Sync cluster took %s", timeElapsed.String())
			}
		case change, ok = <-changes:
			pcChangePending = true

			// we want to measure how long it takes from noticing a
			// pod cluster change to the sync completing
			startTime = time.Now()
			if !ok {
				s.logger.Debugln("Closing pc update channel")
				return // we're closed for business
			}

			var err error
			if !watching && change.current != nil {
				// Start watching for changes of pod membership because we haven't yet
				s.logger.WithFields(logrus.Fields{
					"pc_id":    change.current.ID,
					"selector": change.current.PodSelector.String(),
				}).Debugf("Starting pod selector watch for %v", change.current.ID)
				podWatch, err = s.watcher.WatchMatches(change.current.PodSelector, labels.POD, s.labelAggregationRate, podWatchQuit)
				if err != nil {
					s.logger.WithError(err).Errorln("Unable to start pod selector watch for %v", change.current.ID)
				} else {
					watching = true
				}
			} else if change.current == nil && change.previous != nil {
				// if no current cluster exists, but there is a previous cluster,
				// it means we need to destroy this concrete cluster
				s.logger.WithField("pc_id", change.previous.ID).Infof("Calling DeleteCluster with %v", change.previous)
				err := concrete.DeleteCluster(change.previous.ID)
				if err != nil {
					s.logger.Errorf("Deletion of cluster failed! %v", err)
				}
				return
			} else if change.current != nil && change.previous != nil {
				// if there's a current and a previous pod cluster, update concrete cluster metadata and
				// refresh the pod selector watch if it changed
				if change.current.PodSelector.String() != change.previous.PodSelector.String() {
					close(podWatchQuit)
					podWatchQuit = make(chan struct{})
					s.logger.WithFields(logrus.Fields{
						"pc_id":        change.current.ID,
						"old_selector": change.previous.PodSelector.String(),
						"new_selector": change.current.PodSelector.String(),
					}).Debugf("Altering pod selector for %v", change.current.ID)
					podWatch, err = s.watcher.WatchMatches(change.current.PodSelector, labels.POD, s.labelAggregationRate, podWatchQuit)
					if err != nil {
						// TODO: retry this. Today it's not an issue because the applicator we're using doesn't actually error
						s.logger.WithError(err).Errorln("Unable to alter pod selector watch for %v", change.current.ID)
					}
				}
			}
		}
	}
}

func labeledEqual(left, right []labels.Labeled) bool {
	leftSet, rightSet := sets.NewString(), sets.NewString()
	for _, l := range left {
		leftSet.Insert(l.ID)
	}
	for _, r := range right {
		rightSet.Insert(r.ID)
	}
	return leftSet.Equal(rightSet)
}

func (s *ConsulStore) LockForSync(id fields.ID, syncerType ConcreteSyncerType, session Session) (consul.Unlocker, error) {
	return session.Lock(pcSyncLockPath(id, syncerType))
}

func kvpsToPC(pairs api.KVPairs) ([]fields.PodCluster, error) {
	ret := make([]fields.PodCluster, 0, len(pairs))
	for _, kvp := range pairs {
		var pc fields.PodCluster
		var err error
		if pc, err = kvpToPC(kvp); err != nil {
			return nil, err
		}
		ret = append(ret, pc)
	}
	return ret, nil
}

func kvpToPC(pair *api.KVPair) (fields.PodCluster, error) {
	pc := fields.PodCluster{}
	err := json.Unmarshal(pair.Value, &pc)
	if err != nil {
		return pc, util.Errorf("Could not unmarshal pod cluster ('%s') as json: %s", string(pair.Value), err)
	}

	return pc, nil
}
