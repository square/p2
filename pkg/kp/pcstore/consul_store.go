package pcstore

import (
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	"github.com/rcrowley/go-metrics"
	klabels "k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/util/sets"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/util"
)

type consulKV interface {
	Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

type consulStore struct {
	kv      consulKV
	labeler pcLabeler
	watcher pcWatcher

	logger logging.Logger

	metricsRegistry MetricsRegistry
}

type pcLabeler interface {
	SetLabel(labelType labels.Type, id, name, value string) error
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
	SetLabels(labelType labels.Type, id string, labels map[string]string) error
	RemoveAllLabels(labelType labels.Type, id string) error
	GetMatches(selector klabels.Selector, labelType labels.Type, cachedMatch bool) ([]labels.Labeled, error)
}

type pcWatcher interface {
	WatchMatches(selector klabels.Selector, labelType labels.Type, quitCh <-chan struct{}) chan []labels.Labeled
}

var _ Store = &consulStore{}

func NewConsul(client consulutil.ConsulClient, labeler pcLabeler, watcher pcWatcher, logger *logging.Logger) Store {
	return &consulStore{
		kv:      client.KV(),
		logger:  *logger,
		labeler: labeler,
		watcher: watcher,
	}
}

func (s *consulStore) SetMetricsRegistry(reg MetricsRegistry) {
	s.metricsRegistry = reg
}

func (s *consulStore) Create(
	podID store.PodID,
	availabilityZone store.AvailabilityZone,
	clusterName store.ClusterName,
	podSelector klabels.Selector,
	annotations store.Annotations,
	session Session,
) (store.PodCluster, error) {
	id := store.PodClusterID(uuid.New())

	unlocker, err := s.lockForCreation(podID, availabilityZone, clusterName, session)
	if err != nil {
		return store.PodCluster{}, err
	}
	defer unlocker.Unlock()

	existing, err := s.FindWhereLabeled(podID, availabilityZone, clusterName)
	if err != nil {
		return store.PodCluster{}, util.Errorf("Couldn't determine if pod cluster exists already: %v", err)
	}
	if len(existing) > 0 {
		return existing[0], PodClusterAlreadyExists
	}

	pc := store.PodCluster{
		ID:               id,
		PodID:            podID,
		AvailabilityZone: availabilityZone,
		Name:             clusterName,
		PodSelector:      podSelector,
		Annotations:      annotations,
	}

	key, err := pcPath(id)
	if err != nil {
		return store.PodCluster{}, err
	}

	jsonPC, err := json.Marshal(pc)
	if err != nil {
		// Probably the annotations don't marshal to JSON
		return store.PodCluster{}, util.Errorf("Unable to marshal pod cluster as JSON: %s", err)
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
		return store.PodCluster{}, consulutil.NewKVError("cas", key, err)
	}

	if !success {
		return store.PodCluster{}, util.Errorf("Could not set pod cluster at path '%s'", key)
	}

	err = s.setLabelsForPC(pc)
	if err != nil {
		// TODO: what if this delete fails?
		deleteErr := s.Delete(pc.ID)
		if deleteErr != nil {
			err = util.Errorf("%s\n%s", err, deleteErr)
		}
		return store.PodCluster{}, err
	}

	return pc, nil
}

func (s *consulStore) setLabelsForPC(pc store.PodCluster) error {
	pcLabels := klabels.Set{}
	pcLabels[store.PodIDLabel] = pc.PodID.String()
	pcLabels[store.AvailabilityZoneLabel] = pc.AvailabilityZone.String()
	pcLabels[store.ClusterNameLabel] = pc.Name.String()

	return s.labeler.SetLabels(labels.PC, pc.ID.String(), pcLabels)
}

func (s *consulStore) Get(id store.PodClusterID) (store.PodCluster, error) {
	key, err := pcPath(id)
	if err != nil {
		return store.PodCluster{}, err
	}

	kvp, _, err := s.kv.Get(key, nil)
	if err != nil {
		return store.PodCluster{}, consulutil.NewKVError("get", key, err)
	}
	if kvp == nil {
		// ID didn't exist
		return store.PodCluster{}, NoPodCluster
	}

	return kvpToPC(kvp)
}

func (s *consulStore) Delete(id store.PodClusterID) error {
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

func (s *consulStore) List() ([]store.PodCluster, error) {
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
func (s *consulStore) MutatePC(
	id store.PodClusterID,
	mutator func(store.PodCluster) (store.PodCluster, error),
) (store.PodCluster, error) {
	pcp, err := pcPath(id)
	if err != nil {
		return store.PodCluster{}, err
	}

	kvp, meta, err := s.kv.Get(pcp, nil)
	if err != nil {
		return store.PodCluster{}, consulutil.NewKVError("get", pcp, err)
	}

	if kvp == nil {
		return store.PodCluster{}, NoPodCluster
	}

	pc, err := kvpToPC(kvp)
	if err != nil {
		return store.PodCluster{}, err
	}

	pc, err = mutator(pc)
	if err != nil {
		return store.PodCluster{}, err
	}

	jsonPC, err := json.Marshal(pc)
	if err != nil {
		// Probably the annotations don't marshal to JSON
		return store.PodCluster{}, util.Errorf("Unable to marshal pod cluster as JSON: %s", err)
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
		return store.PodCluster{}, consulutil.NewKVError("cas", pcp, err)
	}

	if !success {
		return store.PodCluster{}, util.Errorf("Could not set pod cluster at path '%s'", pcp)
	}

	err = s.setLabelsForPC(pc)
	if err != nil {
		// TODO: what if this delete fails?
		deleteErr := s.Delete(pc.ID)
		if deleteErr != nil {
			err = util.Errorf("%s\n%s", err, deleteErr)
		}
		return store.PodCluster{}, err
	}

	return pc, nil
}

func pcPath(pcID store.PodClusterID) (string, error) {
	if pcID == "" {
		return "", util.Errorf("Path requested for empty pod cluster ID")
	}

	return path.Join(podClusterTree, pcID.String()), nil
}

func (s *consulStore) lockForCreation(podID store.PodID,
	availabilityZone store.AvailabilityZone,
	clusterName store.ClusterName,
	session Session) (consulutil.Unlocker, error) {
	return session.Lock(pcCreateLockPath(podID, availabilityZone, clusterName))
}

func pcCreateLockPath(podID store.PodID,
	availabilityZone store.AvailabilityZone,
	clusterName store.ClusterName) string {
	return path.Join(consulutil.LOCK_TREE, podID.String(), availabilityZone.String(), clusterName.String())
}

func pcSyncLockPath(id store.PodClusterID, syncerType ConcreteSyncerType) string {
	return path.Join(consulutil.LOCK_TREE, podClusterTree, id.String(), syncerType.String())
}

func (s *consulStore) FindWhereLabeled(podID store.PodID,
	availabilityZone store.AvailabilityZone,
	clusterName store.ClusterName) ([]store.PodCluster, error) {

	sel := klabels.Everything().
		Add(store.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(store.AvailabilityZoneLabel, klabels.EqualsOperator, []string{availabilityZone.String()}).
		Add(store.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	podClusters, err := s.labeler.GetMatches(sel, labels.PC, false)
	if err != nil {
		return nil, err
	}
	ret := make([]store.PodCluster, len(podClusters))
	for i, pc := range podClusters {
		ret[i], err = s.Get(store.PodClusterID(pc.ID))
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
func (s *consulStore) Watch(quit <-chan struct{}) <-chan WatchedPodClusters {
	inCh := make(chan api.KVPairs)
	outCh := make(chan WatchedPodClusters)
	errChan := make(chan error, 1)

	go consulutil.WatchPrefix(podClusterTree, s.kv, inCh, quit, errChan, 5*time.Second)

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
func (s *consulStore) WatchPodCluster(id store.PodClusterID, quit <-chan struct{}) <-chan WatchedPodCluster {
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
	previous *store.PodCluster
	current  *store.PodCluster
}

func (p podClusterChange) different() bool {
	return !p.previous.Equals(p.current)
}

func (s *consulStore) WatchAndSync(syncer ConcreteSyncer, quit <-chan struct{}) error {
	watchedRes := s.Watch(quit)

	clusterUpdaters := map[store.PodClusterID]chan podClusterChange{}
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

func (s *consulStore) getInitialClusters(syncer ConcreteSyncer) (WatchedPodClusters, error) {
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
		prevResults.Clusters = append(prevResults.Clusters, &store.PodCluster{
			ID: id,
		})
	}
	return prevResults, nil
}

// zipResults takes two sets of watched pod clusters and joins them such that they
// are paired together in a map of pc ID -> change objects. Each change will be sent
// to the respective sync channels of each pod cluster later on.
func (s *consulStore) zipResults(current, previous WatchedPodClusters) map[store.PodClusterID]podClusterChange {
	allPrevious := make(map[store.PodClusterID]*store.PodCluster)
	for _, prev := range previous.Clusters {
		allPrevious[prev.ID] = prev
	}
	ret := map[store.PodClusterID]podClusterChange{}
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
func (s *consulStore) handlePCUpdates(concrete ConcreteSyncer, changes chan podClusterChange, histogram metrics.Histogram) {
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

			if !watching && change.current != nil {
				// Start watching for changes of pod membership because we haven't yet
				s.logger.WithFields(logrus.Fields{
					"pc_id":    change.current.ID,
					"selector": change.current.PodSelector.String(),
				}).Debugf("Starting pod selector watch for %v", change.current.ID)
				podWatch = s.watcher.WatchMatches(change.current.PodSelector, labels.POD, podWatchQuit)
				watching = true
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
					podWatch = s.watcher.WatchMatches(change.current.PodSelector, labels.POD, podWatchQuit)
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

func (s *consulStore) LockForSync(id store.PodClusterID, syncerType ConcreteSyncerType, session Session) (consulutil.Unlocker, error) {
	return session.Lock(pcSyncLockPath(id, syncerType))
}

func kvpsToPC(pairs api.KVPairs) ([]store.PodCluster, error) {
	ret := make([]store.PodCluster, 0, len(pairs))
	for _, kvp := range pairs {
		var pc store.PodCluster
		var err error
		if pc, err = kvpToPC(kvp); err != nil {
			return nil, err
		}
		ret = append(ret, pc)
	}
	return ret, nil
}

func kvpToPC(pair *api.KVPair) (store.PodCluster, error) {
	pc := store.PodCluster{}
	err := json.Unmarshal(pair.Value, &pc)
	if err != nil {
		return pc, util.Errorf("Could not unmarshal pod cluster ('%s') as json: %s", string(pair.Value), err)
	}

	return pc, nil
}
