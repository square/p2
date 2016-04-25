package pcstore

import (
	"encoding/json"
	"path"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/github.com/pborman/uuid"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

type consulKV interface {
	Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

type consulStore struct {
	kv         consulKV
	applicator labels.Applicator

	logger logging.Logger
}

var ErrNoPodCluster error

var _ Store = &consulStore{}

// NOTE: The "retries" concept is mimicking what is built in rcstore.
// TODO: explore transactionality of operations and returning errors instead of
// using retries
func NewConsul(client api.Client, retries int, logger *logging.Logger) Store {
	return &consulStore{
		applicator: labels.NewConsulApplicator(&client, retries),
		kv:         client.KV(),
		logger:     *logger,
	}
}

func (s *consulStore) Create(
	podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName,
	podSelector klabels.Selector,
	annotations fields.Annotations,
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
		return existing[0], util.Errorf("Pod cluster already exists for %v", pcCreateLockPath(podID, availabilityZone, clusterName))
	}

	pc := fields.PodCluster{
		ID:               id,
		PodID:            podID,
		AvailabilityZone: availabilityZone,
		Name:             clusterName,
		PodSelector:      podSelector,
		Annotations:      annotations,
	}

	key, err := s.pcPath(id)
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
		_ = s.Delete(pc.ID)
		return fields.PodCluster{}, err
	}

	return pc, nil
}

func (s *consulStore) setLabelsForPC(pc fields.PodCluster) error {
	pcLabels := klabels.Set{}
	pcLabels[fields.PodIDLabel] = pc.PodID.String()
	pcLabels[fields.AvailabilityZoneLabel] = pc.AvailabilityZone.String()
	pcLabels[fields.ClusterNameLabel] = pc.Name.String()

	return s.applicator.SetLabels(labels.PC, pc.ID.String(), pcLabels)
}

func (s *consulStore) Get(id fields.ID) (fields.PodCluster, error) {
	key, err := s.pcPath(id)
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

func (s *consulStore) Delete(id fields.ID) error {
	key, err := s.pcPath(id)
	if err != nil {
		return err
	}

	_, err = s.kv.Delete(key, nil)
	if err != nil {
		return consulutil.NewKVError("delete", key, err)
	}

	return s.applicator.RemoveAllLabels(labels.PC, id.String())
}

func (s *consulStore) pcPath(pcID fields.ID) (string, error) {
	if pcID == "" {
		return "", util.Errorf("Path requested for empty pod cluster ID")
	}

	return path.Join(podClusterTree, pcID.String()), nil
}

func (s *consulStore) lockForCreation(podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName,
	session Session) (consulutil.Unlocker, error) {
	return session.Lock(pcCreateLockPath(podID, availabilityZone, clusterName))
}

func pcCreateLockPath(podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName) string {
	return path.Join(consulutil.LOCK_TREE, podID.String(), availabilityZone.String(), clusterName.String())
}

func (s *consulStore) FindWhereLabeled(podID types.PodID,
	availabilityZone fields.AvailabilityZone,
	clusterName fields.ClusterName) ([]fields.PodCluster, error) {

	sel := klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{availabilityZone.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{clusterName.String()})

	podClusters, err := s.applicator.GetMatches(sel, labels.PC)
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
func (s *consulStore) Watch(quit <-chan struct{}) <-chan WatchedPodClusters {
	inCh := make(chan api.KVPairs)
	outCh := make(chan WatchedPodClusters)
	errChan := make(chan error, 1)

	go consulutil.WatchPrefix(podClusterTree, s.kv, inCh, quit, errChan)

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
				clusters.Clusters = append(clusters.Clusters, &pc)
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
func (s *consulStore) WatchPodCluster(id fields.ID, quit <-chan struct{}) <-chan WatchedPodCluster {
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
					case outCh <- WatchedPodCluster{PodCluster: nil, Err: ErrNoPodCluster}:
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
