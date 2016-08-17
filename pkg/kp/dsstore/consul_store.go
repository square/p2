package dsstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

type consulKV interface {
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
}

type consulStore struct {
	applicator labels.Applicator
	kv         consulKV
	logger     logging.Logger
	retries    int
}

// TODO: combine with similar CASError type in pkg/labels
type CASError string

func (e CASError) Error() string {
	return fmt.Sprintf("Could not check-and-set key %q", string(e))
}

// Make sure functions declared in the Store interface have the proper contract
var _ Store = &consulStore{}

func NewConsul(client consulutil.ConsulClient, retries int, logger *logging.Logger) Store {
	return &consulStore{
		retries:    retries,
		applicator: labels.NewConsulApplicator(client, retries),
		kv:         client.KV(),
		logger:     *logger,
	}
}

func (s *consulStore) Create(
	manifest manifest.Manifest,
	minHealth int,
	name fields.ClusterName,
	nodeSelector klabels.Selector,
	podID types.PodID,
	timeout time.Duration,
) (fields.DaemonSet, error) {
	if err := checkManifestPodID(podID, manifest); err != nil {
		return fields.DaemonSet{}, util.Errorf("Error verifying manifest pod id: %v", err)
	}

	ds, err := s.innerCreate(manifest, minHealth, name, nodeSelector, podID, timeout)
	// TODO: measure whether retries are is important in practice
	for i := 0; i < s.retries; i++ {
		if _, ok := err.(CASError); ok {
			ds, err = s.innerCreate(manifest, minHealth, name, nodeSelector, podID, timeout)
		} else {
			break
		}
	}
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("Error creating daemon set: %v", err)
	}
	return ds, nil
}

// these parts of Create may require a retry
func (s *consulStore) innerCreate(
	manifest manifest.Manifest,
	minHealth int,
	name fields.ClusterName,
	nodeSelector klabels.Selector,
	podID types.PodID,
	timeout time.Duration,
) (fields.DaemonSet, error) {
	id := fields.ID(uuid.New())
	dsPath, err := s.dsPath(id)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("Error getting daemon set path: %v", err)
	}
	ds := fields.DaemonSet{
		ID:           id,
		Disabled:     false,
		Manifest:     manifest,
		MinHealth:    minHealth,
		Name:         name,
		NodeSelector: nodeSelector,
		PodID:        podID,
		Timeout:      timeout,
	}
	// Marshals ds into []bytes using overloaded MarshalJSON
	rawDS, err := json.Marshal(ds)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("Could not marshal DS as json: %s", err)
	}

	success, _, err := s.kv.CAS(&api.KVPair{
		Key:         dsPath,
		Value:       rawDS,
		ModifyIndex: 0,
	}, nil)
	if err != nil {
		return fields.DaemonSet{}, consulutil.NewKVError("cas", dsPath, err)
	}
	if !success {
		return fields.DaemonSet{}, CASError(dsPath)
	}
	return ds, nil
}

func (s *consulStore) Delete(id fields.ID) error {
	dsPath, err := s.dsPath(id)
	if err != nil {
		return util.Errorf("Error getting daemon set path: %v", err)
	}

	_, err = s.kv.Delete(dsPath, nil)
	if err != nil {
		return consulutil.NewKVError("delete", dsPath, err)
	}
	return nil
}

func (s *consulStore) Get(id fields.ID) (fields.DaemonSet, *api.QueryMeta, error) {
	var metadata *api.QueryMeta
	dsPath, err := s.dsPath(id)
	if err != nil {
		return fields.DaemonSet{}, metadata, util.Errorf("Error getting daemon set path: %v", err)
	}

	kvp, metadata, err := s.kv.Get(dsPath, nil)
	if err != nil {
		return fields.DaemonSet{}, metadata, consulutil.NewKVError("get", dsPath, err)
	}
	if metadata == nil {
		// no metadata returned
		return fields.DaemonSet{}, metadata, errors.New("No metadata found")
	}
	if kvp == nil {
		// ID didn't exist
		return fields.DaemonSet{}, metadata, NoDaemonSet
	}

	ds, err := kvpToDS(kvp)
	if err != nil {
		return fields.DaemonSet{}, metadata, util.Errorf("Error translating kvp to daemon set: %v", err)
	}
	return ds, metadata, nil
}

func (s *consulStore) List() ([]fields.DaemonSet, error) {
	listed, _, err := s.kv.List(dsTree+"/", nil)
	if err != nil {
		return nil, consulutil.NewKVError("list", dsTree+"/", err)
	}
	return kvpsToDSs(listed)
}

func (s *consulStore) MutateDS(
	id fields.ID,
	mutator func(fields.DaemonSet) (fields.DaemonSet, error),
) (fields.DaemonSet, error) {
	ds, metadata, err := s.Get(id)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("Error getting daemon set: %v", err)
	}

	ds, err = mutator(ds)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("Error mutating daemon set: %v", err)
	}
	if ds.ID != id {
		// If the user wants a new uuid, they should delete it and create it
		return fields.DaemonSet{},
			util.Errorf("Explicitly changing daemon set ID is not permitted: Wanted '%s' got '%s'", id, ds.ID)
	}
	if err := checkManifestPodID(ds.PodID, ds.Manifest); err != nil {
		return fields.DaemonSet{}, util.Errorf("Error verifying manifest pod id: %v", err)
	}

	rawDS, err := json.Marshal(ds)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("Could not marshal DS as json: %s", err)
	}

	dsPath, err := s.dsPath(id)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("Error getting daemon set path: %v", err)
	}

	success, _, err := s.kv.CAS(&api.KVPair{
		Key:         dsPath,
		Value:       rawDS,
		ModifyIndex: metadata.LastIndex,
	}, nil)
	if err != nil {
		return fields.DaemonSet{}, consulutil.NewKVError("cas", dsPath, err)
	}
	if !success {
		return fields.DaemonSet{}, CASError(dsPath)
	}
	return ds, nil
}

func (s *consulStore) Disable(id fields.ID) (fields.DaemonSet, error) {
	s.logger.Infof("Attempting to disable '%s' in store now", id)

	mutator := func(dsToUpdate fields.DaemonSet) (fields.DaemonSet, error) {
		dsToUpdate.Disabled = true
		return dsToUpdate, nil
	}
	newDS, err := s.MutateDS(id, mutator)

	if err != nil {
		s.logger.Errorf("Error occured when trying to disable daemon set in store")
		return fields.DaemonSet{}, err
	}

	s.logger.Infof("Daemon set '%s' was successfully disabled in store", id)
	return newDS, nil
}

// Watch watches dsTree for changes and returns a blocking channel
// where the client can read a WatchedDaemonSets object which contain
// changed daemon sets
func (s *consulStore) Watch(quitCh <-chan struct{}) <-chan WatchedDaemonSets {
	outCh := make(chan WatchedDaemonSets)
	errCh := make(chan error, 1)

	// Watch for changes in the dsTree and deletedDSTree
	inCh := consulutil.WatchDiff(dsTree, s.kv, quitCh, errCh)

	go func() {
		defer close(outCh)
		defer close(errCh)

		for {
			var kvps *consulutil.WatchedChanges

			// Make a list of changed daemon sets and sends it on outCh
			outgoingDSs := WatchedDaemonSets{}

			// Populate kvps by reading inCh and checking for errors
			select {
			case <-quitCh:
				return
			case err := <-errCh:
				outgoingDSs.Err = util.Errorf("WatchDiff returned error: %v, recovered", err)
				select {
				case <-quitCh:
					return
				case outCh <- outgoingDSs:
					continue
				}
			case kvps = <-inCh:
				if kvps == nil {
					s.logger.Errorf("Very odd, kvps should never be null, recovered.")
					continue
				}
			}

			createdDSs, err := kvpsToDSs(kvps.Created)
			if err != nil {
				outgoingDSs.Err = util.Errorf("Watch create error: %s; ", err)
			}
			updatedDSs, err := kvpsToDSs(kvps.Updated)
			if err != nil {
				outgoingDSs.Err = util.Errorf("%sWatch update error: %s; ", outgoingDSs.Err, err)
			}
			deletedDSs, err := kvpsToDSs(kvps.Deleted)
			if err != nil {
				outgoingDSs.Err = util.Errorf("%sWatch delete error: %s; ", outgoingDSs.Err, err)
			}

			if outgoingDSs.Err != nil {
				// Block until the receiver quits or reads the outCh's previous output
				select {
				case <-quitCh:
					return
				case outCh <- outgoingDSs:
					continue
				}
			}

			for _, ds := range createdDSs {
				// &ds is a pointer to an for statement's iteration variable
				// and should not be used outside of this block
				dsCopy := ds
				outgoingDSs.Created = append(outgoingDSs.Created, &dsCopy)
			}
			for _, ds := range updatedDSs {
				dsCopy := ds
				outgoingDSs.Updated = append(outgoingDSs.Updated, &dsCopy)
			}
			for _, ds := range deletedDSs {
				dsCopy := ds
				outgoingDSs.Deleted = append(outgoingDSs.Deleted, &dsCopy)
			}

			// Block until the receiver quits or reads the outCh's previous output
			select {
			case <-quitCh:
				return
			case outCh <- outgoingDSs:
			}
		}
	}()

	return outCh
}

// WatchAll watches dsTree for all the daemon sets and returns a blocking
// channel where the client can read a WatchedDaemonSetsList object which
// contain all of the daemon sets currently on the tree
func (s *consulStore) WatchAll(quitCh <-chan struct{}) <-chan WatchedDaemonSetList {
	inCh := make(chan api.KVPairs)
	outCh := make(chan WatchedDaemonSetList)
	errCh := make(chan error, 1)

	// Watch for changes in the dsTree and deletedDSTree
	go consulutil.WatchPrefix(dsTree, s.kv, inCh, quitCh, errCh, time.Duration(250*time.Millisecond))

	go func() {
		defer close(outCh)
		defer close(errCh)

		var kvp api.KVPairs
		for {
			select {
			case <-quitCh:
				return
			case err := <-errCh:
				s.logger.WithError(err).Errorf("WatchPrefix returned error, recovered.")
			case kvp = <-inCh:
				if kvp == nil {
					// nothing to do
					continue
				}
			}

			daemonSets := WatchedDaemonSetList{}

			dsList, err := kvpsToDSs(kvp)
			if err != nil {
				daemonSets.Err = err
				select {
				case <-quitCh:
					return
				case outCh <- daemonSets:
					continue
				}
			}

			daemonSets.DaemonSets = dsList

			select {
			case outCh <- daemonSets:
			case <-quitCh:
				return
			}
		}
	}()

	return outCh
}

func checkManifestPodID(dsPodID types.PodID, manifest manifest.Manifest) error {
	if dsPodID == "" {
		return util.Errorf("Daemon set must have a pod id")
	}
	if manifest.ID() == "" {
		return util.Errorf("Daemon set manifest must have a pod id")
	}
	if dsPodID != manifest.ID() {
		return util.Errorf("Daemon set pod id must match manifest pod id. Wanted '%s', got '%s'", dsPodID, manifest.ID())
	}
	return nil
}

func (s *consulStore) dsPath(dsID fields.ID) (string, error) {
	if dsID == "" {
		return "", util.Errorf("Path requested for empty DS id")
	}
	return path.Join(dsTree, dsID.String()), nil
}

func (s *consulStore) dsLockPath(dsID fields.ID) (string, error) {
	dsPath, err := s.dsPath(dsID)
	if err != nil {
		return "", err
	}
	return path.Join(consulutil.LOCK_TREE, dsPath), nil
}

// Acquires a lock on the DS that should be used by DS farm goroutines, whose
// job it is to carry out the intent of the DS
func (s *consulStore) LockForOwnership(dsID fields.ID, session kp.Session) (consulutil.Unlocker, error) {
	lockPath, err := s.dsLockPath(dsID)
	if err != nil {
		return nil, err
	}
	return session.Lock(lockPath)
}

func kvpToDS(kvp *api.KVPair) (fields.DaemonSet, error) {
	ds := fields.DaemonSet{}
	// Unmarshals kvp.Value into ds using overloaded UnmarshalJSON
	err := json.Unmarshal(kvp.Value, &ds)
	if err != nil {
		return ds, util.Errorf("Could not unmarshal DS ('%s') as json: %s", string(kvp.Value), err)
	}
	if ds.Manifest == nil {
		return ds, util.Errorf("%s: DS has no manifest", kvp.Key)
	}

	return ds, nil
}

func kvpsToDSs(l api.KVPairs) ([]fields.DaemonSet, error) {
	ret := make([]fields.DaemonSet, 0, len(l))
	for _, kvp := range l {
		ds, err := kvpToDS(kvp)
		if err != nil {
			return nil, util.Errorf("Error translating kvp to daemon set: %v", err)
		}
		ret = append(ret, ds)
	}
	return ret, nil
}
