package dsstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
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

func NewConsul(client *api.Client, retries int, logger *logging.Logger) Store {
	return &consulStore{
		retries:    retries,
		applicator: labels.NewConsulApplicator(client, retries),
		kv:         client.KV(),
		logger:     *logger,
	}
}

func (s *consulStore) Create(
	manifest pods.Manifest,
	minHealth int,
	name fields.ClusterName,
	nodeSelector klabels.Selector,
	podID types.PodID,
) (fields.DaemonSet, error) {
	if err := checkManifestPodID(podID, manifest); err != nil {
		return fields.DaemonSet{}, err
	}

	ds, err := s.innerCreate(manifest, minHealth, name, nodeSelector, podID)
	// TODO: measure whether retries are is important in practice
	for i := 0; i < s.retries; i++ {
		if _, ok := err.(CASError); ok {
			ds, err = s.innerCreate(manifest, minHealth, name, nodeSelector, podID)
		} else {
			break
		}
	}
	if err != nil {
		return fields.DaemonSet{}, err
	}
	return ds, nil
}

// these parts of Create may require a retry
func (s *consulStore) innerCreate(
	manifest pods.Manifest,
	minHealth int,
	name fields.ClusterName,
	nodeSelector klabels.Selector,
	podID types.PodID,
) (fields.DaemonSet, error) {
	id := fields.ID(uuid.New())
	dsPath, err := s.dsPath(id)
	if err != nil {
		return fields.DaemonSet{}, err
	}
	ds := fields.DaemonSet{
		ID:           id,
		Disabled:     false,
		Manifest:     manifest,
		MinHealth:    minHealth,
		Name:         name,
		NodeSelector: nodeSelector,
		PodID:        podID,
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
		return err
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
		return fields.DaemonSet{}, metadata, err
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
		return fields.DaemonSet{}, metadata, err
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
		return fields.DaemonSet{}, err
	}

	ds, err = mutator(ds)
	if err != nil {
		return fields.DaemonSet{}, err
	}
	if ds.ID != id {
		// If the user wants a new uuid, they should delete it and create it
		return fields.DaemonSet{},
			util.Errorf("Explicitly changing daemon set ID is not permitted: Wanted '%s' got '%s'", id, ds.ID)
	}
	if err := checkManifestPodID(ds.PodID, ds.Manifest); err != nil {
		return fields.DaemonSet{}, err
	}

	rawDS, err := json.Marshal(ds)
	if err != nil {
		return fields.DaemonSet{}, util.Errorf("Could not marshal DS as json: %s", err)
	}

	dsPath, err := s.dsPath(id)
	if err != nil {
		return fields.DaemonSet{}, err
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

func checkManifestPodID(dsPodID types.PodID, manifest pods.Manifest) error {
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

func kvpToDS(kvp *api.KVPair) (fields.DaemonSet, error) {
	ds := fields.DaemonSet{}
	// Unmarshals kvp.Value into ds using overloaded UnmarshalJSON
	err := json.Unmarshal(kvp.Value, &ds)
	if err != nil {
		return ds, util.Errorf("Could not unmarshal DS ('%s') as json: %s", string(kvp.Value), err)
	}

	return ds, nil
}

func kvpsToDSs(l api.KVPairs) ([]fields.DaemonSet, error) {
	ret := make([]fields.DaemonSet, 0, len(l))
	for _, kvp := range l {
		ds, err := kvpToDS(kvp)
		if err != nil {
			return nil, err
		}
		ret = append(ret, ds)
	}
	return ret, nil
}
