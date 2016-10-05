package statusstore

import (
	"fmt"
	"path"
	"strings"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

type consulKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	Put(pair *api.KVPair, opts *api.WriteOptions) (*api.WriteMeta, error)
	Delete(key string, opts *api.WriteOptions) (*api.WriteMeta, error)
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
}

type consulStore struct {
	kv consulKV
}

var _ Store = &consulStore{}

func NewConsul(client consulutil.ConsulClient) Store {
	return &consulStore{
		kv: client.KV(),
	}
}

type NoStatusError struct {
	Key string
}

func (n NoStatusError) Error() string {
	return fmt.Sprintf("No status record found at %s", n.Key)
}

func IsNoStatus(err error) bool {
	_, ok := err.(NoStatusError)
	return ok
}

func (s *consulStore) SetStatus(t ResourceType, id ResourceID, namespace Namespace, status Status) error {
	key, err := namespacedResourcePath(t, id, namespace)
	if err != nil {
		return err
	}

	pair := &api.KVPair{
		Key:   key,
		Value: status.Bytes(),
	}
	_, err = s.kv.Put(pair, nil)
	if err != nil {
		return consulutil.NewKVError("put", key, err)
	}

	return nil
}

// Custom error type for when CAS was performed with a stale index
type staleIndex struct {
	key   string
	index uint64
}

func NewStaleIndex(key string, index uint64) staleIndex {
	return staleIndex{
		key:   key,
		index: index,
	}
}

func (s staleIndex) Error() string {
	return fmt.Sprintf("CAS failed for '%s', index of '%d' was stale", s.key, s.index)
}

func IsStaleIndex(err error) bool {
	_, ok := err.(staleIndex)
	return ok
}

func (s *consulStore) CASStatus(t ResourceType, id ResourceID, namespace Namespace, status Status, modifyIndex uint64) error {
	key, err := namespacedResourcePath(t, id, namespace)
	if err != nil {
		return err
	}

	pair := &api.KVPair{
		Key:         key,
		Value:       status.Bytes(),
		ModifyIndex: modifyIndex,
	}
	success, _, err := s.kv.CAS(pair, nil)
	if err != nil {
		return consulutil.NewKVError("cas", key, err)
	}

	if !success {
		return NewStaleIndex(key, modifyIndex)
	}

	return nil
}

func (s *consulStore) GetStatus(t ResourceType, id ResourceID, namespace Namespace) (Status, *api.QueryMeta, error) {
	key, err := namespacedResourcePath(t, id, namespace)
	if err != nil {
		return nil, nil, err
	}

	pair, queryMeta, err := s.kv.Get(key, nil)
	if err != nil {
		return nil, nil, consulutil.NewKVError("get", key, err)
	}

	if pair == nil {
		return nil, queryMeta, NoStatusError{key}
	}

	return pair.Value, queryMeta, nil
}

func (s *consulStore) DeleteStatus(t ResourceType, id ResourceID, namespace Namespace) error {
	key, err := namespacedResourcePath(t, id, namespace)
	if err != nil {
		return err
	}

	_, err = s.kv.Delete(key, nil)
	if err != nil {
		return consulutil.NewKVError("delete", key, err)
	}

	return nil
}

func (s *consulStore) GetAllStatusForResource(t ResourceType, id ResourceID) (map[Namespace]Status, error) {
	prefix, err := resourcePath(t, id)
	if err != nil {
		return nil, err
	}

	ret := make(map[Namespace]Status)
	pairs, _, err := s.kv.List(prefix, nil)
	if err != nil {
		return nil, consulutil.NewKVError("list", prefix, err)
	}

	for _, pair := range pairs {
		key := pair.Key
		status := pair.Value

		_, _, namespace, err := keyParts(key)
		if err != nil {
			return nil, err
		}

		ret[namespace] = status
	}

	return ret, nil
}

func (s *consulStore) GetAllStatusForResourceType(t ResourceType) (map[ResourceID]map[Namespace]Status, error) {
	prefix, err := resourceTypePath(t)
	if err != nil {
		return nil, err
	}

	ret := make(map[ResourceID]map[Namespace]Status)
	pairs, _, err := s.kv.List(prefix, nil)
	if err != nil {
		return nil, consulutil.NewKVError("list", prefix, err)
	}

	for _, pair := range pairs {
		key := pair.Key
		status := pair.Value

		_, id, namespace, err := keyParts(key)
		if err != nil {
			return nil, err
		}

		_, ok := ret[id]
		if !ok {
			ret[id] = make(map[Namespace]Status)
		}

		ret[id][namespace] = status
	}

	return ret, nil
}

func resourceTypePath(t ResourceType) (string, error) {
	if t == "" {
		return "", util.Errorf("Resource type cannot be blank")
	}
	return path.Join(statusTree, t.String()), nil
}

func resourcePath(t ResourceType, id ResourceID) (string, error) {
	if id == "" {
		return "", util.Errorf("resource ID cannot be blank")
	}

	typePath, err := resourceTypePath(t)
	if err != nil {
		return "", err
	}

	return path.Join(typePath, id.String()), nil
}

func namespacedResourcePath(t ResourceType, id ResourceID, namespace Namespace) (string, error) {
	if namespace == "" {
		return "", util.Errorf("Blank namespace not allowed")
	}

	rPath, err := resourcePath(t, id)
	if err != nil {
		return "", err
	}

	return path.Join(rPath, namespace.String()), nil
}

// in consul, the key for a status looks like
// '/status/pod_clusters/<pod_cluster_id>/<namespace>'.
func keyParts(key string) (ResourceType, ResourceID, Namespace, error) {
	parts := strings.Split(key, "/")
	if len(parts) != 4 {
		return "", "", "", util.Errorf("Malformed status key: %s", key)
	}

	// sanity check that we parsed correctly
	if parts[0] != statusTree {
		return "", "", "", util.Errorf("Malformed status key (did not start with 'status'): %s", key)
	}

	return ResourceType(parts[1]), ResourceID(parts[2]), Namespace(parts[3]), nil
}
