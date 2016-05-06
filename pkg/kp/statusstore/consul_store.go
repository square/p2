package statusstore

import (
	"fmt"
	"path"
	"strings"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
)

type consulKV interface {
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	Put(pair *api.KVPair, opts *api.WriteOptions) (*api.WriteMeta, error)
	Delete(key string, opts *api.WriteOptions) (*api.WriteMeta, error)
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
}

type consulStore struct {
	kv consulKV
}

var _ Store = &consulStore{}

func NewConsul(client *api.Client) Store {
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
	key := namespacedResourcePath(t, id, namespace)
	pair := &api.KVPair{
		Key:   key,
		Value: status.Bytes(),
	}
	_, err := s.kv.Put(pair, nil)
	if err != nil {
		return consulutil.NewKVError("put", key, err)
	}

	return nil
}

func (s *consulStore) GetStatus(t ResourceType, id ResourceID, namespace Namespace) (Status, error) {
	key := namespacedResourcePath(t, id, namespace)
	pair, _, err := s.kv.Get(key, nil)
	if err != nil {
		return nil, consulutil.NewKVError("get", key, err)
	}

	if pair == nil {
		return nil, NoStatusError{key}
	}

	return pair.Value, nil
}

func (s *consulStore) DeleteStatus(t ResourceType, id ResourceID, namespace Namespace) error {
	key := namespacedResourcePath(t, id, namespace)
	_, err := s.kv.Delete(key, nil)
	if err != nil {
		return consulutil.NewKVError("delete", key, err)
	}

	return nil
}

func (s *consulStore) GetAllStatusForResource(t ResourceType, id ResourceID) (map[Namespace]Status, error) {
	ret := make(map[Namespace]Status)
	prefix := resourcePath(t, id)
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
	ret := make(map[ResourceID]map[Namespace]Status)
	prefix := resourceTypePath(t)
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

func resourceTypePath(t ResourceType) string {
	return path.Join(statusTree, t.String())
}

func resourcePath(t ResourceType, id ResourceID) string {
	return path.Join(resourceTypePath(t), id.String())
}

func namespacedResourcePath(t ResourceType, id ResourceID, namespace Namespace) string {
	return path.Join(resourcePath(t, id), namespace.String())
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
