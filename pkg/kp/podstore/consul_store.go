package podstore

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

const PodTree = "pods"

// The structure of values written to the /pods tree.
type Pod struct {
	Manifest manifest.Manifest
	Node     types.NodeName
}

var _ json.Marshaler = Pod{}
var _ json.Unmarshaler = &Pod{}

func (p Pod) MarshalJSON() ([]byte, error) {
	rawPod, err := p.ToRaw()
	if err != nil {
		return nil, err
	}

	return json.Marshal(rawPod)
}

func (p Pod) ToRaw() (RawPod, error) {
	var manifest []byte
	var err error
	if p.Manifest != nil {
		manifest, err = p.Manifest.Marshal()
		if err != nil {
			return RawPod{}, err
		}
	}

	return RawPod{
		Manifest: string(manifest),
		Node:     p.Node,
	}, nil
}

func (p *Pod) UnmarshalJSON(b []byte) error {
	var rawPod RawPod
	if err := json.Unmarshal(b, &rawPod); err != nil {
		return err
	}

	m, err := manifest.FromBytes([]byte(rawPod.Manifest))
	if err != nil {
		return err
	}

	p.Manifest = m
	p.Node = rawPod.Node
	return nil
}

// Defines the JSON structure written to the /pods tree. The Pod type
// can't be used because manifest.Manifest doesn't marshal cleanly
type RawPod struct {
	Manifest string         `json:"manifest"`
	Node     types.NodeName `json:"node"`
}

// The structure of index values written to the /intent
// tree
type PodIndex struct {
	// The uuid of the pod. This can be used to retrieve the full pod from
	// the /pods tree
	PodKey types.PodUniqueKey `json:"pod_key"`
}

// Subset of api.KV{} functionality so we can mock it out for tests.
type KV interface {
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	Put(pair *api.KVPair, w *api.WriteOptions) (*api.WriteMeta, error)
}

var _ KV = &api.KV{}

type consulStore struct {
	consulKV KV
}

func NewConsul(consulKV KV) Store {
	return &consulStore{
		consulKV: consulKV,
	}
}

func (c *consulStore) Schedule(manifest manifest.Manifest, node types.NodeName) (key types.PodUniqueKey, err error) {
	manifestBytes, err := manifest.Marshal()
	if err != nil {
		return types.PodUniqueKey{}, err
	}

	podKey := types.NewPodUUID()

	podPath := computePodPath(podKey)
	indexPath := computeIndexPath(podKey, node)

	// Write the Pod to /pods/<key>
	pod := RawPod{
		Manifest: string(manifestBytes),
		Node:     node,
	}

	podBytes, err := json.Marshal(pod)
	if err != nil {
		return types.PodUniqueKey{}, err
	}

	pair := &api.KVPair{
		Key:   podPath,
		Value: podBytes,
	}

	_, err = c.consulKV.Put(pair, nil)
	if err != nil {
		return types.PodUniqueKey{}, consulutil.NewKVError("put", podPath, err)
	}

	// Now, write the secondary index to /intent/<node>/<key>
	index := PodIndex{
		PodKey: podKey,
	}

	// NOTE: errors might happen after this point which means we've written
	// a pod to /pods and we haven't written the corresponding index. In
	// those cases, we do a single attempt to delete the main pod key. In
	// the event of a Consul outage it's likely that the cleanup will fail,
	// so there will be a pod with no secondary index. For that purpose, a
	// sweeper process is planned to remove pods for which there is no index.
	// TODO: use a transaction when we can rely upon consul 0.7
	defer func() {
		if err != nil {
			_, _ = c.consulKV.Delete(podPath, nil)
		}
	}()

	indexBytes, err := json.Marshal(index)
	if err != nil {
		return types.PodUniqueKey{}, util.Errorf("Could not marshal index as json: %s", err)
	}

	indexPair := &api.KVPair{
		Key:   indexPath,
		Value: indexBytes,
	}
	_, err = c.consulKV.Put(indexPair, nil)
	if err != nil {
		return types.PodUniqueKey{}, consulutil.NewKVError("put", indexPath, err)
	}

	return podKey, nil
}

func (c *consulStore) Unschedule(podKey types.PodUniqueKey) error {
	if !podKey.IsUUID {
		return util.Errorf("Pod store can only delete pods with uuid keys (key was '%s')", podKey.ID)
	}

	// Read the pod so we know which node the secondary index will have
	pod, err := c.ReadPod(podKey)
	if err != nil {
		return err
	}

	podPath := computePodPath(podKey)
	indexPath := computeIndexPath(podKey, pod.Node)

	// Due to lack of transactionality in deleting both keys, the below code has the
	// following steps:
	// 1) attempt to delete the main pod key. If it fails, return an error without
	// deleting the index. The caller can retry the deletion if it matters
	// 2) attempt to delete the index. If this fails, return a special error type
	// so errors know they shouldn't retry the deletion. A "sweeper" process is
	// planned to remove hanging indices which will clean up the problem eventually.
	// TODO: use a transaction when we can rely upon consul 0.7

	_, err = c.consulKV.Delete(podPath, nil)
	if err != nil {
		return consulutil.NewKVError("delete", podPath, err)
	}

	_, err = c.consulKV.Delete(indexPath, nil)
	if err != nil {
		return IndexDeletionFailure{
			path: indexPath,
			err:  consulutil.NewKVError("delete", indexPath, err),
		}
	}

	return nil
}

// Custom error type to be returned when a pod is removed but there is a
// failure removing the secondary index, so callers know not to retry the full
// deletion but errors can be reported.
type IndexDeletionFailure struct {
	path string
	err  error
}

func (u IndexDeletionFailure) Error() string {
	return fmt.Sprintf("pod index '%s' could not be deleted: %s", u.path, u.err)
}

func IsIndexDeletionFailure(err error) bool {
	_, ok := err.(IndexDeletionFailure)
	return ok
}

type NoPod struct {
	key types.PodUniqueKey
}

func (n NoPod) Error() string {
	return fmt.Sprintf("Pod '%s' does not exist", n.key.ID)
}

func NoPodError(key types.PodUniqueKey) NoPod {
	return NoPod{
		key: key,
	}
}

func IsNoPod(err error) bool {
	_, ok := err.(NoPod)
	return ok
}

func (c *consulStore) ReadPod(podKey types.PodUniqueKey) (Pod, error) {
	if !podKey.IsUUID {
		return Pod{}, util.Errorf("Pod store can only read pods with uuid keys (key was '%s')", podKey.ID)
	}

	podPath := computePodPath(podKey)

	pair, _, err := c.consulKV.Get(podPath, nil)
	if err != nil {
		return Pod{}, consulutil.NewKVError("get", podPath, err)
	}

	if pair == nil {
		return Pod{}, NoPodError(podKey)
	}

	var pod Pod
	err = json.Unmarshal(pair.Value, &pod)
	if err != nil {
		return Pod{}, util.Errorf("Could not unmarshal pod '%s' as json: %s", podKey.ID, err)
	}

	return pod, nil
}

func (c *consulStore) ReadPodFromIndex(index PodIndex) (Pod, error) {
	return c.ReadPod(index.PodKey)
}

// Given a pod unique key and a node, compute the path to which the main pod
// should be written as well as the secondary index
func computePodPath(key types.PodUniqueKey) string {
	return path.Join(PodTree, key.ID)
}

func computeIndexPath(key types.PodUniqueKey, node types.NodeName) string {
	return path.Join("intent", node.String(), key.ID)
}
