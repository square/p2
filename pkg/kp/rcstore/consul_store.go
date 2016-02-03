package rcstore

import (
	"encoding/json"
	"fmt"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/github.com/pborman/uuid"
	klabels "github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/util"
)

const (
	// This label is applied to an RC, to identify the ID of its pod manifest.
	PodIDLabel = "pod_id"
)

type kvPair struct {
	key   string
	value []byte
}

type consulKV interface {
	Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	DeleteCAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	Acquire(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
}

type consulStore struct {
	applicator labels.Applicator
	kv         consulKV
	retries    int
}

// TODO: combine with similar CASError type in pkg/labels
type CASError string

func (e CASError) Error() string {
	return fmt.Sprintf("Could not check-and-set key %q", string(e))
}

var _ Store = &consulStore{}

func NewConsul(client *api.Client, retries int) *consulStore {
	return &consulStore{
		retries:    retries,
		applicator: labels.NewConsulApplicator(client, retries),
		kv:         client.KV(),
	}
}

func (s *consulStore) Create(manifest pods.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (fields.RC, error) {
	rc, err := s.innerCreate(manifest, nodeSelector, podLabels)
	for i := 0; i < s.retries; i++ {
		if _, ok := err.(CASError); ok {
			rc, err = s.innerCreate(manifest, nodeSelector, podLabels)
		} else {
			break
		}
	}
	if err != nil {
		return fields.RC{}, err
	}

	// labels do not need to be retried, consul applicator does that itself
	err = s.forEachLabel(rc, func(id, k, v string) error {
		return s.applicator.SetLabel(labels.RC, rc.ID.String(), k, v)
	})
	if err != nil {
		return fields.RC{}, err
	}

	return rc, nil
}

// these parts of Create may require a retry
func (s *consulStore) innerCreate(manifest pods.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (fields.RC, error) {
	id := fields.ID(uuid.New())
	rcp := kp.RCPath(id.String())
	rc := fields.RC{
		ID:              id,
		Manifest:        manifest,
		NodeSelector:    nodeSelector,
		PodLabels:       podLabels,
		ReplicasDesired: 0,
		Disabled:        false,
	}

	jsonRC, err := json.Marshal(rc)
	if err != nil {
		return fields.RC{}, err
	}
	success, _, err := s.kv.CAS(&api.KVPair{
		Key:   rcp,
		Value: jsonRC,
		// the chance of the UUID already existing is vanishingly small, but
		// technically not impossible, so we should use the CAS index to guard
		// against duplicate UUIDs
		ModifyIndex: 0,
	}, nil)

	if err != nil {
		return fields.RC{}, consulutil.NewKVError("cas", rcp, err)
	}
	if !success {
		return fields.RC{}, CASError(rcp)
	}
	return rc, nil
}

func (s *consulStore) Get(id fields.ID) (fields.RC, error) {
	kvp, _, err := s.kv.Get(kp.RCPath(id.String()), nil)
	if err != nil {
		return fields.RC{}, err
	}
	if kvp == nil {
		// ID didn't exist
		return fields.RC{}, nil
	}
	return s.kvpToRC(kvp)
}

func (s *consulStore) List() ([]fields.RC, error) {
	listed, _, err := s.kv.List(kp.RC_TREE+"/", nil)
	if err != nil {
		return nil, err
	}
	return s.kvpsToRCs(listed)
}

func (s *consulStore) WatchNew(quit <-chan struct{}) (<-chan []fields.RC, <-chan error) {
	outCh := make(chan []fields.RC)
	errCh := make(chan error)
	inCh := make(chan api.KVPairs)

	go consulutil.WatchPrefix(kp.RC_TREE+"/", s.kv, inCh, quit, errCh)

	go func() {
		defer close(outCh)
		defer close(errCh)

		for listed := range inCh {
			out, err := s.kvpsToRCs(listed)
			if err != nil {
				select {
				case errCh <- err:
				case <-quit:
				}
			} else {
				select {
				case outCh <- out:
				case <-quit:
				}
			}
		}
	}()

	return outCh, errCh
}

func (s *consulStore) kvpToRC(kvp *api.KVPair) (fields.RC, error) {
	rc := fields.RC{}
	err := json.Unmarshal(kvp.Value, &rc)
	return rc, err
}

func (s *consulStore) kvpsToRCs(l api.KVPairs) ([]fields.RC, error) {
	ret := make([]fields.RC, 0, len(l))
	for _, kvp := range l {
		rc, err := s.kvpToRC(kvp)
		if err != nil {
			return nil, err
		}
		ret = append(ret, rc)
	}
	return ret, nil
}

func (s *consulStore) Disable(id fields.ID) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		rc.Disabled = true
		return rc, nil
	})
}

func (s *consulStore) Enable(id fields.ID) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		rc.Disabled = false
		return rc, nil
	})
}

func (s *consulStore) SetDesiredReplicas(id fields.ID, n int) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		rc.ReplicasDesired = n
		return rc, nil
	})
}

func (s *consulStore) AddDesiredReplicas(id fields.ID, n int) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		rc.ReplicasDesired += n
		if rc.ReplicasDesired < 0 {
			rc.ReplicasDesired = 0
		}
		return rc, nil
	})
}

func (s *consulStore) Delete(id fields.ID, force bool) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		if !force && rc.ReplicasDesired != 0 {
			return fields.RC{}, fmt.Errorf("replication controller %s has %d desired replicas (must reduce to 0 before deleting)", rc.ID, rc.ReplicasDesired)
		}
		return fields.RC{}, nil
	})
}

// TODO: this function is almost a verbatim copy of pkg/labels retryMutate, can
// we find some way to combine them?
func (s *consulStore) retryMutate(id fields.ID, mutator func(fields.RC) (fields.RC, error)) error {
	err := s.mutateRc(id, mutator)
	for i := 0; i < s.retries; i++ {
		if _, ok := err.(CASError); ok {
			err = s.mutateRc(id, mutator)
		} else {
			break
		}
	}
	return err
}

// performs a safe (ie check-and-set) mutation of the rc with the given id,
// using the given function
// if the mutator returns an error, it will be propagated out
// if the returned RC has id="", then it will be deleted
func (s *consulStore) mutateRc(id fields.ID, mutator func(fields.RC) (fields.RC, error)) error {
	rcp := kp.RCPath(id.String())
	kvp, meta, err := s.kv.Get(rcp, nil)
	if err != nil {
		return err
	}
	if kvp == nil {
		return fmt.Errorf("replication controller %s does not exist", id)
	}

	rc, err := s.kvpToRC(kvp)
	if err != nil {
		return err
	}
	newKVP := &api.KVPair{
		Key:         rcp,
		ModifyIndex: meta.LastIndex,
	}

	var success bool
	newRC, err := mutator(rc)
	if err != nil {
		return err
	}
	if newRC.ID.String() == "" {
		// TODO: If this fails, then we have some dangling labels.
		// Perhaps they can be cleaned up later.
		// note that if the CAS fails afterwards, we will have still deleted
		// the labels, and then we will retry, which will involve deleting them
		// again
		// really the only way to solve this is a transaction
		err = s.applicator.RemoveAllLabels(labels.RC, id.String())
		if err != nil {
			return err
		}

		success, _, err = s.kv.DeleteCAS(newKVP, nil)
	} else {
		b, err := json.Marshal(newRC)
		if err != nil {
			return err
		}
		newKVP.Value = b
		success, _, err = s.kv.CAS(newKVP, nil)
	}

	if err != nil {
		return err
	}
	if !success {
		return CASError(rcp)
	}
	return nil
}

func (s *consulStore) Watch(rc *fields.RC, quit <-chan struct{}) (<-chan struct{}, <-chan error) {
	updated := make(chan struct{})
	errors := make(chan error)
	input := make(chan *api.KVPair)

	go consulutil.WatchSingle(kp.RCPath(rc.ID.String()), s.kv, input, quit, errors)

	go func() {
		defer close(updated)
		defer close(errors)

		for kvp := range input {
			if kvp == nil {
				// seems this RC got deleted from under us. quitting
				// would be unexpected, so we'll just wait for it to
				// reappear in consul
				continue
			}
			newRC, err := s.kvpToRC(kvp)
			if err != nil {
				select {
				case errors <- err:
				case <-quit:
				}
			} else {
				*rc = newRC
				select {
				case updated <- struct{}{}:
				case <-quit:
				}
			}
		}
	}()

	return updated, errors
}

// forEachLabel Attempts to apply the supplied function to labels of the replication controller.
// If forEachLabel encounters any error applying the function, it returns that error immediately.
// The function is not further applied to subsequent labels on an error.
func (s *consulStore) forEachLabel(rc fields.RC, f func(id, k, v string) error) error {
	id := rc.ID.String()
	// As of this writing the only label we want is the pod ID.
	// There may be more in the future.

	if rc.Manifest == nil {
		return util.Errorf("RC manifest is nil, cannot compute its ID")
	}
	return f(id, PodIDLabel, string(rc.Manifest.ID()))
}
