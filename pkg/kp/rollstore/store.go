package rollstore

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	rcf "github.com/square/p2/pkg/rc/fields"
	rollf "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/util"
)

const rollTree string = "rolls"

// Store persists Updates into Consul. Updates are uniquely identified by their
// new RC's ID.
type Store interface {
	// retrieve this Update
	Get(rcf.ID) (rollf.Update, error)
	// retrieve all updates
	List() ([]rollf.Update, error)
	// put this Update into the store. Updates are immutable - if another Update
	// exists with this newRC ID, an error is returned
	Put(rollf.Update) error
	// delete this Update from the store
	Delete(rcf.ID) error
	// take a lock on this ID. Before taking ownership of an Update, its new RC
	// ID, and old RC ID if any, should both be locked. If the error return is
	// nil, then the boolean indicates whether the lock was successfully taken.
	Lock(rcf.ID, string) (bool, error)
	// Watch for changes to the store and generate a list of Updates for each
	// change. This function does not block.
	Watch(<-chan struct{}) (<-chan []rollf.Update, <-chan error)
}

// Interface that allows us to inject a test implementation of the consul api
type KV interface {
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
}

var _ KV = &api.KV{}

type consulStore struct {
	kv KV
}

var _ Store = consulStore{}

func NewConsul(c *api.Client) Store {
	return consulStore{c.KV()}
}

func (s consulStore) Get(id rcf.ID) (rollf.Update, error) {
	key, err := RollPath(id)
	if err != nil {
		return rollf.Update{}, nil
	}

	kvp, _, err := s.kv.Get(key, nil)
	if err != nil {
		return rollf.Update{}, consulutil.NewKVError("get", key, err)
	}
	if kvp == nil {
		return rollf.Update{}, nil
	}

	return kvpToRU(kvp)
}

func (s consulStore) List() ([]rollf.Update, error) {
	listed, _, err := s.kv.List(rollTree+"/", nil)
	if err != nil {
		return nil, err
	}

	ret := make([]rollf.Update, 0, len(listed))
	for _, kvp := range listed {
		ru, err := kvpToRU(kvp)
		if err != nil {
			return nil, err
		}
		ret = append(ret, ru)
	}
	return ret, nil
}

func (s consulStore) Put(u rollf.Update) error {
	b, err := json.Marshal(u)
	if err != nil {
		return err
	}

	key, err := RollPath(u.NewRC)
	if err != nil {
		return err
	}

	success, _, err := s.kv.CAS(&api.KVPair{
		Key:   key,
		Value: b,
		// it must not already exist
		ModifyIndex: 0,
	}, nil)
	if err != nil {
		return consulutil.NewKVError("cas", key, err)
	}
	if !success {
		return fmt.Errorf("update with new RC ID %s already exists", u.NewRC)
	}
	return nil
}

func (s consulStore) Delete(id rcf.ID) error {
	key, err := RollPath(id)
	if err != nil {
		return err
	}

	_, err = s.kv.Delete(key, nil)
	if err != nil {
		return consulutil.NewKVError("delete", key, err)
	}
	return nil
}

func (s consulStore) Lock(id rcf.ID, session string) (bool, error) {
	key, err := RollLockPath(id)
	if err != nil {
		return false, err
	}

	success, _, err := s.kv.Acquire(&api.KVPair{
		Key:     key,
		Value:   []byte(session),
		Session: session,
	}, nil)
	if err != nil {
		return false, consulutil.NewKVError("acquire", key, err)
	}
	return success, nil
}

func (s consulStore) Watch(quit <-chan struct{}) (<-chan []rollf.Update, <-chan error) {
	outCh := make(chan []rollf.Update)
	errCh := make(chan error)
	inCh := make(chan api.KVPairs)

	go consulutil.WatchPrefix(rollTree+"/", s.kv, inCh, quit, errCh)

	go func() {
		defer close(outCh)
		defer close(errCh)

		for listed := range inCh {
			out := make([]rollf.Update, 0, len(listed))
			for _, kvp := range listed {
				var next rollf.Update
				if err := json.Unmarshal(kvp.Value, &next); err != nil {
					select {
					case errCh <- err:
					case <-quit:
						// stop processing this kvp list; inCh should be closed
						// in a moment
						break
					}
				} else {
					out = append(out, next)
				}
			}
			select {
			case outCh <- out:
			case <-quit:
			}
		}
	}()

	return outCh, errCh
}

func RollPath(rcId rcf.ID) (string, error) {
	if rcId == "" {
		return "", util.Errorf("rcId not specified when computing roll path")
	}
	return path.Join(rollTree, string(rcId)), nil
}

// Roll paths are computed using the id of the new replication controller
func RollLockPath(rcId rcf.ID) (string, error) {
	subRollPath, err := RollPath(rcId)
	if err != nil {
		return "", err
	}

	return path.Join(kp.LOCK_TREE, subRollPath), nil
}

func kvpToRU(kvp *api.KVPair) (rollf.Update, error) {
	ru := rollf.Update{}
	err := json.Unmarshal(kvp.Value, &ru)
	if err != nil {
		return ru, util.Errorf("Unable to unmarshal value as rolling update: %s", err)
	}
	return ru, nil
}
