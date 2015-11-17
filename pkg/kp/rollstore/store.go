package rollstore

import (
	"encoding/json"
	"fmt"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	rcf "github.com/square/p2/pkg/rc/fields"
	rollf "github.com/square/p2/pkg/roll/fields"
)

// Store persists Updates into Consul. Updates are uniquely identified by their
// new RC's ID.
type Store interface {
	// retrieve this Update
	Get(rcf.ID) (rollf.Update, error)
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

type consulStore struct {
	kv *api.KV
}

var _ Store = consulStore{}

func NewConsul(c *api.Client) Store {
	return consulStore{c.KV()}
}

func (s consulStore) Get(id rcf.ID) (rollf.Update, error) {
	key := kp.RollPath(id.String())
	kvp, _, err := s.kv.Get(key, nil)
	if err != nil {
		return rollf.Update{}, consulutil.NewKVError("get", key, err)
	}

	var ret rollf.Update
	err = json.Unmarshal(kvp.Value, &ret)
	if err != nil {
		return rollf.Update{}, err
	}
	return ret, nil
}

func (s consulStore) Put(u rollf.Update) error {
	b, err := json.Marshal(u)
	if err != nil {
		return err
	}

	key := kp.RollPath(u.NewRC.String())
	success, _, err := s.kv.CAS(&api.KVPair{
		Key:   kp.RollPath(u.NewRC.String()),
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
	key := kp.RollPath(id.String())
	_, err := s.kv.Delete(key, nil)
	if err != nil {
		return consulutil.NewKVError("delete", key, err)
	}
	return nil
}

func (s consulStore) Lock(id rcf.ID, session string) (bool, error) {
	key := kp.LockPath(kp.RollPath(id.String()))
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

	go consulutil.WatchPrefix(kp.ROLL_TREE, s.kv, inCh, quit, errCh)

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
