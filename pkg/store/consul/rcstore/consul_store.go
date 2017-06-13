package rcstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/util"
)

const (
	// This label is applied to an RC, to identify the ID of its pod manifest.
	PodIDLabel = "pod_id"
	// This is called "update" for backwards compatibility reasons, it
	// should probably be named "mutate"
	mutationSuffix       = "update"
	updateCreationSuffix = "update_creation"
)

const rcTree string = "replication_controllers"

var NoReplicationController error = errors.New("No replication controller found")

func IsNotExist(err error) bool {
	return err == NoReplicationController
}

type LockType int

const (
	OwnershipLockType LockType = iota
	MutationLockType
	UpdateCreationLockType
	UnknownLockType
)

func (l LockType) String() string {
	switch l {
	case OwnershipLockType:
		return "ownership"
	case MutationLockType:
		return "mutation"
	case UpdateCreationLockType:
		return "update_creation"
	}
	return "unknown"
}

type kvPair struct {
	key   string
	value []byte
}

type consulKV interface {
	Get(key string, opts *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	Keys(prefix string, separator string, opts *api.QueryOptions) ([]string, *api.QueryMeta, error)
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	DeleteCAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error)
}

type ConsulStore struct {
	labeler RCLabeler
	kv      consulKV
	retries int
}

// TODO: combine with similar CASError type in pkg/labels
type CASError string

func (e CASError) Error() string {
	return fmt.Sprintf("Could not check-and-set key %q", string(e))
}

type RCLabeler interface {
	SetLabels(labelType labels.Type, id string, labels map[string]string) error
	RemoveAllLabels(labelType labels.Type, id string) error
	SetLabelsTxn(ctx context.Context, labelType labels.Type, id string, labels map[string]string) error
	RemoveAllLabelsTxn(ctx context.Context, labelType labels.Type, id string) error
}

func NewConsul(client consulutil.ConsulClient, labeler RCLabeler, retries int) *ConsulStore {
	return &ConsulStore{
		retries: retries,
		labeler: labeler,
		kv:      client.KV(),
	}
}

// Create creates a replication controller with the specified manifest and selectors.
// The node selector is used to determine what nodes the replication controller may schedule on.
// The pod label set is applied to every pod the replication controller schedules.
// The additionalLabels label set is applied to the RCs own labels
func (s *ConsulStore) Create(manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set, additionalLabels klabels.Set) (fields.RC, error) {
	rc, err := s.innerCreate(manifest, nodeSelector, podLabels)

	// TODO: measure whether retries are is important in practice
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

	// labels do not need to be retried, consul labeler does that itself
	labelsToSet := s.computeLabels(rc, additionalLabels)
	err = s.labeler.SetLabels(labels.RC, rc.ID.String(), labelsToSet)
	if err != nil {
		return fields.RC{}, err
	}

	return rc, nil
}

// TODO: replace Create() with this
func (s *ConsulStore) CreateTxn(
	ctx context.Context,
	manifest manifest.Manifest,
	nodeSelector klabels.Selector,
	podLabels klabels.Set,
	additionalLabels klabels.Set,
) (fields.RC, error) {
	rc, err := s.innerCreateTxn(ctx, manifest, nodeSelector, podLabels)
	if err != nil {
		return fields.RC{}, err
	}

	// labels do not need to be retried, consul labeler does that itself
	labelsToSet := s.computeLabels(rc, additionalLabels)
	err = s.labeler.SetLabelsTxn(ctx, labels.RC, rc.ID.String(), labelsToSet)
	if err != nil {
		return fields.RC{}, err
	}

	return rc, nil
}

// these parts of Create may require a retry
func (s *ConsulStore) innerCreate(manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (fields.RC, error) {
	id := fields.ID(uuid.New())
	rcp, err := s.rcPath(id)
	if err != nil {
		return fields.RC{}, err
	}

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
		return fields.RC{}, util.Errorf("Could not marshal RC as json: %s", err)
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

// TODO: replace innerCreate() with this function
func (s *ConsulStore) innerCreateTxn(ctx context.Context, manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (fields.RC, error) {
	id := fields.ID(uuid.New())
	rcp, err := s.rcPath(id)
	if err != nil {
		return fields.RC{}, err
	}

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
		return fields.RC{}, util.Errorf("Could not marshal RC as json: %s", err)
	}

	err = transaction.Add(ctx, api.KVTxnOp{
		Verb:  api.KVCAS,
		Key:   rcp,
		Value: jsonRC,
		// the chance of the UUID already existing is vanishingly small, but
		// technically not impossible, so we should use the CAS index to guard
		// against duplicate UUIDs
		Index: 0,
	})
	if err != nil {
		return fields.RC{}, err
	}

	return rc, nil
}

// Get retrieves a replication controller by its ID. Returns
// NoReplicationController if the fetch operation succeeds but no replication
// controller with that ID is found.
func (s *ConsulStore) Get(id fields.ID) (fields.RC, error) {
	rc, _, err := s.getWithIndex(id)
	return rc, err
}

func (s *ConsulStore) getWithIndex(id fields.ID) (fields.RC, uint64, error) {
	rcp, err := s.rcPath(id)
	if err != nil {
		return fields.RC{}, 0, err
	}

	kvp, _, err := s.kv.Get(rcp, nil)
	if err != nil {
		return fields.RC{}, 0, consulutil.NewKVError("get", rcp, err)
	}
	if kvp == nil {
		// ID didn't exist
		return fields.RC{}, 0, NoReplicationController
	}

	rc, err := kvpToRC(kvp)
	if err != nil {
		return fields.RC{}, kvp.ModifyIndex, err
	}

	return rc, kvp.ModifyIndex, nil
}

// List retrieves all of the replication controllers from the consul store.
func (s *ConsulStore) List() ([]fields.RC, error) {
	listed, _, err := s.kv.List(rcTree+"/", nil)
	if err != nil {
		return nil, consulutil.NewKVError("list", rcTree+"/", err)
	}
	return kvpsToRCs(listed)
}

// Watches the consul store for changes to the RC tree and attempts to return
// the full RC list on each update.
//
// Because processing the full list of RCs may take a large amount of time,
// particularly when there are 100s of RCs, WatchNew() takes care to drop
// writes to the output channel if they're being consumed too slowly.  It will
// block writing a value to the output channel until 1) it is read or 2) a new
// value comes in, in which case that value will be written instead
func (s *ConsulStore) WatchNew(quit <-chan struct{}) (<-chan []fields.RC, <-chan error) {
	inCh := make(chan api.KVPairs)

	outCh, errCh := publishLatestRCs(inCh, quit)
	go consulutil.WatchPrefix(rcTree+"/", s.kv, inCh, quit, errCh, 1*time.Second)

	return outCh, errCh
}

// Wraps an RC ID with lock information
// This structure is useful for low-bandwidth detection of new RCs, for use
// in the RC farm.
type RCLockResult struct {
	ID                      fields.ID
	LockedForOwnership      bool
	LockedForUpdateCreation bool
	LockedForMutation       bool
}

// WatchRCKeysWithLockInfo executes periodic consul watch queries for
// replication controller keys and passes the results on a channel along with
// information about which keys have locks on them. Only keys are returned as a
// bandwidth performance optimization. To observe changes to the fields of a
// replication controller, individual watches should be used. Lock info is
// returned so that it can be used to not try to lock RCs that are already
// locked. In other words, it's a QPS optimization over the naive solution of
// always attempting to lock every RC and relying on the consul server to
// decide whether the lock is owned.
//
// Since lock information is retrieved once per update to the RC list, it's
// possible that lock information will be out of date as the list is processed.
// However, a subsequent update will get the correct view of the world so the
// behavior should be correct
func (s *ConsulStore) WatchRCKeysWithLockInfo(quit <-chan struct{}, pauseTime time.Duration) (<-chan []RCLockResult, <-chan error) {
	combinedErrCh := make(chan error)

	keyCh := consulutil.WatchKeys(rcTree+"/", s.kv, quit, pauseTime)

	var combinedErrChWG sync.WaitGroup
	rcIDCh := make(chan []fields.ID)
	combinedErrChWG.Add(1)
	go func() {
		defer combinedErrChWG.Done()
		for watchedKeys := range keyCh {
			if watchedKeys.Err != nil {
				select {
				case <-quit:
					return
				case combinedErrCh <- watchedKeys.Err:
					continue
				}
			}

			rcIDs, err := keysToRCIDs(watchedKeys.Keys)
			if err != nil {
				select {
				case <-quit:
					return
				case combinedErrCh <- err:
					continue
				}
			}

			select {
			case <-quit:
				return
			case rcIDCh <- rcIDs:
			}
		}
	}()
	// Process RC updates and augment them with lock information
	outCh, lockInfoErrCh := s.publishLatestRCKeysWithLockInfo(rcIDCh, quit)

	// Fan-in the two error channels into one source
	combinedErrChWG.Add(1)
	go func() {
		defer combinedErrChWG.Done()
		var err error
		for {
			select {
			case err = <-lockInfoErrCh:
			case <-quit:
				return
			}

			select {
			case combinedErrCh <- err:
			case <-quit:
				return
			}
		}
	}()

	// goroutine that will close the combined error channel once all the
	// goroutines that publish to it have exited
	go func() {
		combinedErrChWG.Wait()
		close(combinedErrCh)
	}()
	return outCh, combinedErrCh
}

// Pulled out of WatchNew for testing purposes (faking watches on a mock KV is
// hard). It converts api.KVPairs values received on inCh to []fields.RC, and
// attempts to pass them on outCh. It will discard updates that are not read
// quickly enough (see WatchNew)
func publishLatestRCs(inCh <-chan api.KVPairs, quit <-chan struct{}) (<-chan []fields.RC, chan error) {
	outCh := make(chan []fields.RC)
	errCh := make(chan error)

	go func() {
		defer close(outCh)
		defer close(errCh)

		// Initialize one value off the inCh
		var listed api.KVPairs
		var ok bool
		select {
		case listed, ok = <-inCh:
			if !ok {
				// in channel closed
				return
			}
		case <-quit:
			return
		}
		needToWrite := true

		for {
			if !needToWrite {
				// We don't have a value to write yet, block for one
				select {
				case listed, ok = <-inCh:
					if !ok {
						// channel closed
						return
					}
					needToWrite = true
				case <-quit:
					return
				}
			}

			out, err := kvpsToRCs(listed)
			if err != nil {
				select {
				case errCh <- err:
					// The most recent update was corrupted,
					// let's not try to write it and block
					// for another.
					needToWrite = false
				case <-quit:
					return
				}
			} else {
				select {
				case outCh <- out:
					needToWrite = false
				case listed, ok = <-inCh:
					if !ok {
						// channel closed
						return
					}
					// We got a new update from inCh but the
					// consumer hasn't read our last one, that's
					// OK, throw out the unread data
					needToWrite = true
				case <-quit:
					return
				}
			}
		}
	}()

	return outCh, errCh
}

func (s *ConsulStore) publishLatestRCKeysWithLockInfo(inCh <-chan []fields.ID, quit <-chan struct{}) (chan []RCLockResult, chan error) {
	// Buffered to deal with slow consumers
	outCh := make(chan []RCLockResult, 1)
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		defer close(outCh)
		for {
			select {
			case <-quit:
				return
			case rcIDs, ok := <-inCh:
				if !ok {
					return
				}

				var res []RCLockResult
				rcMap := make(map[fields.ID]*RCLockResult)
				for _, rcID := range rcIDs {
					rcMap[rcID] = &RCLockResult{
						ID: rcID,
					}
				}

				// Grab all RC related locks and populate map
				// with the information
				lockPairs, _, err := s.kv.List(s.rcLockRoot(), nil)
				if err != nil {
					select {
					case errCh <- consulutil.NewKVError("list", s.rcLockRoot(), util.Errorf("Unable to retrieve rc lock info, reporting them all as not locked: %s", err)):
					case <-quit:
						return
					}
				}

				// This is safe even if we had an err above
				for _, pair := range lockPairs {
					rcID, lockType, err := s.lockTypeFromKey(pair.Key)
					if err != nil {
						select {
						case errCh <- err:
						case <-quit:
							return
						}
						continue
					}

					if _, ok := rcMap[rcID]; !ok {
						select {
						case errCh <- util.Errorf("Found lock for rc '%s' but that RC wasn't found", rcID):
						case <-quit:
							return
						}
						continue
					}

					switch lockType {
					case OwnershipLockType:
						rcMap[rcID].LockedForOwnership = true
					case MutationLockType:
						rcMap[rcID].LockedForMutation = true
					case UpdateCreationLockType:
						rcMap[rcID].LockedForUpdateCreation = true
					}
				}

				for _, rcLockResult := range rcMap {
					res = append(res, *rcLockResult)
				}

				// attempt to drain stale value from out channel
				select {
				case <-outCh:
				default:
				}

				select {
				case outCh <- res:
				case <-quit:
					return
				}
			}
		}
	}()
	return outCh, errCh
}

func kvpToRC(kvp *api.KVPair) (fields.RC, error) {
	rc := fields.RC{}
	err := json.Unmarshal(kvp.Value, &rc)
	if err != nil {
		return rc, util.Errorf("Could not unmarshal RC ('%s') as json: %s", string(kvp.Value), err)
	}
	if rc.Manifest == nil {
		return rc, util.Errorf("%s: RC has no manifest", kvp.Key)
	}

	return rc, nil
}

func kvpsToRCs(l api.KVPairs) ([]fields.RC, error) {
	ret := make([]fields.RC, 0, len(l))
	for _, kvp := range l {
		rc, err := kvpToRC(kvp)
		if err != nil {
			return nil, err
		}
		ret = append(ret, rc)
	}
	return ret, nil
}

// Disable sets the disabled flag on the given replication controller, instructing
// any running farm instances to cease handling it.
func (s *ConsulStore) Disable(id fields.ID) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		rc.Disabled = true
		return rc, nil
	})
}

// Enable unsets the disabled flag for the given RC, instructing any running
// farms to begin handling it.
func (s *ConsulStore) Enable(id fields.ID) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		rc.Disabled = false
		return rc, nil
	})
}

// SetDesiredReplicas updates the replica count for the RC with the
// given ID.
func (s *ConsulStore) SetDesiredReplicas(id fields.ID, n int) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		rc.ReplicasDesired = n
		return rc, nil
	})
}

// AddDesiredReplicas increments the replica count for the specified RC
// by n.
func (s *ConsulStore) AddDesiredReplicas(id fields.ID, n int) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		rc.ReplicasDesired += n
		if rc.ReplicasDesired < 0 {
			rc.ReplicasDesired = 0
		}
		return rc, nil
	})
}

// CASDesiredReplicas first checks that the desired replica count for
// the given RC is the given integer (returning an error if it is not),
// and if it is, sets it to the given integer.
func (s *ConsulStore) CASDesiredReplicas(id fields.ID, expected int, n int) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		if rc.ReplicasDesired != expected {
			return rc, fmt.Errorf("replication controller %s has %d desired replicas instead of %d, not setting to %d", rc.ID, rc.ReplicasDesired, expected, n)
		}
		rc.ReplicasDesired = n
		return rc, nil
	})
}

// Delete removes the RC with the given ID the targeted RC, returning an error
// if it does not exist.  Normally an RC can only be deleted if its desired
// replica count is zero; pass force=true to override this check.
func (s *ConsulStore) Delete(id fields.ID, force bool) error {
	return s.retryMutate(id, func(rc fields.RC) (fields.RC, error) {
		if !force && rc.ReplicasDesired != 0 {
			return fields.RC{}, fmt.Errorf("replication controller %s has %d desired replicas (must reduce to 0 before deleting)", rc.ID, rc.ReplicasDesired)
		}
		return fields.RC{}, nil
	})
}

// DeleteTxn adds a deletion operation to the passed context rather than
// immediately deleting ig
func (s *ConsulStore) DeleteTxn(ctx context.Context, id fields.ID, force bool) error {
	err := s.labeler.RemoveAllLabelsTxn(ctx, labels.RC, id.String())
	if err != nil {
		return err
	}

	keyPath, err := s.rcPath(id)
	if err != nil {
		return err
	}

	if force {
		return transaction.Add(ctx, api.KVTxnOp{
			Verb: api.KVDelete,
			Key:  keyPath,
		})
	}

	rc, index, err := s.getWithIndex(id)
	if err != nil {
		return util.Errorf("could not fetch RC %s to determine its replica count is 0: %s", id, err)
	}

	if rc.ReplicasDesired != 0 {
		return util.Errorf("cannot delete RC %s because its replica count is nonzero, was %d", id, rc.ReplicasDesired)
	}

	return transaction.Add(ctx, api.KVTxnOp{
		Verb:  api.KVDeleteCAS,
		Key:   keyPath,
		Index: index,
	})
}

// UpdateManifest will set the manifest on the RC at the given ID. Be careful with this function!
func (s *ConsulStore) UpdateManifest(id fields.ID, man manifest.Manifest) error {
	manifestUpdater := func(rc fields.RC) (fields.RC, error) {
		rc.Manifest = man
		return rc, nil
	}
	return s.retryMutate(id, manifestUpdater)
}

// TODO: this function is almost a verbatim copy of pkg/labels retryMutate, can
// we find some way to combine them?
func (s *ConsulStore) retryMutate(id fields.ID, mutator func(fields.RC) (fields.RC, error)) error {
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
func (s *ConsulStore) mutateRc(id fields.ID, mutator func(fields.RC) (fields.RC, error)) error {
	rcp, err := s.rcPath(id)
	if err != nil {
		return err
	}

	kvp, meta, err := s.kv.Get(rcp, nil)
	if err != nil {
		return consulutil.NewKVError("get", rcp, err)
	}

	if kvp == nil {
		return NoReplicationController
	}

	rc, err := kvpToRC(kvp)
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
		err = s.labeler.RemoveAllLabels(labels.RC, id.String())
		if err != nil {
			return err
		}

		success, _, err = s.kv.DeleteCAS(newKVP, nil)
		if err != nil {
			return consulutil.NewKVError("delete-cas", newKVP.Key, err)
		}
	} else {
		b, err := json.Marshal(newRC)
		if err != nil {
			return util.Errorf("Could not marshal RC as JSON: %s", err)
		}
		newKVP.Value = b
		success, _, err = s.kv.CAS(newKVP, nil)
		if err != nil {
			return consulutil.NewKVError("cas", newKVP.Key, err)
		}
	}

	if !success {
		return CASError(rcp)
	}
	return nil
}

// Watch watches for any changes to the replication controller `rc`.
// This returns two output channels.
// A `struct{}` is sent on the first output channel whenever a change has occurred.
// At that time, the replication controller will have been updated in place.
// Errors are sent on the second output channel.
// Send a value on `quitChannel` to stop watching.
// The two output channels will be closed in response.
// The passed mutex is used to synchronize access to the `rc` variable which is mutated
// by this function
func (s *ConsulStore) Watch(rc *fields.RC, mu *sync.Mutex, quit <-chan struct{}) (<-chan struct{}, <-chan error) {
	updated := make(chan struct{})

	rcp, err := s.rcPath(rc.ID)
	if err != nil {
		errors := make(chan error, 1)
		errors <- err
		close(errors)
		close(updated)
		return updated, errors
	}

	errors := make(chan error)
	input := make(chan *api.KVPair)
	go consulutil.WatchSingle(rcp, s.kv, input, quit, errors)

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
			newRC, err := kvpToRC(kvp)
			if err != nil {
				select {
				case errors <- err:
				case <-quit:
				}
			} else {
				mu.Lock()
				*rc = newRC
				mu.Unlock()
				select {
				case updated <- struct{}{}:
				case <-quit:
				}
			}
		}
	}()

	return updated, errors
}

func (s *ConsulStore) rcLockRoot() string {
	return path.Join(consulutil.LOCK_TREE, rcTree)
}

func (s *ConsulStore) rcPath(rcID fields.ID) (string, error) {
	if rcID == "" {
		return "", util.Errorf("Path requested for empty RC id")
	}

	return path.Join(rcTree, string(rcID)), nil
}

func (s *ConsulStore) rcLockPath(rcID fields.ID) (string, error) {
	rcPath, err := s.rcPath(rcID)
	if err != nil {
		return "", err
	}

	return path.Join(consulutil.LOCK_TREE, rcPath), nil
}

// The path for an ownership lock is simply the base path
func (s *ConsulStore) ownershipLockPath(rcID fields.ID) (string, error) {
	return s.rcLockPath(rcID)
}

// LockForOwnership qcquires a lock on the RC that should be used by RC farm
// goroutines, whose job it is to carry out the intent of the RC
func (s *ConsulStore) LockForOwnership(rcID fields.ID, session consul.Session) (consulutil.Unlocker, error) {
	lockPath, err := s.rcLockPath(rcID)
	if err != nil {
		return nil, err
	}

	return session.Lock(lockPath)
}

func (s *ConsulStore) mutationLockPath(rcID fields.ID) (string, error) {
	baseLockPath, err := s.rcLockPath(rcID)
	if err != nil {
		return "", err
	}

	return path.Join(baseLockPath, mutationSuffix), nil
}

// LockForMutation acquires a lock on the RC with the intent of mutating it.
// Must be held by goroutines in the rolling update farm as well as any other
// tool that may mutate an RC
func (s *ConsulStore) LockForMutation(rcID fields.ID, session consul.Session) (consulutil.Unlocker, error) {
	mutationLockPath, err := s.mutationLockPath(rcID)
	if err != nil {
		return nil, err
	}

	return session.Lock(mutationLockPath)
}

// UpdateCreationLockPath computes the consul key that should be locked by callers
// creating an RU that will operate on this RC. This function is exported so these
// callers can perform operations on the lock during a consul transaction
func (s *ConsulStore) UpdateCreationLockPath(rcID fields.ID) (string, error) {
	baseLockPath, err := s.rcLockPath(rcID)
	if err != nil {
		return "", err
	}

	return path.Join(baseLockPath, updateCreationSuffix), nil
}

// LockForUpdateCreation acquires a lock on the RC for ensuring that no two
// rolling updates are created that operate on the same replication
// controllers. A lock on both the intended "new" and "old" replication
// controllers should be held before the update is created.
func (s *ConsulStore) LockForUpdateCreation(rcID fields.ID, session consul.Session) (consulutil.Unlocker, error) {
	updateCreationLockPath, err := s.UpdateCreationLockPath(rcID)
	if err != nil {
		return nil, err
	}

	return session.Lock(updateCreationLockPath)
}

type TransferReplicaCountsRequest struct {
	// ToRCID is the RC ID of the replication controller which will have its
	// replica count increased
	ToRCID fields.ID

	// FromRCID is the RC ID of the replication controller which will have its
	// replica count decreased
	FromRCID fields.ID

	// ReplicasToAdd is the number of replicas to add to the RC indicated by
	// ToRCID
	ReplicasToAdd *int

	// ReplicasToRemove is the number of replicas to remove from the RC indicated
	// by FromRCID
	ReplicasToRemove *int

	// StartingToReplicas is the number of replicas the caller expects the
	// RC specified by ToRCID to have prior to the transaction being
	// applied. This is useful to guarantee that assumptions that went into
	// the calculation of ReplicasToAdd and ReplicasToRemove haven't
	// changed.
	StartingToReplicas *int

	// StartingFromReplicas is the number of replicas the caller expects
	// the RC specified by FromRCID to have prior to the transaction being
	// applied. This is useful to guarantee that assumptions that went into
	// the calculation of ReplicasToAdd and ReplicasToRemove haven't
	// changed.
	StartingFromReplicas *int
}

// TransferReplicaCounts supports transactionally updating the replica counts
// of two RCs in consul.  This is useful for rolling updates to transition
// nodes from the old RC to the new one without risking the consul database
// dying between updates and violating replica count invariants
func (s *ConsulStore) TransferReplicaCounts(req TransferReplicaCountsRequest) error {
	if req.ToRCID == "" {
		return util.Errorf("couldn't transfer replica counts: ToRCID was empty")
	}
	if req.FromRCID == "" {
		return util.Errorf("couldn't transfer replica counts: FromRCID was empty")
	}
	if req.ReplicasToAdd == nil {
		return util.Errorf("couldn't transfer replica counts: ReplicasToAdd was nil")
	}
	if req.ReplicasToRemove == nil {
		return util.Errorf("couldn't transfer replica counts: ReplicasToRemove was nil")
	}
	if req.StartingToReplicas == nil {
		return util.Errorf("couldn't transfer replica counts: StartingToReplicas was nil")
	}
	if req.StartingFromReplicas == nil {
		return util.Errorf("couldn't transfer replica counts: StartingFromReplicas was nil")
	}

	toRCPath, err := s.rcPath(req.ToRCID)
	if err != nil {
		return util.Errorf("couldn't transfer replica counts: %s", err)
	}

	fromRCPath, err := s.rcPath(req.FromRCID)
	if err != nil {
		return util.Errorf("couldn't transfer replica counts: %s", err)
	}

	toRC, toRCIndex, err := s.getWithIndex(req.ToRCID)
	if err != nil {
		return util.Errorf("couldn't transfer replica counts: %s", err)
	}

	if toRC.ReplicasDesired != *req.StartingToReplicas {
		return util.Errorf("couldn't transfer replica counts: RC %s had %d replicas but the request expected it to have %d", req.ToRCID, toRC.ReplicasDesired, *req.StartingToReplicas)
	}

	fromRC, fromRCIndex, err := s.getWithIndex(req.FromRCID)
	if err != nil {
		return util.Errorf("couldn't transfer replica counts: %s", err)
	}

	if fromRC.ReplicasDesired != *req.StartingFromReplicas {
		return util.Errorf("couldn't transfer replica counts: RC %s had %d replicas but the request expected it to have %d", req.FromRCID, fromRC.ReplicasDesired, *req.StartingFromReplicas)
	}

	toRC.ReplicasDesired = toRC.ReplicasDesired + *req.ReplicasToAdd
	fromRC.ReplicasDesired = fromRC.ReplicasDesired - *req.ReplicasToRemove

	toRCBytes, err := json.Marshal(toRC)
	if err != nil {
		return util.Errorf("couldn't transfer replica counts: %s", err)
	}

	fromRCBytes, err := json.Marshal(fromRC)
	if err != nil {
		return util.Errorf("couldn't transfer replica counts: %s", err)
	}

	ops := api.KVTxnOps{
		{
			Verb:  api.KVCAS,
			Key:   fromRCPath,
			Value: fromRCBytes,
			Index: fromRCIndex,
		},
		{
			Verb:  api.KVCAS,
			Key:   toRCPath,
			Value: toRCBytes,
			Index: toRCIndex,
		},
	}

	ok, resp, _, err := s.kv.Txn(ops, nil)
	if err != nil {
		return util.Errorf("replica count transfer failed: %s", err)
	}

	if !ok {
		return util.Errorf("replica count transfer transaction was rolled back. errors: %s", formatTxnErrors(resp))
	}

	// we don't care what the response was if it worked
	return nil
}

func formatTxnErrors(txnResponse *api.KVTxnResponse) string {
	if txnResponse == nil {
		return "none"
	}

	txnErrorsStrings := make([]string, len(txnResponse.Errors))
	for i, err := range txnResponse.Errors {
		txnErrorsStrings[i] = fmt.Sprintf("op %d: %s", err.OpIndex, err.What)
	}

	return strings.Join(txnErrorsStrings, ", ")
}

func keysToRCIDs(keys []string) ([]fields.ID, error) {
	// we expect keys to be formatted like fmt.Sprintf("%s/%s", rcTree, rcID). If
	// that's not the format we get then something is really messed up
	out := make([]fields.ID, len(keys))
	for i, key := range keys {
		rcIDStr := strings.TrimPrefix(key, rcTree+"/")
		rcID, err := fields.ToRCID(rcIDStr)
		if err != nil {
			return nil, util.Errorf("couldn't convert key to uuid: %s", err)
		}

		out[i] = rcID
	}

	return out, nil
}

// computeLabels returns the set of labels to apply when creating a replication
// controller. Currently the only automatic label is the pod ID label which is
// inferred from the rc's manifest. Additional user-supplied labels are also
// added into the label set
func (s *ConsulStore) computeLabels(rc fields.RC, additionalLabels klabels.Set) map[string]string {
	labels := make(map[string]string)
	for k, v := range additionalLabels {
		labels[k] = v
	}

	labels[PodIDLabel] = rc.Manifest.ID().String()

	return labels
}

// Given a consul key path, returns the RC ID and the lock type. Returns an err
// if the key does not resemble an RC lock key
func (s *ConsulStore) lockTypeFromKey(key string) (fields.ID, LockType, error) {
	keyParts := strings.Split(key, "/")
	// Sanity check key structure e.g. /lock/replication_controllers/abcd-1234
	if len(keyParts) < 3 || len(keyParts) > 4 {
		return "", UnknownLockType, util.Errorf("Key '%s' does not resemble an RC lock", key)
	}

	if keyParts[0] != consulutil.LOCK_TREE {
		return "", UnknownLockType, util.Errorf("Key '%s' does not resemble an RC lock", key)
	}

	if keyParts[1] != rcTree {
		return "", UnknownLockType, util.Errorf("Key '%s' does not resemble an RC lock", key)
	}

	rcID := keyParts[2]
	if len(keyParts) == 3 {
		// There's no lock suffix, so this is an ownership lock
		return fields.ID(rcID), OwnershipLockType, nil
	}

	switch keyParts[3] {
	case mutationSuffix:
		return fields.ID(rcID), MutationLockType, nil
	case updateCreationSuffix:
		return fields.ID(rcID), UpdateCreationLockType, nil
	default:
		return fields.ID(rcID), UnknownLockType, nil
	}
}
