package rcstore

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store"
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
	List(prefix string, opts *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	CAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
	DeleteCAS(pair *api.KVPair, opts *api.WriteOptions) (bool, *api.WriteMeta, error)
}

type consulStore struct {
	labeler rcLabeler
	kv      consulKV
	retries int
}

// TODO: combine with similar CASError type in pkg/labels
type CASError string

func (e CASError) Error() string {
	return fmt.Sprintf("Could not check-and-set key %q", string(e))
}

var _ Store = &consulStore{}

type rcLabeler interface {
	SetLabel(labelType labels.Type, id, name, value string) error
	RemoveAllLabels(labelType labels.Type, id string) error
}

func NewConsul(client consulutil.ConsulClient, labeler rcLabeler, retries int) *consulStore {
	return &consulStore{
		retries: retries,
		labeler: labeler,
		kv:      client.KV(),
	}
}

func (s *consulStore) Create(manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (store.ReplicationController, error) {
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
		return store.ReplicationController{}, err
	}

	// labels do not need to be retried, consul labeler does that itself
	err = s.forEachLabel(rc, func(id, k, v string) error {
		return s.labeler.SetLabel(labels.RC, rc.ID.String(), k, v)
	})
	if err != nil {
		return store.ReplicationController{}, err
	}

	return rc, nil
}

// these parts of Create may require a retry
func (s *consulStore) innerCreate(manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (store.ReplicationController, error) {
	id := store.ReplicationControllerID(uuid.New())
	rcp, err := s.rcPath(id)
	if err != nil {
		return store.ReplicationController{}, err
	}
	rc := store.ReplicationController{
		ID:              id,
		Manifest:        manifest,
		NodeSelector:    nodeSelector,
		PodLabels:       podLabels,
		ReplicasDesired: 0,
		Disabled:        false,
	}

	jsonRC, err := json.Marshal(rc)
	if err != nil {
		return store.ReplicationController{}, util.Errorf("Could not marshal RC as json: %s", err)
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
		return store.ReplicationController{}, consulutil.NewKVError("cas", rcp, err)
	}
	if !success {
		return store.ReplicationController{}, CASError(rcp)
	}
	return rc, nil
}

func (s *consulStore) Get(id store.ReplicationControllerID) (store.ReplicationController, error) {
	rcp, err := s.rcPath(id)
	if err != nil {
		return store.ReplicationController{}, err
	}

	kvp, _, err := s.kv.Get(rcp, nil)
	if err != nil {
		return store.ReplicationController{}, consulutil.NewKVError("get", rcp, err)
	}
	if kvp == nil {
		// ID didn't exist
		return store.ReplicationController{}, NoReplicationController
	}
	return kvpToRC(kvp)
}

func (s *consulStore) List() ([]store.ReplicationController, error) {
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
func (s *consulStore) WatchNew(quit <-chan struct{}) (<-chan []store.ReplicationController, <-chan error) {
	inCh := make(chan api.KVPairs)

	outCh, errCh := publishLatestRCs(inCh, quit)
	go consulutil.WatchPrefix(rcTree+"/", s.kv, inCh, quit, errCh, 1*time.Second)

	return outCh, errCh
}

// Wraps an RC with lock information
type RCLockResult struct {
	store.ReplicationController
	LockedForOwnership      bool
	LockedForUpdateCreation bool
	LockedForMutation       bool
}

// Like WatchNew() but instead of returning raw store.ReplicationController types it wraps them
// in a struct that indicates what types of locks are held on the RC. This is
// useful in reducing failed lock attempts in the RC farms which have a latency
// cost. WatchNewWithLockInfo() can retrieve the contents of the entire
// replication controller lock tree once and farms can only attempt to acquire
// locks that were not held at some recent time
//
// Since lock information is retrieved once per update to the RC list, it's
// possible that lock information will be out of date as the list is processed.
// However, a subsequent update will get the correct view of the world so the
// behavior should be correct
func (s *consulStore) WatchNewWithRCLockInfo(quit <-chan struct{}, pauseTime time.Duration) (<-chan []RCLockResult, <-chan error) {
	inCh := make(chan api.KVPairs)
	lockInfoErrCh := make(chan error)
	combinedErrCh := make(chan error)

	rcCh, rcErrCh := publishLatestRCs(inCh, quit)
	go consulutil.WatchPrefix(rcTree+"/", s.kv, inCh, quit, rcErrCh, pauseTime)

	// Process RC updates and augment them with lock information
	outCh, lockInfoErrCh := s.publishLatestRCsWithLockInfo(rcCh, quit)

	// Fan-in the two error channels into one source
	go func() {
		defer close(combinedErrCh)
		var err error
		for {
			select {
			case err = <-lockInfoErrCh:
			case err = <-rcErrCh:
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
	return outCh, combinedErrCh
}

// Pulled out of WatchNew for testing purposes (faking watches on a mock KV is
// hard). It converts api.KVPairs values received on inCh to []store.ReplicationController, and
// attempts to pass them on outCh. It will discard updates that are not read
// quickly enough (see WatchNew)
func publishLatestRCs(inCh <-chan api.KVPairs, quit <-chan struct{}) (<-chan []store.ReplicationController, chan error) {
	outCh := make(chan []store.ReplicationController)
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

func (s *consulStore) publishLatestRCsWithLockInfo(inCh <-chan []store.ReplicationController, quit <-chan struct{}) (chan []RCLockResult, chan error) {
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
			case rcFields, ok := <-inCh:
				if !ok {
					return
				}

				var res []RCLockResult
				rcMap := make(map[store.ReplicationControllerID]*RCLockResult)
				for _, rc := range rcFields {
					rcMap[rc.ID] = &RCLockResult{
						ReplicationController: rc,
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

func kvpToRC(kvp *api.KVPair) (store.ReplicationController, error) {
	rc := store.ReplicationController{}
	err := json.Unmarshal(kvp.Value, &rc)
	if err != nil {
		return rc, util.Errorf("Could not unmarshal RC ('%s') as json: %s", string(kvp.Value), err)
	}
	if rc.Manifest == nil {
		return rc, util.Errorf("%s: RC has no manifest", kvp.Key)
	}

	return rc, nil
}

func kvpsToRCs(l api.KVPairs) ([]store.ReplicationController, error) {
	ret := make([]store.ReplicationController, 0, len(l))
	for _, kvp := range l {
		rc, err := kvpToRC(kvp)
		if err != nil {
			return nil, err
		}
		ret = append(ret, rc)
	}
	return ret, nil
}

func (s *consulStore) Disable(id store.ReplicationControllerID) error {
	return s.retryMutate(id, func(rc store.ReplicationController) (store.ReplicationController, error) {
		rc.Disabled = true
		return rc, nil
	})
}

func (s *consulStore) Enable(id store.ReplicationControllerID) error {
	return s.retryMutate(id, func(rc store.ReplicationController) (store.ReplicationController, error) {
		rc.Disabled = false
		return rc, nil
	})
}

func (s *consulStore) SetDesiredReplicas(id store.ReplicationControllerID, n int) error {
	return s.retryMutate(id, func(rc store.ReplicationController) (store.ReplicationController, error) {
		rc.ReplicasDesired = n
		return rc, nil
	})
}

func (s *consulStore) AddDesiredReplicas(id store.ReplicationControllerID, n int) error {
	return s.retryMutate(id, func(rc store.ReplicationController) (store.ReplicationController, error) {
		rc.ReplicasDesired += n
		if rc.ReplicasDesired < 0 {
			rc.ReplicasDesired = 0
		}
		return rc, nil
	})
}

func (s *consulStore) CASDesiredReplicas(id store.ReplicationControllerID, expected int, n int) error {
	return s.retryMutate(id, func(rc store.ReplicationController) (store.ReplicationController, error) {
		if rc.ReplicasDesired != expected {
			return rc, fmt.Errorf("replication controller %s has %d desired replicas instead of %d, not setting to %d", rc.ID, rc.ReplicasDesired, expected, n)
		}
		rc.ReplicasDesired = n
		return rc, nil
	})
}

func (s *consulStore) Delete(id store.ReplicationControllerID, force bool) error {
	return s.retryMutate(id, func(rc store.ReplicationController) (store.ReplicationController, error) {
		if !force && rc.ReplicasDesired != 0 {
			return store.ReplicationController{}, fmt.Errorf("replication controller %s has %d desired replicas (must reduce to 0 before deleting)", rc.ID, rc.ReplicasDesired)
		}
		return store.ReplicationController{}, nil
	})
}

// TODO: this function is almost a verbatim copy of pkg/labels retryMutate, can
// we find some way to combine them?
func (s *consulStore) retryMutate(id store.ReplicationControllerID, mutator func(store.ReplicationController) (store.ReplicationController, error)) error {
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
func (s *consulStore) mutateRc(id store.ReplicationControllerID, mutator func(store.ReplicationController) (store.ReplicationController, error)) error {
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

func (s *consulStore) Watch(rc *store.ReplicationController, quit <-chan struct{}) (<-chan struct{}, <-chan error) {
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

func (s *consulStore) rcLockRoot() string {
	return path.Join(consulutil.LOCK_TREE, rcTree)
}

func (s *consulStore) rcPath(rcID store.ReplicationControllerID) (string, error) {
	if rcID == "" {
		return "", util.Errorf("Path requested for empty RC id")
	}

	return path.Join(rcTree, string(rcID)), nil
}

func (s *consulStore) rcLockPath(rcID store.ReplicationControllerID) (string, error) {
	rcPath, err := s.rcPath(rcID)
	if err != nil {
		return "", err
	}

	return path.Join(consulutil.LOCK_TREE, rcPath), nil
}

// The path for an ownership lock is simply the base path
func (s *consulStore) ownershipLockPath(rcID store.ReplicationControllerID) (string, error) {
	return s.rcLockPath(rcID)
}

// Acquires a lock on the RC that should be used by RC farm goroutines, whose
// job it is to carry out the intent of the RC
func (s *consulStore) LockForOwnership(rcID store.ReplicationControllerID, session kp.Session) (consulutil.Unlocker, error) {
	lockPath, err := s.rcLockPath(rcID)
	if err != nil {
		return nil, err
	}

	return session.Lock(lockPath)
}

func (s *consulStore) mutationLockPath(rcID store.ReplicationControllerID) (string, error) {
	baseLockPath, err := s.rcLockPath(rcID)
	if err != nil {
		return "", err
	}

	return path.Join(baseLockPath, mutationSuffix), nil
}

// Acquires a lock on the RC with the intent of mutating it. Must be held by
// goroutines in the rolling update farm as well as any other tool that may
// mutate an RC
func (s *consulStore) LockForMutation(rcID store.ReplicationControllerID, session kp.Session) (consulutil.Unlocker, error) {
	mutationLockPath, err := s.mutationLockPath(rcID)
	if err != nil {
		return nil, err
	}

	return session.Lock(mutationLockPath)
}

func (s *consulStore) updateCreationLockPath(rcID store.ReplicationControllerID) (string, error) {
	baseLockPath, err := s.rcLockPath(rcID)
	if err != nil {
		return "", err
	}

	return path.Join(baseLockPath, updateCreationSuffix), nil
}

// Acquires a lock on the RC for ensuring that no two rolling updates are
// created that operate on the same replication controllers.  A lock on both
// the intended "new" and "old" replication controllers should be held before
// the update is created.
func (s *consulStore) LockForUpdateCreation(rcID store.ReplicationControllerID, session kp.Session) (consulutil.Unlocker, error) {
	updateCreationLockPath, err := s.updateCreationLockPath(rcID)
	if err != nil {
		return nil, err
	}

	return session.Lock(updateCreationLockPath)
}

// forEachLabel Attempts to apply the supplied function to labels of the replication controller.
// If forEachLabel encounters any error applying the function, it returns that error immediately.
// The function is not further applied to subsequent labels on an error.
func (s *consulStore) forEachLabel(rc store.ReplicationController, f func(id, k, v string) error) error {
	id := rc.ID.String()
	// As of this writing the only label we want is the pod ID.
	// There may be more in the future.
	return f(id, PodIDLabel, string(rc.Manifest.ID()))
}

// Given a consul key path, returns the RC ID and the lock type. Returns an err
// if the key does not resemble an RC lock key
func (s *consulStore) lockTypeFromKey(key string) (store.ReplicationControllerID, LockType, error) {
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
		return store.ReplicationControllerID(rcID), OwnershipLockType, nil
	}

	switch keyParts[3] {
	case mutationSuffix:
		return store.ReplicationControllerID(rcID), MutationLockType, nil
	case updateCreationSuffix:
		return store.ReplicationControllerID(rcID), UpdateCreationLockType, nil
	default:
		return store.ReplicationControllerID(rcID), UnknownLockType, nil
	}
}
