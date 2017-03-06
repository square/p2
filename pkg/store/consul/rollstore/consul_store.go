package rollstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"time"

	"github.com/hashicorp/consul/api"
	klabels "k8s.io/kubernetes/pkg/labels"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	roll_fields "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/util"
)

const rollTree string = "rolls"

// Interface that allows us to inject a test implementation of the consul api
type KV interface {
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	CAS(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
	Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error)
	Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
}

var _ KV = &api.KV{}

var AmbiguousRCSelector error = errors.New("The old RC selector was ambigous and produced > 1 matches")

func IsAmbiguousRCSelector(err error) bool {
	return err == AmbiguousRCSelector
}

// A subset of the consul.Store interface used by rollstore
type sessionStore interface {
	NewSession(name string, renewalCh <-chan time.Time) (consul.Session, chan error, error)
}

type rollLabeler interface {
	SetLabel(labelType labels.Type, id, name, value string) error
	SetLabels(labelType labels.Type, id string, labels map[string]string) error
	RemoveAllLabels(labelType labels.Type, id string) error
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
	GetMatches(selector klabels.Selector, labelType labels.Type, cachedMatch bool) ([]labels.Labeled, error)
}

type ReplicationControllerStore interface {
	LockForUpdateCreation(rcID rc_fields.ID, session consul.Session) (consulutil.Unlocker, error)
	Create(manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (rc_fields.RC, error)
	Delete(id rc_fields.ID, force bool) error
}

type consulStore struct {
	kv KV

	// Necessary for acquiring locks on the RCs referred to by an RU update
	// request. This ensures that multiple rolling updates will not be
	// admitted for the same replication controllers
	rcstore ReplicationControllerStore

	// Needed for obtaining consul.Session
	store sessionStore

	// Used for the ability to specify RC for an RU to operate on using a
	// label selector (see labelRCSpecifier)
	labeler rollLabeler

	logger logging.Logger
}

func NewConsul(c consulutil.ConsulClient, labeler rollLabeler, logger *logging.Logger) consulStore {
	if logger == nil {
		logger = &logging.DefaultLogger
	}
	return consulStore{
		kv:      c.KV(),
		rcstore: rcstore.NewConsul(c, labeler, 3),
		logger:  *logger,
		labeler: labeler,
		store:   consul.NewConsulStore(c),
	}
}

// Get retrieves a rolling update record by it ID.
func (s consulStore) Get(id roll_fields.ID) (roll_fields.Update, error) {
	key, err := RollPath(id)
	if err != nil {
		return roll_fields.Update{}, nil
	}

	kvp, _, err := s.kv.Get(key, nil)
	if err != nil {
		return roll_fields.Update{}, consulutil.NewKVError("get", key, err)
	}
	if kvp == nil {
		return roll_fields.Update{}, nil
	}

	return kvpToRU(kvp)
}

// List returns all rolling update records.
func (s consulStore) List() ([]roll_fields.Update, error) {
	listed, _, err := s.kv.List(rollTree+"/", nil)
	if err != nil {
		return nil, err
	}

	ret := make([]roll_fields.Update, 0, len(listed))
	for _, kvp := range listed {
		ru, err := kvpToRU(kvp)
		if err != nil {
			return nil, err
		}
		ret = append(ret, ru)
	}
	return ret, nil
}

func (s consulStore) newRUCreationSession() (consul.Session, chan error, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, nil, util.Errorf("Could not determine hostname for RU create lock name: %s", err)
	}

	lockName := fmt.Sprintf(
		"RU create lock for %s",
		hostname,
	)

	return s.store.NewSession(lockName, nil)
}

// Obtains a lock for each RC in the list, or errors. RCs are locked in
// lexicographical order by RC id to avoid deadlocks.
func (s consulStore) lockRCs(rcIDs rc_fields.IDs, session consul.Session) error {
	sort.Sort(rcIDs)
	for _, rcID := range rcIDs {
		_, err := s.rcstore.LockForUpdateCreation(rcID, session)
		if err != nil {
			return err
		}
	}

	return nil
}

type ConflictingRUError struct {
	ConflictingID   roll_fields.ID
	ConflictingRCID rc_fields.ID
}

func (c *ConflictingRUError) Error() string {
	return fmt.Sprintf("A deploy is already in progress. (RU %s conflicts on RC %s)", c.ConflictingID, c.ConflictingRCID)
}

func (s consulStore) checkForConflictingUpdates(rcIDs rc_fields.IDs) error {
	// Now that locks are held, check every RU and confirm that none of
	// them refer to the new or old RCs
	// This is potentially a scaling bottleneck (see function comment)
	rus, err := s.List()
	if err != nil {
		return err
	}

	var conflictingRUError *ConflictingRUError
	for _, ru := range rus {
		for _, id := range rcIDs {
			if ru.NewRC == id || ru.OldRC == id {
				conflictingRUError = &ConflictingRUError{
					ConflictingID:   ru.ID(),
					ConflictingRCID: id,
				}
				break
			}
		}
	}

	if conflictingRUError != nil {
		return conflictingRUError
	}

	return nil
}

// CreateRollingUpdateFromExistingRCs creates a rolling update in consul, under
// the following conditions:
// 1) A lock can be acquired for both the old and new replication controllers.
//    - this is in lexicographical order by replication controller id so that
//      identical update requests will not deadlock eachother
// 2) A rolling update does not already exist that refers to the same replication
//    controllers (whether new or old).
//    - This check is done while the locks are held, so that no new rolling
//      update can be created referring to old and new while doing this query.
//    - It naively searches every rolling update and checks its fields. While this
//      introduces O(n) runtime complexity on the number of updates, since updates
//      are short lived we don't anticipate it to be a scalability bottleneck.
//      If scalability were a concern, an alternate approach would be to place
//      labels on replication controllers referring back to the RUs that they
//      refer to. Then a constant lookup can be done for those labels, and the
//      operation can be aborted.
func (s consulStore) CreateRollingUpdateFromExistingRCs(u roll_fields.Update, newRCLabels klabels.Set, rollLabels klabels.Set) (roll_fields.Update, error) {
	session, renewalErrCh, err := s.newRUCreationSession()
	if err != nil {
		return roll_fields.Update{}, err
	}
	defer session.Destroy()

	rcIDs := rc_fields.IDs{u.NewRC, u.OldRC}
	err = s.lockRCs(rcIDs, session)
	if err != nil {
		return roll_fields.Update{}, err
	}

	err = s.checkForConflictingUpdates(rcIDs)
	if err != nil {
		return roll_fields.Update{}, err
	}

	err = s.labeler.SetLabels(labels.RC, u.NewRC.String(), newRCLabels)
	if err != nil {
		// This is potentially bad, because the labels that were there
		// before have been overwritten. However its unreasonable to
		// expect that a rollback will succeed if the roll-forward
		// didn't succeed, so just report the error
		return roll_fields.Update{}, err
	}

	return s.attemptRUCreation(u, rollLabels, renewalErrCh)
}

// CreateRollingUpdateFromOneExistingWithRCID is like
// CreateRollingUpdateFromExistingRCs except will create the new RC based on
// passed parameters, using oldRCID for the old RC. The new RC and new RU will
// be created transactionally (all or nothing)
func (s consulStore) CreateRollingUpdateFromOneExistingRCWithID(
	oldRCID rc_fields.ID,
	desiredReplicas int,
	minimumReplicas int,
	leaveOld bool,
	rollDelay time.Duration,
	newRCManifest manifest.Manifest,
	newRCNodeSelector klabels.Selector,
	newRCPodLabels klabels.Set,
	newRCLabels klabels.Set,
	rollLabels klabels.Set,
) (u roll_fields.Update, err error) {
	// There are cases where this function will create the new RC and
	// subsequently fail, in which case we need to do some cleanup.

	// cleans up new RC, might be nil if we didn't create one
	var newRCCleanup func()

	// If we had an error and the rc cleanup function is set, run it
	defer func() {
		if err != nil && newRCCleanup != nil {
			newRCCleanup()
		}
	}()

	var session consul.Session
	var renewalErrCh chan error
	session, renewalErrCh, err = s.newRUCreationSession()
	if err != nil {
		return roll_fields.Update{}, err
	}
	defer session.Destroy()

	rcIDs := rc_fields.IDs{oldRCID}
	err = s.lockRCs(rcIDs, session)
	if err != nil {
		return roll_fields.Update{}, err
	}

	err = s.checkForConflictingUpdates(rcIDs)
	if err != nil {
		return roll_fields.Update{}, err
	}

	// Now create the new RC, first checking if our session is still valid
	var newRCID rc_fields.ID
	select {
	case err = <-renewalErrCh:
		return roll_fields.Update{}, err
	default:
		rc, err := s.rcstore.Create(newRCManifest, newRCNodeSelector, newRCPodLabels)
		if err != nil {
			return roll_fields.Update{}, err
		}

		newRCCleanup = func() {
			err := s.rcstore.Delete(newRCID, false)
			if err != nil {
				s.logger.WithError(err).Errorln("Unable to cleanup RC %s after failed RU creation attempt", newRCID)
			}
		}

		newRCID = rc.ID

		// Get a lock on the new RC we just created so no parallel
		// update creations can use it
		err = s.lockRCs(rc_fields.IDs{newRCID}, session)
		if err != nil {
			return roll_fields.Update{}, err
		}
	}

	rcIDs = append(rcIDs, newRCID)
	// Check for conflicts again in case an update was created on the new
	// RC between when we created it and locked it
	err = s.checkForConflictingUpdates(rcIDs)
	if err != nil {
		return roll_fields.Update{}, err
	}

	err = s.labeler.SetLabels(labels.RC, newRCID.String(), newRCLabels)
	if err != nil {
		return roll_fields.Update{}, err
	}

	u = roll_fields.Update{
		OldRC:           oldRCID,
		NewRC:           newRCID,
		DesiredReplicas: desiredReplicas,
		MinimumReplicas: minimumReplicas,
		LeaveOld:        leaveOld,
		RollDelay:       rollDelay,
	}

	return s.attemptRUCreation(u, rollLabels, renewalErrCh)
}

// CreateRollingUpdateFromOneMaybeExistingWithLabelSelector creates a rolling
// update that may or may not already have an existing old RC. If one matches
// the oldRCSelector, it will be used as the old RC in the new update. If one
// does not exist, a "dummy" old RC will be created that is identical to the
// specifications for the new RC.  Returns an error if the old RC exists but is
// part of another RU, or if the label selector returns more than one match.
func (s consulStore) CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
	oldRCSelector klabels.Selector,
	desiredReplicas int,
	minimumReplicas int,
	leaveOld bool,
	rollDelay time.Duration,
	newRCManifest manifest.Manifest,
	newRCNodeSelector klabels.Selector,
	newRCPodLabels klabels.Set,
	newRCLabels klabels.Set,
	rollLabels klabels.Set,
) (u roll_fields.Update, err error) {
	// This function may or may not create old and new RCs and subsequently
	// fail, so we defer a function that does any cleanup (if applicable)
	var cleanupOldRC func()
	var cleanupNewRC func()

	defer func() {
		if err != nil {
			if cleanupOldRC != nil {
				cleanupOldRC()
			}

			if cleanupNewRC != nil {
				cleanupNewRC()
			}
		}
	}()

	session, renewalErrCh, err := s.newRUCreationSession()
	if err != nil {
		return roll_fields.Update{}, err
	}
	defer session.Destroy()

	// Check if any RCs match the oldRCSelector
	matches, err := s.labeler.GetMatches(oldRCSelector, labels.RC, false)
	if err != nil {
		return roll_fields.Update{}, err
	}

	var oldRCID rc_fields.ID
	if len(matches) > 1 {
		return roll_fields.Update{}, AmbiguousRCSelector
	} else if len(matches) == 1 {
		oldRCID = rc_fields.ID(matches[0].ID)
	} else {
		if leaveOld {
			return roll_fields.Update{}, util.Errorf(
				"Can't create an update with LeaveOld set if there is no old RC (sel=%s)",
				oldRCSelector.String(),
			)
		}

		// Create the old RC using the same info as the new RC, it'll be
		// removed when the update completes anyway
		rc, err := s.rcstore.Create(newRCManifest, newRCNodeSelector, newRCPodLabels)
		if err != nil {
			return roll_fields.Update{}, err
		}

		oldRCID = rc.ID
		cleanupOldRC = func() {
			err = s.rcstore.Delete(oldRCID, false)
			if err != nil {
				s.logger.WithError(err).Errorf("Unable to cleanup newly-created old RC %s after update creation failure:", oldRCID)
			}

			// Any labels we wrote will be deleted by rcstore.Delete()
		}

		// Copy the new RC labels to the old RC as well
		err = s.labeler.SetLabels(labels.RC, oldRCID.String(), newRCLabels)
		if err != nil {
			return roll_fields.Update{}, err
		}
	}

	// Lock the old RC to guarantee that no new updates can use it
	err = s.lockRCs(rc_fields.IDs{oldRCID}, session)
	if err != nil {
		return roll_fields.Update{}, err
	}

	// Check for updates that exist that operate on the old RC
	err = s.checkForConflictingUpdates(rc_fields.IDs{oldRCID})
	if err != nil {
		return roll_fields.Update{}, err
	}

	// Create the new RC
	var newRCID rc_fields.ID
	select {
	case err = <-renewalErrCh:
		return roll_fields.Update{}, err
	default:
		rc, err := s.rcstore.Create(newRCManifest, newRCNodeSelector, newRCPodLabels)
		if err != nil {
			return roll_fields.Update{}, err
		}

		newRCID = rc.ID
	}

	cleanupNewRC = func() {
		err = s.rcstore.Delete(newRCID, false)
		if err != nil {
			s.logger.WithError(err).Errorf("Unable to cleanup newly-created new RC %s after update creation failure:", newRCID)
		}

		// Any labels we wrote will be deleted by rcstore.Delete()
	}

	// lock newly-created new rc so it's less likely to race on it
	// with another parallel update creation
	err = s.lockRCs(rc_fields.IDs{newRCID}, session)
	if err != nil {
		return roll_fields.Update{}, err
	}

	// Check once again for conflicting updates in case a racing update
	// creation grabbed the new RC we just created
	err = s.checkForConflictingUpdates(rc_fields.IDs{newRCID})
	if err != nil {
		return roll_fields.Update{}, err
	}

	// Now that we know there are no RUs in progress, and we have the
	// update creation locks, we can safely apply labels.
	err = s.labeler.SetLabels(labels.RC, newRCID.String(), newRCLabels)
	if err != nil {
		return roll_fields.Update{}, err
	}

	u = roll_fields.Update{
		OldRC:           oldRCID,
		NewRC:           newRCID,
		DesiredReplicas: desiredReplicas,
		MinimumReplicas: minimumReplicas,
		LeaveOld:        leaveOld,
		RollDelay:       rollDelay,
	}
	return s.attemptRUCreation(u, rollLabels, renewalErrCh)
}

// Delete deletes a rolling update based on its ID.
func (s consulStore) Delete(id roll_fields.ID) error {
	key, err := RollPath(id)
	if err != nil {
		return err
	}

	_, err = s.kv.Delete(key, nil)
	if err != nil {
		return consulutil.NewKVError("delete", key, err)
	}

	err = s.labeler.RemoveAllLabels(labels.RU, id.String())
	if err != nil {
		// TODO: If this fails, then we have some dangling labels. The labeler does retry a few times though
		return err
	}

	return nil
}

// Lock takes a lock on a rolling update by ID. Before taking ownership of an
// Update, its new RC ID, and old RC ID if any, should both be locked. If the
// error return is nil, then the boolean indicates whether the lock was
// successfully taken.
func (s consulStore) Lock(id roll_fields.ID, session string) (bool, error) {
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

// Watch wtches for changes to the store and generate a list of Updates for each
// change. This function does not block.
func (s consulStore) Watch(quit <-chan struct{}) (<-chan []roll_fields.Update, <-chan error) {
	inCh := make(chan api.KVPairs)

	outCh, errCh := publishLatestRolls(inCh, quit)
	go consulutil.WatchPrefix(rollTree+"/", s.kv, inCh, quit, errCh, 0)

	return outCh, errCh
}

func publishLatestRolls(inCh <-chan api.KVPairs, quit <-chan struct{}) (<-chan []roll_fields.Update, chan error) {
	outCh := make(chan []roll_fields.Update)
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

			out := make([]roll_fields.Update, 0, len(listed))
			for _, kvp := range listed {
				var next roll_fields.Update
				if err := json.Unmarshal(kvp.Value, &next); err != nil {
					select {
					case errCh <- err:
						// abandon the value we were writing, it's probably corrupt
						needToWrite = false
					case <-quit:
						// stop processing this kvp list; inCh should be closed
						// in a moment
						return
					}
				} else {
					out = append(out, next)
				}
			}

			if needToWrite {
				select {
				case outCh <- out:
					needToWrite = false
				case listed, ok = <-inCh:
					if !ok {
						// channel closed
						return
					}
					// We got a new value before we write the old one,
					// drop the old one
					needToWrite = true
				case <-quit:
					return
				}
			}
		}
	}()

	return outCh, errCh
}

func RollPath(id roll_fields.ID) (string, error) {
	if id == "" {
		return "", util.Errorf("id not specified when computing roll path")
	}
	return path.Join(rollTree, string(id)), nil
}

// Roll paths are computed using the id of the new replication controller
func RollLockPath(id roll_fields.ID) (string, error) {
	subRollPath, err := RollPath(id)
	if err != nil {
		return "", err
	}

	return path.Join(consulutil.LOCK_TREE, subRollPath), nil
}

func kvpToRU(kvp *api.KVPair) (roll_fields.Update, error) {
	ru := roll_fields.Update{}
	err := json.Unmarshal(kvp.Value, &ru)
	if err != nil {
		return ru, util.Errorf("Unable to unmarshal value as rolling update: %s", err)
	}
	return ru, nil
}

// Attempts to create a rolling update. Checks sessionErrCh for session renewal
// errors just before actually doing the creation to minimize the likelihood of
// race conditions resulting in conflicting RUs
func (s consulStore) attemptRUCreation(u roll_fields.Update, rollLabels klabels.Set, sessionErrCh chan error) (createdRU roll_fields.Update, err error) {
	// If we create an RU, we also want to create its labels. If the second step
	// fails, we want to best-effort remove the RU
	var ruCleanup func()
	defer func() {
		if err != nil && ruCleanup != nil {
			ruCleanup()
		}
	}()

	b, err := json.Marshal(u)
	if err != nil {
		return u, err
	}

	key, err := RollPath(roll_fields.ID(u.NewRC))
	if err != nil {
		return u, err
	}

	// Confirm that our lock session is still valid, and then create the
	// rolling update. If session isn't valid, delete the newRC we just
	// created
	select {
	case err := <-sessionErrCh:
		if err == nil {
			err = util.Errorf("Cannot create ru because session was destroyed")
		}
		return u, err
	default:
		success, _, err := s.kv.CAS(&api.KVPair{
			Key:   key,
			Value: b,
		}, nil)
		if err != nil {
			return u, consulutil.NewKVError("cas", key, err)
		}

		// Shouldn't be possible if our session is still valid, preventing other insertions
		if !success {
			return u, util.Errorf("update with new RC ID %s already exists", u.NewRC)
		}

		ruCleanup = func() {
			err := s.Delete(u.ID())
			if err != nil {
				s.logger.WithError(err).Errorln("Unable to cleanup RU %s after failed labeling attempt", u.ID())
			}
		}
	}

	err = s.labeler.SetLabels(labels.RU, u.ID().String(), rollLabels)
	if err != nil {
		return roll_fields.Update{}, err
	}

	return u, nil
}
