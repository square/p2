package rollstore

import (
	"context"
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
	"github.com/square/p2/pkg/store/consul/transaction"
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

type RollLabeler interface {
	SetLabel(labelType labels.Type, id, name, value string) error
	SetLabels(labelType labels.Type, id string, labels map[string]string) error
	SetLabelsTxn(ctx context.Context, labelType labels.Type, id string, labels map[string]string) error
	RemoveAllLabels(labelType labels.Type, id string) error
	GetLabels(labelType labels.Type, id string) (labels.Labeled, error)
	GetMatches(selector klabels.Selector, labelType labels.Type, cachedMatch bool) ([]labels.Labeled, error)
}

type ReplicationControllerStore interface {
	LockForUpdateCreation(rcID rc_fields.ID, session consul.Session) (consulutil.Unlocker, error)
	CreateTxn(ctx context.Context, manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (rc_fields.RC, error)
	Delete(id rc_fields.ID, force bool) error
	UpdateCreationLockPath(rcID rc_fields.ID) (string, error)

	// TODO: delete this. the tests are still using it but the real code isn't
	Create(manifest manifest.Manifest, nodeSelector klabels.Selector, podLabels klabels.Set) (rc_fields.RC, error)
}

type ConsulStore struct {
	kv KV

	// Necessary for acquiring locks on the RCs referred to by an RU update
	// request. This ensures that multiple rolling updates will not be
	// admitted for the same replication controllers
	rcstore ReplicationControllerStore

	// Needed for obtaining consul.Session
	store sessionStore

	// Used for the ability to specify RC for an RU to operate on using a
	// label selector (see labelRCSpecifier)
	labeler RollLabeler

	logger logging.Logger
}

func NewConsul(c consulutil.ConsulClient, labeler RollLabeler, logger *logging.Logger) ConsulStore {
	if logger == nil {
		logger = &logging.DefaultLogger
	}
	return ConsulStore{
		kv:      c.KV(),
		rcstore: rcstore.NewConsul(c, labeler, 3),
		logger:  *logger,
		labeler: labeler,
		store:   consul.NewConsulStore(c),
	}
}

// Get retrieves a rolling update record by it ID.
func (s ConsulStore) Get(id roll_fields.ID) (roll_fields.Update, error) {
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
func (s ConsulStore) List() ([]roll_fields.Update, error) {
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

func (s ConsulStore) newRUCreationSession() (consul.Session, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, util.Errorf("Could not determine hostname for RU create lock name: %s", err)
	}

	lockName := fmt.Sprintf(
		"RU create lock for %s",
		hostname,
	)

	session, renewalErrCh, err := s.store.NewSession(lockName, nil)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range renewalErrCh {
			s.logger.WithError(err).Errorln("error renewing RU creation session")
			// We don't need to do anything else because the
			// transaction in which the RU is created will check
			// that the session still has locks on the RCs
		}
	}()

	return session, nil
}

// Obtains a lock for each RC in the list, or errors. RCs are locked in
// lexicographical order by RC id to avoid deadlocks.
// TODO: lock RCs in a transaction so order doesn't matter
// Returns an error if the locks could not be acquired
// Mutates ctx so that the consul transaction it contains will enforce that the
// locks acquired in this function are still held at the time the transaction
// is applied
func (s ConsulStore) lockRCs(ctx context.Context, rcIDs rc_fields.IDs, session consul.Session) error {
	sort.Sort(rcIDs)
	for _, rcID := range rcIDs {
		updateCreationLockPath, err := s.rcstore.UpdateCreationLockPath(rcID)
		if err != nil {
			return err
		}

		err = transaction.Add(ctx, api.KVTxnOp{
			Verb:    api.KVCheckSession,
			Key:     updateCreationLockPath,
			Session: session.Session(),
		})
		if err != nil {
			return err
		}

		_, err = s.rcstore.LockForUpdateCreation(rcID, session)
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

func (s ConsulStore) checkForConflictingUpdates(rcIDs rc_fields.IDs) error {
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
func (s ConsulStore) CreateRollingUpdateFromExistingRCs(
	ctx context.Context,
	u roll_fields.Update,
	newRCLabels klabels.Set,
	rollLabels klabels.Set,
) (roll_fields.Update, error) {
	session, err := s.newRUCreationSession()
	if err != nil {
		return roll_fields.Update{}, err
	}
	go func() {
		<-ctx.Done()
		_ = session.Destroy()
	}()

	rcIDs := rc_fields.IDs{u.NewRC, u.OldRC}
	err = s.lockRCs(ctx, rcIDs, session)
	if err != nil {
		return roll_fields.Update{}, err
	}

	err = s.checkForConflictingUpdates(rcIDs)
	if err != nil {
		return roll_fields.Update{}, err
	}

	err = s.labeler.SetLabelsTxn(ctx, labels.RC, u.NewRC.String(), newRCLabels)
	if err != nil {
		return roll_fields.Update{}, err
	}

	return u, s.createRU(ctx, u, rollLabels, session.Session())
}

// CreateRollingUpdateFromOneExistingWithRCID is like
// CreateRollingUpdateFromExistingRCs except will create the new RC based on
// passed parameters, using oldRCID for the old RC. The new RC and new RU will
// be created transactionally (all or nothing)
func (s ConsulStore) CreateRollingUpdateFromOneExistingRCWithID(
	ctx context.Context,
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
) (roll_fields.Update, error) {
	session, err := s.newRUCreationSession()
	if err != nil {
		return roll_fields.Update{}, err
	}

	go func() {
		<-ctx.Done()
		_ = session.Destroy()
	}()

	rcIDs := rc_fields.IDs{oldRCID}
	err = s.lockRCs(ctx, rcIDs, session)
	if err != nil {
		return roll_fields.Update{}, err
	}

	err = s.checkForConflictingUpdates(rcIDs)
	if err != nil {
		return roll_fields.Update{}, err
	}

	rc, err := s.rcstore.CreateTxn(ctx, newRCManifest, newRCNodeSelector, newRCPodLabels)
	if err != nil {
		return roll_fields.Update{}, err
	}

	newRCID := rc.ID

	// Get a lock on the new RC we just created so no parallel
	// update creations can use it
	err = s.lockRCs(ctx, rc_fields.IDs{newRCID}, session)
	if err != nil {
		return roll_fields.Update{}, err
	}

	rcIDs = append(rcIDs, newRCID)
	// Check for conflicts again in case an update was created on the new
	// RC between when we created it and locked it
	err = s.checkForConflictingUpdates(rcIDs)
	if err != nil {
		return roll_fields.Update{}, err
	}

	err = s.labeler.SetLabelsTxn(ctx, labels.RC, newRCID.String(), newRCLabels)
	if err != nil {
		return roll_fields.Update{}, err
	}

	u := roll_fields.Update{
		OldRC:           oldRCID,
		NewRC:           newRCID,
		DesiredReplicas: desiredReplicas,
		MinimumReplicas: minimumReplicas,
		LeaveOld:        leaveOld,
		RollDelay:       rollDelay,
	}

	return u, s.createRU(ctx, u, rollLabels, session.Session())
}

// CreateRollingUpdateFromOneMaybeExistingWithLabelSelector creates a rolling
// update that may or may not already have an existing old RC. If one matches
// the oldRCSelector, it will be used as the old RC in the new update. If one
// does not exist, a "dummy" old RC will be created that is identical to the
// specifications for the new RC.  Returns an error if the old RC exists but is
// part of another RU, or if the label selector returns more than one match.
func (s ConsulStore) CreateRollingUpdateFromOneMaybeExistingWithLabelSelector(
	ctx context.Context,
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
) (roll_fields.Update, error) {

	session, err := s.newRUCreationSession()
	if err != nil {
		return roll_fields.Update{}, err
	}
	go func() {
		<-ctx.Done()
		_ = session.Destroy()
	}()

	// Check if any RCs match the oldRCSelector
	matches, err := s.labeler.GetMatches(oldRCSelector, labels.RC, false)
	if err != nil && err != labels.NoLabelsFound {
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
		rc, err := s.rcstore.CreateTxn(ctx, newRCManifest, newRCNodeSelector, newRCPodLabels)
		if err != nil {
			return roll_fields.Update{}, err
		}

		oldRCID = rc.ID
		// Copy the new RC labels to the old RC as well
		err = s.labeler.SetLabels(labels.RC, oldRCID.String(), newRCLabels)
		if err != nil {
			return roll_fields.Update{}, err
		}
	}

	// Lock the old RC to guarantee that no new updates can use it
	err = s.lockRCs(ctx, rc_fields.IDs{oldRCID}, session)
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
	rc, err := s.rcstore.CreateTxn(ctx, newRCManifest, newRCNodeSelector, newRCPodLabels)
	if err != nil {
		return roll_fields.Update{}, err
	}

	newRCID = rc.ID

	// lock newly-created new rc so it's less likely to race on it
	// with another parallel update creation
	err = s.lockRCs(ctx, rc_fields.IDs{newRCID}, session)
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
	err = s.labeler.SetLabelsTxn(ctx, labels.RC, newRCID.String(), newRCLabels)
	if err != nil {
		return roll_fields.Update{}, err
	}

	u := roll_fields.Update{
		OldRC:           oldRCID,
		NewRC:           newRCID,
		DesiredReplicas: desiredReplicas,
		MinimumReplicas: minimumReplicas,
		LeaveOld:        leaveOld,
		RollDelay:       rollDelay,
	}
	return u, s.createRU(ctx, u, rollLabels, session.Session())
}

// Delete deletes a rolling update based on its ID.
func (s ConsulStore) Delete(id roll_fields.ID) error {
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
func (s ConsulStore) Lock(id roll_fields.ID, session string) (bool, error) {
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
func (s ConsulStore) Watch(quit <-chan struct{}) (<-chan []roll_fields.Update, <-chan error) {
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

// createRU adds operations to create the rolling update record and its labels
// and then executes the transaction
// TODO: create an audit log record here as well.
func (s ConsulStore) createRU(
	ctx context.Context,
	u roll_fields.Update,
	rollLabels klabels.Set,
	session string,
) error {
	b, err := json.Marshal(u)
	if err != nil {
		return err
	}

	key, err := RollPath(roll_fields.ID(u.NewRC))
	if err != nil {
		return err
	}

	// Create the rolling update
	err = transaction.Add(ctx, api.KVTxnOp{
		Verb:  api.KVCAS,
		Key:   key,
		Value: b,
	})
	if err != nil {
		return err
	}

	err = s.labeler.SetLabelsTxn(ctx, labels.RU, u.ID().String(), rollLabels)
	if err != nil {
		return err
	}

	return nil
}
