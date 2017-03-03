package main

import (
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/square/p2/pkg/cli"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/podstore"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/types"
)

type store interface {
	NewSession(name string, renewalCh <-chan time.Time) (consul.Session, chan error, error)
	DeletePod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (time.Duration, error)
}

type ReplicationControllerLocker interface {
	LockForMutation(rcID fields.ID, session consul.Session) (consulutil.Unlocker, error)
}

type ReplicationControllerStore interface {
	Disable(id fields.ID) error
	AddDesiredReplicas(id fields.ID, n int) error
	Enable(id fields.ID) error
}

type P2RM struct {
	Store    store
	RCLocker ReplicationControllerLocker
	RCStore  ReplicationControllerStore
	Client   consulutil.ConsulClient
	Labeler  labels.ApplicatorWithoutWatches
	PodStore podstore.Store

	LabelID      string
	NodeName     types.NodeName
	PodID        types.PodID
	PodUniqueKey types.PodUniqueKey
}

// NewLegacyP2RM is a constructor for the P2RM type which configures it to
// remove a "legacy" pod. It will generate the storage types based on its
// api.Client argument
func NewLegacyP2RM(client consulutil.ConsulClient, podName types.PodID, nodeName types.NodeName, labeler labels.ApplicatorWithoutWatches) *P2RM {
	rm := &P2RM{}
	rm.LabelID = path.Join(nodeName.String(), podName.String())
	rm.PodID = podName
	rm.NodeName = nodeName
	rm.PodUniqueKey = ""
	rm.configureStorage(client, labeler)
	return rm
}

// Constructs a *P2RM configured to remove a pod identified by a PodUniqueKey (uuid)
func NewUUIDP2RM(client consulutil.ConsulClient, podUniqueKey types.PodUniqueKey, podID types.PodID, labeler labels.ApplicatorWithoutWatches) *P2RM {
	rm := &P2RM{}
	rm.LabelID = podUniqueKey.String()
	rm.PodID = podID
	rm.NodeName = "" // don't need node name to look up a uuid pod
	rm.PodUniqueKey = podUniqueKey
	rm.configureStorage(client, labeler)
	return rm
}

func (rm *P2RM) configureStorage(client consulutil.ConsulClient, labeler labels.ApplicatorWithoutWatches) {
	rm.Client = client
	rm.Store = consul.NewConsulStore(client)
	consulStore := rcstore.NewConsul(client, labeler, 5)

	// one day these might have different implementations
	rm.RCStore = consulStore
	rm.RCLocker = consulStore

	rm.Labeler = labeler
	rm.PodStore = podstore.NewConsul(client.KV())
}

func (rm *P2RM) checkForManagingReplicationController() (bool, fields.ID, error) {
	podLabels, err := rm.Labeler.GetLabels(labels.POD, rm.LabelID)
	if err != nil {
		return false, "", fmt.Errorf("unable to check node for labels: %v", err)
	}

	if podLabels.Labels.Has(rc.RCIDLabel) {
		return true, fields.ID(podLabels.Labels.Get(rc.RCIDLabel)), nil
	}

	return false, "", nil
}

func (rm *P2RM) decrementDesiredCount(id fields.ID) error {
	session, _, err := rm.Store.NewSession(sessionName(id), nil)
	if err != nil {
		return fmt.Errorf("Unable to get consul session: %v", err)
	}

	rcLock, err := rm.RCLocker.LockForMutation(id, session)
	if err != nil {
		return fmt.Errorf("Unable to lock RC for mutation: %v", err)
	}
	defer rcLock.Unlock()

	err = rm.RCStore.Disable(id)
	if err != nil {
		return fmt.Errorf("Could not disable RC %s: %v", id, err)
	}

	err = rm.deletePod()
	if err != nil {
		return fmt.Errorf("Unable to delete pod. Please re-run this command to clean up: %v", err)
	}

	err = rm.RCStore.AddDesiredReplicas(id, -1)
	if err != nil {
		return fmt.Errorf("Unable to decrement RC count: %v", err)
	}

	err = rm.RCStore.Enable(id)
	if err != nil {
		return fmt.Errorf("Could not enable RC %s: %v", id, err)
	}

	if err = session.Destroy(); err != nil {
		return fmt.Errorf("Unable to destroy consul session: %v", err)
	}

	return nil
}

func (rm *P2RM) deletePod() error {
	if rm.PodUniqueKey == "" {
		err := rm.deleteLegacyPod()
		if err != nil {
			return err
		}
	} else {
		err := rm.deleteUUIDPod()
		if err != nil {
			return err
		}
	}

	return rm.removePodLabels()
}

func (rm *P2RM) deleteLegacyPod() error {
	_, err := rm.Store.DeletePod(consul.INTENT_TREE, rm.NodeName, types.PodID(rm.PodID))
	if err != nil {
		return fmt.Errorf("unable to remove pod: %v", err)
	}

	return nil
}

func (rm *P2RM) deleteUUIDPod() error {
	// Sanity check that the passed pod ID matches the pod being deleted
	pod, err := rm.PodStore.ReadPod(rm.PodUniqueKey)
	switch {
	case err == nil:
		if rm.PodID != "" {
			if pod.Manifest.ID() != rm.PodID {
				return fmt.Errorf("pod %s has a podID of %s, but %s was passed as the --pod option", rm.PodUniqueKey, pod.Manifest.ID(), rm.PodID)
			}
		} else {
			fmt.Println(fmt.Sprintf("%s has pod ID of %s, do you wish to proceed?", rm.PodUniqueKey, pod.Manifest.ID()))
			confirmed := cli.Confirm()
			if !confirmed {
				return errors.New("aborted")
			}
		}

		err = rm.PodStore.Unschedule(rm.PodUniqueKey)
		if err != nil {
			return fmt.Errorf("Unable to unschedule pod: %s", err)
		}
	case podstore.IsNoPod(err):
		// This is okay, the command might be re-run to flush out label deletion errors
	case err != nil:
		return fmt.Errorf("Could not verify that pod %s matches pod ID of %s: %s", rm.PodUniqueKey, rm.PodID, err)
	}

	return nil
}

func (rm *P2RM) removePodLabels() error {
	err := rm.Labeler.RemoveAllLabels(labels.POD, rm.LabelID)
	if err != nil {
		return fmt.Errorf(
			"pod is partially deleted. re-run command to finish deleting\n"+
				"unable to remove pod labels: %v",
			err,
		)
	}

	return nil
}
