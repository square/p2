// p2-rm is a command line tool for removing a pods and its labels.
package main

import (
	"fmt"
	"os"
	"os/user"
	"path"

	"github.com/hashicorp/consul/api"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
)

// Command line flags
var (
	podName      = kingpin.Arg("pod", "The names of the pod to be removed").Required().String()
	nodeName     = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	deallocation = kingpin.Flag("deallocate", "Specifies that we are deallocating this pod on this node. Using this switch will mutate the desired_replicas value on a managing RC, if one exists.").Bool()
)

// P2RM contains the state necessary to safely remove a pod from a node
type P2RM struct {
	Store   kp.Store
	RCStore rcstore.Store
	Client  *api.Client
	Labeler labels.Applicator

	LabelID  string
	NodeName types.NodeName
	PodName  string
}

// NewP2RM is a constructor for the P2RM type. It will generate the necessary
// storage types based on its api.Client argument
func NewP2RM(client *api.Client, podName string, nodeName types.NodeName) *P2RM {
	rm := &P2RM{}
	rm.Client = client
	rm.Store = kp.NewConsulStore(client)
	rm.RCStore = rcstore.NewConsul(client, 5)
	rm.Labeler = labels.NewConsulApplicator(client, 3)
	rm.LabelID = path.Join(nodeName.String(), podName)
	rm.PodName = podName
	rm.NodeName = nodeName

	return rm
}

func main() {
	kingpin.Version(version.VERSION)
	_, opts := flags.ParseWithConsulOptions()

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error getting hostname. use --node to specify a node: %v\n", err)
			os.Exit(1)
		}
		*nodeName = hostname
	}

	rm := NewP2RM(kp.NewConsulClient(opts), *podName, types.NodeName(*nodeName))

	podIsManagedByRC, rcID, err := rm.checkForManagingReplicationController()
	if err != nil {
		os.Exit(2)
	}

	if !podIsManagedByRC {
		err = rm.deletePod()
		if err != nil {
			os.Exit(2)
		}
	}

	if podIsManagedByRC && !*deallocation {
		fmt.Fprintf(
			os.Stderr,
			"error: %s is managed by replication controller: %s\n"+
				"It's possible you meant you deallocate this pod on this node. If so, please confirm your intention with --deallocate\n", *nodeName, rcID)
		os.Exit(2)
	}

	if podIsManagedByRC && *deallocation {
		err = rm.decrementDesiredCount(rcID)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"Encountered error deallocating from the RC %s. You may attempt this command again or use `p2-rctl` to cleanup manually.\n%v",
				rcID,
				err)
		}
	}

	fmt.Printf("%s: successfully removed %s\n", rm.NodeName, rm.PodName)
}

func sessionName(rcID fields.ID) string {
	currentUser, err := user.Current()
	username := "unknown"
	if err == nil {
		username = currentUser.Username
	}

	return fmt.Sprintf("p2-rm:user:%s:rcID:%s", username, rcID)
}

func (rm *P2RM) decrementDesiredCount(id fields.ID) error {
	session, renewalErrChan, err := rm.Store.NewSession(sessionName(id), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to get consul session. Halting \n%v\n", err)
		os.Exit(1)
	}

	go func(errChan chan error) {
		select {
		case <-errChan:
			fmt.Fprintf(os.Stderr, "Got renewal error, proceeding without a lock\n%v\n", err)
		default:
		}
	}(renewalErrChan)

	rcLock, err := rm.RCStore.LockForMutation(id, session)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to lock RC for mutation. Halting.\n %v", err)
		os.Exit(1)
	}
	defer rcLock.Unlock()

	err = rm.RCStore.Disable(id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not disable RC. %s \n %v", id, err)
		os.Exit(1)
	}

	err = rm.deletePod()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to delete pod. Please re-run this command to clean up. \n%v", err)
		os.Exit(1)
	}

	err = rm.RCStore.AddDesiredReplicas(id, -1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to decrement RC count: %v", err)
		os.Exit(1)
	}

	err = rm.RCStore.Enable(id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not enable RC. %s \n %v", id, err)
		os.Exit(1)
	}

	if err = session.Destroy(); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to destroy consul session. %v", err)
	}

	return nil
}

func (rm *P2RM) checkForManagingReplicationController() (bool, fields.ID, error) {
	podLabels, err := rm.Labeler.GetLabels(labels.POD, rm.LabelID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to check node for labels: %v\n", err)
		return false, "", err
	}

	if podLabels.Labels.Has(rc.RCIDLabel) {
		return true, fields.ID(podLabels.Labels.Get(rc.RCIDLabel)), nil
	}

	return false, "", nil
}

func (rm *P2RM) deletePod() error {
	_, err := rm.Store.DeletePod(kp.INTENT_TREE, rm.NodeName, types.PodID(rm.PodName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to remove pod: %v", err)
		return err
	}

	err = rm.Labeler.RemoveAllLabels(labels.POD, rm.LabelID)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"node is partially deleted. re-run command to finish deleting\n"+
				"unable to remove pod labels: %v",
			err,
		)
		return err
	}
	return nil
}
