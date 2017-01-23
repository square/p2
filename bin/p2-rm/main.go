// p2-rm is a command line tool for removing a pods and its labels.
package main

import (
	"fmt"
	"os"
	"os/user"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/flags"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
)

// Command line flags
var (
	podName      = kingpin.Arg("pod", "The names of the pod to be removed").Required().String()
	nodeName     = kingpin.Flag("node", "The node to unschedule the pod from. Uses the hostname by default. Only applies to \"legacy\" pods.").String()
	podUniqueKey = kingpin.Flag("pod-unique-key", "The pod unique key to unschedule. Only applies to \"uuid\" pods. Cannot be used with --node").Short('k').String()
	deallocation = kingpin.Flag("deallocate", "Specifies that we are deallocating this pod on this node. Using this switch will mutate the desired_replicas value on a managing RC, if one exists.").Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts, labeler := flags.ParseWithConsulOptions()

	consulClient := consul.NewConsulClient(opts)

	err := handlePodRemoval(consulClient, labeler)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func handlePodRemoval(consulClient consulutil.ConsulClient, labeler labels.ApplicatorWithoutWatches) error {
	var rm *P2RM
	if *podUniqueKey != "" {
		rm = NewUUIDP2RM(consulClient, types.PodUniqueKey(*podUniqueKey), types.PodID(*podName), labeler)
	} else {
		if *nodeName == "" {
			hostname, err := os.Hostname()
			if err != nil {
				return fmt.Errorf("error getting hostname. use --node to specify a node: %v\n", err)
			}
			*nodeName = hostname
		}

		rm = NewLegacyP2RM(consulClient, types.PodID(*podName), types.NodeName(*nodeName), labeler)
	}

	podIsManagedByRC, rcID, err := rm.checkForManagingReplicationController()
	if err != nil {
		return err
	}

	if !podIsManagedByRC {
		err = rm.deletePod()
		if err != nil {
			return err
		}
	}

	if podIsManagedByRC && !*deallocation {
		return fmt.Errorf("error: %s is managed by replication controller: %s\n"+
			"It's possible you meant you deallocate this pod on this node. If so, please confirm your intention with --deallocate\n", *nodeName, rcID)
	}

	if podIsManagedByRC && *deallocation {
		err = rm.decrementDesiredCount(rcID)
		if err != nil {
			return fmt.Errorf("Encountered error deallocating from the RC %s. You may attempt this command again or use `p2-rctl` to cleanup manually.\n%v",
				rcID,
				err)
		}
	}

	if rm.NodeName != "" {
		fmt.Printf("%s: successfully removed %s\n", rm.NodeName, rm.PodID)
	} else {
		fmt.Printf("successfully removed %s-%s\n", rm.PodID, rm.PodUniqueKey)
	}
	return nil
}

func sessionName(rcID fields.ID) string {
	currentUser, err := user.Current()
	username := "unknown"
	if err == nil {
		username = currentUser.Username
	}

	return fmt.Sprintf("p2-rm:user:%s:rcID:%s", username, rcID)
}
