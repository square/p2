// p2-rm is a command line tool for removing a pods and its labels.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
)

// Command line flags
var (
	podName  = kingpin.Arg("pod", "The names of the pod to be removed").Required().String()
	nodeName = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(opts)
	store := kp.NewConsulStore(client)
	labeler := labels.NewConsulApplicator(client, 3)

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error getting hostname. use --node to specify a node: %v\n", err)
			os.Exit(1)
		}
		*nodeName = hostname
	}

	// If the node is managed by a controller, let the user know. Changes to the
	// controller might be needed.
	labelID := path.Join(*nodeName, *podName)
	podLabels, err := labeler.GetLabels(labels.POD, labelID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to check node for labels: %v\n", err)
		os.Exit(1)
	}
	if podLabels.Labels.Has(rc.RCIDLabel) {
		fmt.Fprintf(
			os.Stderr,
			"warning: %s is managed by replication controller: %v\n"+
				"         To reduce the controller's capacity, use \"p2-rctl set-replicas\"\n"+
				"continue [y|n]? ",
			labelID,
			podLabels.Labels.Get(rc.RCIDLabel),
		)
		input, err := bufio.NewReader(os.Stdin).ReadString('\n')
		answer := (strings.ToLower(strings.TrimSpace(input)) + "n")[0]
		if err != nil || answer != 'y' {
			fmt.Fprintf(os.Stderr, "aborting\n")
			os.Exit(1)
		}
	}

	_, err = store.DeletePod(kp.INTENT_TREE, *nodeName, types.PodID(*podName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to remove pod: %v", err)
		os.Exit(1)
	}
	err = labeler.RemoveAllLabels(labels.POD, labelID)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"node is partially deleted. re-run command to finish deleting\n"+
				"unable to remove pod labels: %v",
			err,
		)
		os.Exit(2)
	}
	fmt.Printf("%s: successfully removed %s\n", *nodeName, *podName)
}
