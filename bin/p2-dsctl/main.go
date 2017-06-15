package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/cli"
	"github.com/square/p2/pkg/ds"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/dsstore"
	"github.com/square/p2/pkg/store/consul/flags"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	CmdCreate       = "create"
	CmdGet          = "get"
	CmdList         = "list"
	CmdEnable       = "enable"
	CmdDisable      = "disable"
	CmdDelete       = "delete"
	CmdUpdate       = "update"
	CmdTestSelector = "test-selector"

	TimeoutNotSpecified = time.Duration(-1)
)

func flagUsed(marker *bool) kingpin.Action {
	return func(*kingpin.ParseContext) error {
		*marker = true
		return nil
	}
}

var (
	cmdCreate        = kingpin.Command(CmdCreate, "Create a daemon set.")
	createSelector   = cmdCreate.Flag("selector", "The node selector, uses the same syntax as the test-selector command").Required().String()
	createManifest   = cmdCreate.Flag("manifest", "Path to signed manifest file").Required().String()
	createMinHealth  = cmdCreate.Flag("minhealth", "The minimum health of the daemon set").Required().String()
	createName       = cmdCreate.Flag("name", "The cluster name (ie. staging, production)").Required().String()
	createTimeout    = cmdCreate.Flag("timeout", "Non-zero timeout for replicating hosts. e.g. 1m2s for 1 minute and 2 seconds").Required().Duration()
	createEverywhere = cmdCreate.Flag("everywhere", "Sets selector to match everything regardless of its value").Bool()

	cmdGet = kingpin.Command(CmdGet, "Show a daemon set.")
	getID  = cmdGet.Arg("id", "The uuid for the daemon set").Required().String()

	cmdList  = kingpin.Command(CmdList, "List daemon sets.")
	listPod  = cmdList.Flag("pod", "The pod ID of the daemon set").String()
	listJSON = cmdList.Flag("json", "output the entire JSON object of each daemon set").Short('j').Bool()

	cmdEnable = kingpin.Command(CmdEnable, "Enable daemon set.")
	enableID  = cmdEnable.Arg("id", "The uuid for the daemon set").Required().String()

	cmdDisable = kingpin.Command(CmdDisable, "Disable daemon set.")
	disableID  = cmdDisable.Arg("id", "The uuid for the daemon set").Required().String()

	cmdDelete = kingpin.Command(CmdDelete, "Delete daemon set.")
	deleteID  = cmdDelete.Arg("id", "The uuid for the daemon set").Required().String()

	cmdUpdate           = kingpin.Command(CmdUpdate, "Update a daemon set.")
	updateID            = cmdUpdate.Arg("id", "The uuid for the daemon set").Required().String()
	updateSelectorGiven = false
	updateSelector      = cmdUpdate.Flag("selector", "The node selector, uses the same syntax as the test-selector command").Action(flagUsed(&updateSelectorGiven)).String()
	updateManifest      = cmdUpdate.Flag("manifest", "Path to signed manifest file").String()
	updateMinHealth     = cmdUpdate.Flag("minhealth", "The minimum health of the daemon set").String()
	updateName          = cmdUpdate.Flag("name", "The cluster name (ie. staging, production)").String()
	updateTimeout       = cmdUpdate.Flag("timeout", "Non-zero timeout for replicating hosts. e.g. 1m2s for 1 minute and 2 seconds").Default(TimeoutNotSpecified.String()).Duration()
	updateEverywhere    = cmdUpdate.Flag("everywhere", "Sets selector to match everything regardless of its value").Bool()

	cmdTestSelector = kingpin.Command(CmdTestSelector, `
		This will output the hosts that match the selector,
		The selector string uses same syntax as the kubernetes selectors without flags.
		An example command is:
		p2-dsctl test-selector --selector SELECTOR`,
	)
	testSelectorString     = cmdTestSelector.Flag("selector", "The raw selector represented as a string").String()
	testSelectorEverywhere = cmdTestSelector.Flag("everywhere", "Sets selector to match everything regardless of its value").Bool()
)

func main() {
	cmd, consulOpts, applicator := flags.ParseWithConsulOptions()
	client := consul.NewConsulClient(consulOpts)
	logger := logging.NewLogger(logrus.Fields{})
	dsstore := dsstore.NewConsul(client, 3, &logger)

	switch cmd {
	case CmdCreate:
		minHealth, err := strconv.Atoi(*createMinHealth)
		if err != nil {
			log.Fatalf("Invalid value for minimum health, expected integer: %v", err)
		}
		name := ds_fields.ClusterName(*createName)

		manifest, err := manifest.FromPath(*createManifest)
		if err != nil {
			log.Fatalf("%s", err)
		}

		podID := manifest.ID()

		if *createTimeout <= time.Duration(0) {
			log.Fatalf("Timeout must be a positive non-zero value, got '%v'", *createTimeout)
		}

		selectorString := *createSelector
		if *createEverywhere {
			selectorString = klabels.Everything().String()
		} else if selectorString == "" {
			selectorString = labels.Nothing().String()
			log.Fatal("Explicit everything selector not allowed, please use the --everwhere flag")
		}
		selector, err := parseNodeSelectorWithPrompt(labels.Nothing(), selectorString, applicator)
		if err != nil {
			log.Fatalf("Error occurred: %v", err)
		}

		if err = confirmMinheathForSelector(minHealth, selector, applicator); err != nil {
			log.Fatalf("Error occurred: %v", err)
		}

		ctx, cancelFunc := transaction.New(context.Background())
		defer cancelFunc()
		newDS, err := dsstore.Create(ctx, manifest, minHealth, name, selector, podID, *createTimeout)
		if err != nil {
			log.Fatalf("err: %v", err)
		}

		fmt.Fprintf(os.Stderr, "checking that that the given selector doesn't overlap nodes with other %s daemon sets\n", manifest.ID())

		conflictingDS, isContending, err := ds.DSContends(&newDS, scheduler.NewApplicatorScheduler(applicator), dsstore)
		if err != nil {
			log.Fatalf("failed to check for daemon set overlap: %s", err)
		}

		if isContending {
			log.Fatalf("daemon set %q contends with the given selector, correct this before re-attempting", conflictingDS.ID)
		}

		err = transaction.MustCommit(ctx, client.KV())
		if err != nil {
			log.Fatalf("err: %v", err)
		}

		fmt.Printf("%v has been created in consul", newDS.ID)
		fmt.Println()

	case CmdGet:
		id := ds_fields.ID(*getID)
		ds, _, err := dsstore.Get(id)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
		bytes, err := json.Marshal(ds)
		if err != nil {
			logger.WithError(err).Fatalln("Unable to marshal daemon set as JSON")
		}
		fmt.Printf("%s", bytes)

	case CmdList:
		dsList, err := dsstore.List()
		if err != nil {
			log.Fatalf("err: %v", err)
		}
		podID := types.PodID(*listPod)
		for _, ds := range dsList {
			if *listPod == "" || podID == ds.PodID {
				if *listJSON {
					bytes, err := json.Marshal(ds)
					if err != nil {
						log.Fatalf("could not marshal %s as json: %s", ds.ID, err)
					}

					fmt.Println(string(bytes))
				} else {
					fmt.Printf("%s/%s:%s\n", ds.PodID, ds.Name, ds.ID)
				}
			}
		}

	case CmdEnable:
		id := ds_fields.ID(*enableID)

		mutator := func(ds ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
			if !ds.Disabled {
				return ds, util.Errorf("Daemon set has already been enabled")
			}
			ds.Disabled = false
			return ds, nil
		}

		_, err := dsstore.MutateDS(id, mutator)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
		fmt.Printf("The daemon set '%s' has been successfully enabled in consul", id.String())
		fmt.Println()

	case CmdDisable:
		id := ds_fields.ID(*disableID)

		mutator := func(ds ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
			if ds.Disabled {
				return ds, util.Errorf("Daemon set has already been disabled")
			}
			ds.Disabled = true
			return ds, nil
		}

		_, err := dsstore.MutateDS(id, mutator)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
		fmt.Printf("The daemon set '%s' has been successfully disabled in consul", id.String())
		fmt.Println()

	case CmdDelete:
		id := ds_fields.ID(*deleteID)
		err := dsstore.Delete(id)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
		fmt.Printf("The daemon set '%s' has been successfully deleted from consul", id.String())
		fmt.Println()

	case CmdUpdate:
		id := ds_fields.ID(*updateID)

		mutator := func(ds ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
			changed := false
			if *updateMinHealth != "" {
				minHealth, err := strconv.Atoi(*updateMinHealth)
				if err != nil {
					log.Fatalf("Invalid value for minimum health, expected integer")
				}
				if ds.MinHealth != minHealth {
					changed = true
					ds.MinHealth = minHealth
				}
			}
			if *updateName != "" {
				name := ds_fields.ClusterName(*updateName)
				if ds.Name != name {
					changed = true
					ds.Name = name
				}
			}

			if *updateTimeout != TimeoutNotSpecified {
				if *updateTimeout <= time.Duration(0) {
					return ds, util.Errorf("Timeout must be a positive non-zero value, got '%v'", *createTimeout)
				}
				if ds.Timeout != *updateTimeout {
					changed = true
					ds.Timeout = *updateTimeout
				}
			}
			if *updateManifest != "" {
				manifest, err := manifest.FromPath(*updateManifest)
				if err != nil {
					return ds, util.Errorf("%s", err)
				}

				if manifest.ID() != ds.PodID {
					return ds, util.Errorf("Manifest ID of %s does not match daemon set's pod ID (%s)", manifest.ID(), ds.PodID)
				}

				dsSHA, err := ds.Manifest.SHA()
				if err != nil {
					return ds, util.Errorf("Unable to get SHA from consul daemon set manifest: %v", err)
				}
				newSHA, err := manifest.SHA()
				if err != nil {
					return ds, util.Errorf("Unable to get SHA from new manifest: %v", err)
				}
				if dsSHA != newSHA {
					changed = true
					ds.Manifest = manifest
				}
			}
			if updateSelectorGiven {
				selectorString := *updateSelector
				if *updateEverywhere {
					selectorString = klabels.Everything().String()
				} else if selectorString == "" {
					return ds, util.Errorf("Explicit everything selector not allowed, please use the --everwhere flag")
				}
				selector, err := parseNodeSelectorWithPrompt(ds.NodeSelector, selectorString, applicator)
				if err != nil {
					return ds, util.Errorf("Error occurred: %v", err)
				}
				if ds.NodeSelector.String() != selector.String() {
					changed = true
					ds.NodeSelector = selector
				}
			}

			if !changed {
				return ds, util.Errorf("No changes were made")
			}

			if updateSelectorGiven || *updateMinHealth != "" {
				if err := confirmMinheathForSelector(ds.MinHealth, ds.NodeSelector, applicator); err != nil {
					return ds, util.Errorf("Error occurred: %v", err)
				}
			}

			return ds, nil
		}

		_, err := dsstore.MutateDS(id, mutator)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
		fmt.Printf("The daemon set '%s' has been successfully updated in consul", id.String())
		fmt.Println()

	case CmdTestSelector:
		selectorString := *testSelectorString
		if *testSelectorEverywhere {
			selectorString = klabels.Everything().String()
		} else if selectorString == "" {
			fmt.Println("Explicit everything selector not allowed, please use the --everwhere flag")
		}
		selector, err := parseNodeSelector(selectorString)
		if err != nil {
			log.Fatalf("Error occurred: %v", err)
		}

		matches, err := applicator.GetMatches(selector, labels.NODE)
		if err != nil {
			log.Fatalf("Error getting matching labels: %v", err)
		}
		fmt.Println(matches)

	default:
		log.Fatalf("Unrecognized command %v", cmd)
	}
}

func parseNodeSelectorWithPrompt(
	oldSelector klabels.Selector,
	newSelectorString string,
	applicator labels.ApplicatorWithoutWatches,
) (klabels.Selector, error) {
	newSelector, err := parseNodeSelector(newSelectorString)
	if err != nil {
		return newSelector, err
	}
	if oldSelector.String() == newSelector.String() {
		return newSelector, nil
	}

	newNodeLabels, err := applicator.GetMatches(newSelector, labels.NODE)
	if err != nil {
		return newSelector, util.Errorf("Error getting matching labels: %v", err)
	}

	oldNodeLabels, err := applicator.GetMatches(oldSelector, labels.NODE)
	if err != nil {
		return newSelector, util.Errorf("Error getting matching labels: %v", err)
	}

	toRemove, toAdd := makeNodeChanges(oldNodeLabels, newNodeLabels)

	fmt.Printf("Changing deployment from '%v' to '%v':\n", oldSelector.String(), newSelectorString)
	fmt.Printf("Removing:%9s hosts %s\n", fmt.Sprintf("-%v", len(toRemove)), toRemove)
	fmt.Printf("Adding:  %9s hosts %s\n", fmt.Sprintf("+%v", len(toAdd)), toAdd)
	fmt.Println("Continue?")
	if !cli.Confirm() {
		return newSelector, util.Errorf("User cancelled")
	}

	return newSelector, nil
}

func confirmMinheathForSelector(minHealth int, selector klabels.Selector, applicator labels.ApplicatorWithoutWatches) error {
	matches, err := applicator.GetMatches(selector, labels.NODE)
	if err != nil {
		return err
	}
	if len(matches) < minHealth {
		fmt.Printf("Your selector matches %d nodes but your minhealth is set to only %d, this daemon set will not replicate. Continue?\n", len(matches), minHealth)
		if !cli.Confirm() {
			return util.Errorf("User cancelled")
		}
	}
	return nil
}

func parseNodeSelector(selectorString string) (klabels.Selector, error) {
	selector, err := klabels.Parse(selectorString)
	if err != nil {
		return selector, util.Errorf("Malformed selector: %v", err)
	}
	return selector, nil
}

// Returns nodes to be removed and nodes to be added
func makeNodeChanges(oldNodeLabels []labels.Labeled, newNodeLabels []labels.Labeled) ([]types.NodeName, []types.NodeName) {
	var oldNodeNames []types.NodeName
	var newNodeNames []types.NodeName

	for _, node := range oldNodeLabels {
		oldNodeNames = append(oldNodeNames, types.NodeName(node.ID))
	}

	for _, node := range newNodeLabels {
		newNodeNames = append(newNodeNames, types.NodeName(node.ID))
	}

	toRemove := types.NewNodeSet(oldNodeNames...).Difference(types.NewNodeSet(newNodeNames...)).ListNodes()
	toAdd := types.NewNodeSet(newNodeNames...).Difference(types.NewNodeSet(oldNodeNames...)).ListNodes()

	return toRemove, toAdd
}
