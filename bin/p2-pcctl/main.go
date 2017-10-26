package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/user"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/cli"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pc/control"
	"github.com/square/p2/pkg/pc/fields"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/flags"
	"github.com/square/p2/pkg/store/consul/pcstore"
	"github.com/square/p2/pkg/types"
	klabels "k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/util/sets"
)

const (
	cmdCreateText            = "create"
	cmdGetText               = "get"
	cmdDeleteText            = "delete"
	cmdUpdateAnnotationsText = "update-annotations"
	cmdUpdateSelectorText    = "update-selector"
	cmdListText              = "list"
)

// "create" command and flags
var (
	cmdCreate         = kingpin.Command(cmdCreateText, "Create a pod cluster. ")
	createPodID       = cmdCreate.Flag("pod", "The pod ID on the pod cluster").Required().String()
	createAZ          = cmdCreate.Flag("az", "The availability zone of the pod cluster").Required().String()
	createName        = cmdCreate.Flag("name", "The cluster name (ie. staging, production)").Required().String()
	createAnnotations = cmdCreate.Flag("annotations", "Complete set of annotations - must parse as JSON!").String()
	createStrategy    = cmdCreate.Flag("allocation-strategy", "The allocation strategy to use for RCs created for this pod cluster").Required().Enum(rc_fields.StaticStrategy.String(), rc_fields.DynamicStrategy.String())
)

// "get" command and flags
var (
	cmdGet   = kingpin.Command(cmdGetText, "Show a pod cluster. ")
	getPodID = cmdGet.Flag("pod", "The pod ID on the pod cluster").String()
	getAZ    = cmdGet.Flag("az", "The availability zone of the pod cluster").String()
	getName  = cmdGet.Flag("name", "The cluster name (ie. staging, production)").String()
	getID    = cmdGet.Flag("id", "The cluster UUID. This option is mutually exclusive with pod,az,name").String()
)

// "delete" command and flags
var (
	cmdDelete   = kingpin.Command(cmdDeleteText, "Delete a pod cluster. ")
	deletePodID = cmdDelete.Flag("pod", "The pod ID on the pod cluster").String()
	deleteAZ    = cmdDelete.Flag("az", "The availability zone of the pod cluster").String()
	deleteName  = cmdDelete.Flag("name", "The cluster name (ie. staging, production)").String()
	deleteID    = cmdDelete.Flag("id", "The cluster UUID. This option is mutually exclusive with pod,az,name").String()
)

// "update-annotations" command and flags"
var (
	cmdUpdateAnnotations = kingpin.Command(cmdUpdateAnnotationsText, "Update a pod cluster's annotations.")

	// these flags identify the pod cluster to update
	updateAnnotationsPodID = cmdUpdateAnnotations.Flag("pod", "The pod ID on the pod cluster that should be updated.").String()
	updateAnnotationsAZ    = cmdUpdateAnnotations.Flag("az", "The availability zone of the pod cluster that should be updated").String()
	updateAnnotationsName  = cmdUpdateAnnotations.Flag("name", "The cluster name (ie. staging, production) for the pod cluster that should be updated.").String()
	updateAnnotationsID    = cmdUpdateAnnotations.Flag("id", "The UUID of the pod cluster that should be updated. This option is mutually exclusive with pod,az,name").String()

	// this flag specifies the annotations to update.
	updateAnnotations = cmdUpdateAnnotations.Flag("annotations", "JSON string representing the complete annotations that should be applied to the pod cluster. Annotations will not be updated if this flag is unspecified.").Required().String()
)

// "update-selector" command and flags
var (
	cmdUpdateSelector = kingpin.Command(cmdUpdateSelectorText, "Update a pod cluster's pod selector. A diff of pod cluster membership and a confirmation prompt will be shown before submitting the update")

	// these flags identify the pod cluster to update
	updateSelectorPodID = cmdUpdateSelector.Flag("pod", "The pod ID on the pod cluster that should be updated.").String()
	updateSelectorAZ    = cmdUpdateSelector.Flag("az", "The availability zone of the pod cluster that should be updated").String()
	updateSelectorName  = cmdUpdateSelector.Flag("name", "The cluster name (ie. staging, production) for the pod cluster that should be updated.").String()
	updateSelectorID    = cmdUpdateSelector.Flag("id", "The UUID of the pod cluster that should be updated. This option is mutually exclusive with pod,az,name").String()

	// this flag specifies the selector to update the pod cluster with
	updateSelector = cmdUpdateSelector.Flag("selector", "The label selector to use for the pod cluster's pod selector (e.g. \"pod_id=foo,availability_zone=bar\")").Required().String()
)

// "list" command
var (
	cmdList = kingpin.Command(cmdListText, "Lists pod clusters. ")
)

func main() {
	cmd, consulOpts, labeler := flags.ParseWithConsulOptions()
	client := consul.NewConsulClient(consulOpts)
	kv := consul.NewConsulStore(client)
	logger := logging.NewLogger(logrus.Fields{})
	applicator := labels.NewConsulApplicator(client, 0, 1*time.Minute)
	pcstore := pcstore.NewConsul(client, labeler, labels.DefaultAggregationRate, applicator, &logger)

	switch cmd {
	case cmdCreateText:
		az := fields.AvailabilityZone(*createAZ)
		cn := fields.ClusterName(*createName)
		podID := types.PodID(*createPodID)
		selector := defaultSelector(az, cn, podID)
		strategy := rc_fields.Strategy(*createStrategy)
		pccontrol := control.NewPodCluster(az, cn, podID, pcstore, selector, strategy)

		annotations := *createAnnotations
		var parsedAnnotations map[string]interface{}
		err := json.Unmarshal([]byte(annotations), &parsedAnnotations)
		if err != nil {
			log.Fatalf("could not parse json: %v", err)
		}

		session, _, err := kv.NewSession(fmt.Sprintf("pcctl-%s", currentUserName()), nil)
		if err != nil {
			log.Fatalf("Could not create session: %s", err)
		}

		_, err = pccontrol.Create(parsedAnnotations, session)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
	case cmdGetText:
		az := fields.AvailabilityZone(*getAZ)
		cn := fields.ClusterName(*getName)
		podID := types.PodID(*getPodID)
		pcID := fields.ID(*getID)

		var pccontrol *control.PodCluster
		if pcID != "" {
			pccontrol = control.NewPodClusterFromID(pcID, pcstore)
		} else if az != "" && cn != "" && podID != "" {
			selector := defaultSelector(az, cn, podID)
			pccontrol = control.NewPodCluster(az, cn, podID, pcstore, selector, "")
		} else {
			log.Fatalf("Expected one of: pcID or (pod,az,name)")
		}

		pc, err := pccontrol.Get()
		if err != nil {
			log.Fatalf("Caught error while fetching pod cluster: %v", err)
		}

		bytes, err := json.Marshal(pc)
		if err != nil {
			logger.WithError(err).Fatalln("Unable to marshal PC as JSON")
		}
		fmt.Printf("%s", bytes)
	case cmdDeleteText:
		az := fields.AvailabilityZone(*deleteAZ)
		cn := fields.ClusterName(*deleteName)
		podID := types.PodID(*deletePodID)
		pcID := fields.ID(*deleteID)

		var pccontrol *control.PodCluster
		if pcID != "" {
			pccontrol = control.NewPodClusterFromID(pcID, pcstore)
		} else if az != "" && cn != "" && podID != "" {
			selector := defaultSelector(az, cn, podID)
			pccontrol = control.NewPodCluster(az, cn, podID, pcstore, selector, "")
		} else {
			log.Fatalf("Expected one of: pcID or (pod,az,name)")
		}

		errors := pccontrol.Delete()
		if len(errors) >= 1 {
			for _, err := range errors {
				_, _ = os.Stderr.Write([]byte(fmt.Sprintf("Failed to delete one pod cluster matching arguments. Error:\n %s\n", err.Error())))
			}
			os.Exit(1)
		}
	case cmdUpdateAnnotationsText:
		az := fields.AvailabilityZone(*updateAnnotationsAZ)
		cn := fields.ClusterName(*updateAnnotationsName)
		podID := types.PodID(*updateAnnotationsPodID)
		pcID := fields.ID(*updateAnnotationsID)

		var pccontrol *control.PodCluster
		if pcID != "" {
			pccontrol = control.NewPodClusterFromID(pcID, pcstore)
		} else if az != "" && cn != "" && podID != "" {
			selector := defaultSelector(az, cn, podID)
			pccontrol = control.NewPodCluster(az, cn, podID, pcstore, selector, "")
		} else {
			log.Fatalf("Expected one of: pcID or (pod,az,name)")
		}

		var annotations fields.Annotations
		err := json.Unmarshal([]byte(*updateAnnotations), &annotations)
		if err != nil {
			_, _ = os.Stderr.Write([]byte(fmt.Sprintf("Annotations are invalid JSON. Err follows:\n%v", err)))
			os.Exit(1)
		}

		pc, err := pccontrol.UpdateAnnotations(annotations)
		if err != nil {
			log.Fatalf("Error during PodCluster update: %v\n%v", err, pc)
			os.Exit(1)
		}
		bytes, err := json.Marshal(pc)
		if err != nil {
			log.Fatalf("Update succeeded, but error during displaying PC: %v\n%+v", err, pc)
			os.Exit(1)
		}
		fmt.Printf("%s", bytes)
	case cmdUpdateSelectorText:
		az := fields.AvailabilityZone(*updateSelectorAZ)
		cn := fields.ClusterName(*updateSelectorName)
		podID := types.PodID(*updateSelectorPodID)
		pcID := fields.ID(*updateSelectorID)

		// no pccontrol for this one because we want to show a diff with CAS guarantees which is CLI specific
		if pcID == "" {
			if az == "" || cn == "" || podID == "" {
				log.Fatal("you must specify a pod cluster ID or all of pod id, availability zone, and cluster name")
			} else {
				pcs, err := pcstore.FindWhereLabeled(podID, az, cn)
				if err != nil {
					log.Fatalf("could not search for pod cluster matching (%s, %s, %s): %s", podID, az, cn, err)
				}

				if len(pcs) == 0 {
					log.Fatalf("no pod cluster matched query (%s, %s, %s)", podID, az, cn)
				}

				if len(pcs) > 1 {
					// this should be impossible because of creation validation
					log.Fatalf("multiple pod clusters matched query (%s, %s, %s)", podID, az, cn)
				}

				pcID = pcs[0].ID
			}
		}

		newSelector, err := klabels.Parse(*updateSelector)
		if err != nil {
			log.Fatalf("could not parse %q as label selector: %s", *updateSelector, err)
		}

		// Do the update within MutatePC. That way we know the update
		// won't apply if anything about the pod cluster changes while
		// we're showing the operator the label query diff
		mutator := func(pc fields.PodCluster) (fields.PodCluster, error) {
			oldSelector := pc.PodSelector

			err = confirmDiff(oldSelector, newSelector, applicator)
			if err != nil {
				log.Fatal(err)
			}

			pc.PodSelector = newSelector
			return pc, nil
		}

		_, err = pcstore.MutatePC(pcID, mutator)
		if err != nil {
			log.Fatalf("could not apply pod cluster selector update: %s", err)
		}
	case cmdListText:
		pcs, err := pcstore.List()
		if err != nil {
			_, _ = os.Stderr.Write([]byte(fmt.Sprintf("Could not list pcs. Err follows:\n%v", err)))
			os.Exit(1)
		}

		bytes, err := json.Marshal(pcs)
		if err != nil {
			_, _ = os.Stderr.Write([]byte(fmt.Sprintf("Could not marshal pc list. Err follows:\n%v", err)))
			os.Exit(1)
		}
		fmt.Printf("%s", bytes)
	default:
		log.Fatalf("Unrecognized command %v", cmd)
	}
}

func defaultSelector(az fields.AvailabilityZone, cn fields.ClusterName, podID types.PodID) klabels.Selector {
	return klabels.Everything().
		Add(fields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(fields.ClusterNameLabel, klabels.EqualsOperator, []string{cn.String()})
}

func currentUserName() string {
	username := "unknown user"

	if user, err := user.Current(); err == nil {
		username = user.Username
	}
	return username
}

type Matcher interface {
	GetMatches(selector klabels.Selector, labelType labels.Type) ([]labels.Labeled, error)
}

func confirmDiff(oldSelector klabels.Selector, newSelector klabels.Selector, matcher Matcher) error {
	oldPods, err := matcher.GetMatches(oldSelector, labels.POD)
	if err != nil {
		return fmt.Errorf("could not query pods using old selector: %s", err)
	}

	newPods, err := matcher.GetMatches(newSelector, labels.POD)
	if err != nil {
		return fmt.Errorf("could not query pods using new selector: %s", err)
	}

	addedPods, subtractedPods := computeDiff(oldPods, newPods)
	if len(subtractedPods) != 0 {
		fmt.Printf("WARNING: The following nodes will no longer be members of the pod cluster: %s\n", subtractedPods)
	}

	if len(addedPods) != 0 {
		fmt.Printf("The following nodes will be added as members of the pod cluster: %s\n", addedPods)
	}

	if len(addedPods) == 0 && len(subtractedPods) == 0 {
		fmt.Println("There will be no changes to pod cluster membership based on this selector change")
	}

	fmt.Println("Do you wish to proceed?")
	confirmed := cli.Confirm()
	if !confirmed {
		return errors.New("aborted")
	}

	return nil
}

func computeDiff(oldPods []labels.Labeled, newPods []labels.Labeled) ([]string, []string) {
	var oldStrings, newStrings []string
	for _, pod := range oldPods {
		oldStrings = append(oldStrings, pod.ID)
	}

	for _, pod := range newPods {
		newStrings = append(newStrings, pod.ID)
	}

	oldSet := sets.NewString(oldStrings...)
	newSet := sets.NewString(newStrings...)

	added := newSet.Difference(oldSet)
	removed := oldSet.Difference(newSet)

	return added.List(), removed.List()
}
