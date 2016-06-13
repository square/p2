package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/user"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/kp/pcstore"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pc/control"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
	"k8s.io/kubernetes/pkg/labels"
)

const (
	CmdCreate = "create"
	CmdGet    = "get"
	CmdDelete = "delete"
	CmdUpdate = "update"
)

var (
	cmdCreate         = kingpin.Command(CmdCreate, "Create a pod cluster. ")
	createPodID       = cmdCreate.Flag("pod", "The pod ID on the pod cluster").Required().String()
	createAZ          = cmdCreate.Flag("az", "The availability zone of the pod cluster").Required().String()
	createName        = cmdCreate.Flag("name", "The cluster name (ie. staging, production)").Required().String()
	createSelector    = cmdCreate.Flag("selector", "A pod selector to use for this pod cluster. This will be generated from other arguments if not provided").String()
	createAnnotations = cmdCreate.Flag("annotations", "Complete set of annotations - must parse as JSON!").String()

	cmdGet   = kingpin.Command(CmdGet, "Show a pod cluster. ")
	getPodID = cmdGet.Flag("pod", "The pod ID on the pod cluster").Required().String()
	getAZ    = cmdGet.Flag("az", "The availability zone of the pod cluster").Required().String()
	getName  = cmdGet.Flag("name", "The cluster name (ie. staging, production)").Required().String()

	cmdDelete   = kingpin.Command(CmdDelete, "Delete a pod cluster. ")
	deletePodID = cmdDelete.Flag("pod", "The pod ID on the pod cluster").Required().String()
	deleteAZ    = cmdDelete.Flag("az", "The availability zone of the pod cluster").Required().String()
	deleteName  = cmdDelete.Flag("name", "The cluster name (ie. staging, production)").Required().String()

	cmdUpdate         = kingpin.Command(CmdUpdate, "Update a pod cluster. ")
	updatePodID       = cmdUpdate.Flag("pod", "The pod ID on the pod cluster").Required().String()
	updateAZ          = cmdUpdate.Flag("az", "The availability zone of the pod cluster").Required().String()
	updateName        = cmdUpdate.Flag("name", "The cluster name (ie. staging, production)").Required().String()
	updateAnnotations = cmdUpdate.Flag("annotations", "JSON string representing the complete update ").Required().String()
)

func main() {
	cmd, consulOpts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(consulOpts)
	kv := kp.NewConsulStore(client)
	logger := logging.NewLogger(logrus.Fields{})
	pcstore := pcstore.NewConsul(client, 3, &logger)

	switch cmd {
	case CmdCreate:
		az := fields.AvailabilityZone(*createAZ)
		cn := fields.ClusterName(*createName)
		podID := types.PodID(*createPodID)
		selector := selectorFrom(az, cn, podID)
		pccontrol := control.NewPodCluster(az, cn, podID, pcstore, selector, kv.NewUnmanagedSession("pcctl", currentUserName()))

		annotations := *createAnnotations
		var parsedAnnotations map[string]interface{}
		err := json.Unmarshal([]byte(annotations), &parsedAnnotations)
		if err != nil {
			log.Fatalf("could not parse json: %v", err)
		}
		_, err = pccontrol.Create(parsedAnnotations)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
	case CmdGet:
		az := fields.AvailabilityZone(*getAZ)
		cn := fields.ClusterName(*getName)
		podID := types.PodID(*getPodID)
		selector := selectorFrom(az, cn, podID)

		pccontrol := control.NewPodCluster(az, cn, podID, pcstore, selector, kv.NewUnmanagedSession("pcctl", currentUserName()))
		pc, err := pccontrol.Get()
		if err != nil {
			log.Fatalf("Caught error while fetching pod cluster: %v", err)
		}

		bytes, err := json.Marshal(pc)
		if err != nil {
			logger.WithError(err).Fatalln("Unable to marshal PC as JSON")
		}
		fmt.Printf("%s", bytes)
	case CmdDelete:
		az := fields.AvailabilityZone(*deleteAZ)
		cn := fields.ClusterName(*deleteName)
		podID := types.PodID(*deletePodID)
		selector := selectorFrom(az, cn, podID)
		pccontrol := control.NewPodCluster(az, cn, podID, pcstore, selector, kv.NewUnmanagedSession("pcctl", currentUserName()))
		errors := pccontrol.Delete()
		if len(errors) >= 1 {
			for _, err := range errors {
				_, _ = os.Stderr.Write([]byte(fmt.Sprintf("Failed to delete one pod cluster matching arguments. Error:\n %s\n", err.Error())))
			}
			os.Exit(1)
		}
	case CmdUpdate:
		az := fields.AvailabilityZone(*updateAZ)
		cn := fields.ClusterName(*updateName)
		podID := types.PodID(*updatePodID)
		selector := selectorFrom(az, cn, podID)
		pccontrol := control.NewPodCluster(az, cn, podID, pcstore, selector, kv.NewUnmanagedSession("pcctl", currentUserName()))
		var annotations fields.Annotations
		err := json.Unmarshal([]byte(*updateAnnotations), &annotations)
		if err != nil {
			_, _ = os.Stderr.Write([]byte(fmt.Sprintf("Annotations are invalid JSON. Err follows:\n%v", err)))
			os.Exit(1)
		}

		pc, err := pccontrol.Update(annotations)
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
	default:
		log.Fatalf("Unrecognized command %v", cmd)
	}
}

func selectorFrom(az fields.AvailabilityZone, cn fields.ClusterName, podID types.PodID) labels.Selector {
	return labels.Everything().
		Add(fields.PodIDLabel, labels.EqualsOperator, []string{podID.String()}).
		Add(fields.AvailabilityZoneLabel, labels.EqualsOperator, []string{az.String()}).
		Add(fields.ClusterNameLabel, labels.EqualsOperator, []string{cn.String()})
}

func currentUserName() string {
	username := "unknown user"

	if user, err := user.Current(); err == nil {
		username = user.Username
	}
	return username
}
