package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Sirupsen/logrus"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	"k8s.io/kubernetes/pkg/labels"
)

const (
	CmdCreate  = "create"
	CmdGet     = "get"
	CmdList    = "list"
	CmdEnable  = "enable"
	CmdDisable = "disable"
	CmdDelete  = "delete"
	CmdUpdate  = "update"
)

var (
	cmdCreate       = kingpin.Command(CmdCreate, "Create a daemon set.")
	createAZ        = cmdCreate.Flag("az", "The availability zone of the pod cluster").Required().String()
	createManifest  = cmdCreate.Flag("manifest", "Complete manifest - must parse as JSON!").Required().String()
	createMinHealth = cmdCreate.Flag("minhealth", "The minimum health of the daemon set").Required().String()
	createName      = cmdCreate.Flag("name", "The cluster name (ie. staging, production)").Required().String()
	createPodID     = cmdCreate.Flag("pod", "The pod ID on the daemon set").Required().String()

	cmdGet = kingpin.Command(CmdGet, "Show a daemon set.")
	getID  = cmdGet.Arg("id", "The uuid for the daemon set").Required().String()

	cmdList = kingpin.Command(CmdList, "List daemon sets.")

	cmdEnable = kingpin.Command(CmdEnable, "Enable daemon set.")
	enableID  = cmdEnable.Arg("id", "The uuid for the daemon set").Required().String()

	cmdDisable = kingpin.Command(CmdDisable, "Disable daemon set.")
	disableID  = cmdDisable.Arg("id", "The uuid for the daemon set").Required().String()

	cmdDelete = kingpin.Command(CmdDelete, "Delete daemon set.")
	deleteID  = cmdDelete.Arg("id", "The uuid for the daemon set").Required().String()

	cmdUpdate       = kingpin.Command(CmdUpdate, "Update a daemon set.")
	updateID        = cmdUpdate.Arg("id", "The uuid for the daemon set").Required().String()
	updateAZ        = cmdUpdate.Flag("az", "The availability zone of the pod cluster").String()
	updateManifest  = cmdUpdate.Flag("manifest", "Complete manifest - must parse as JSON!").String()
	updateMinHealth = cmdUpdate.Flag("minhealth", "The minimum health of the daemon set").String()
	updateName      = cmdUpdate.Flag("name", "The cluster name (ie. staging, production)").String()
	updatePodID     = cmdUpdate.Flag("pod", "The pod ID on the daemon set").String()
)

func main() {
	cmd, consulOpts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(consulOpts)
	logger := logging.NewLogger(logrus.Fields{})
	dsstore := dsstore.NewConsul(client, 3, &logger)

	switch cmd {
	case CmdCreate:
		az := pc_fields.AvailabilityZone(*createAZ)
		minHealth, err := strconv.Atoi(*createMinHealth)
		if err != nil {
			log.Fatalf("Invalid value for minimum health, expected integer: %v", err)
		}
		name := ds_fields.ClusterName(*createName)
		podID := types.PodID(*createPodID)
		selector := selectorFrom(az)

		manifest, err := manifest.FromPath(*createManifest)
		if err != nil {
			log.Fatalf("%s", err)
		}

		ds, err := dsstore.Create(manifest, minHealth, name, selector, podID)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
		fmt.Printf("%v has been created in consul", ds.ID)
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
		for _, ds := range dsList {
			fmt.Println(ds.ID)
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

			if *updateAZ != "" {
				selector := selectorFrom(pc_fields.AvailabilityZone(*updateAZ))
				if ds.NodeSelector.String() != selector.String() {
					changed = true
					ds.NodeSelector = selectorFrom(pc_fields.AvailabilityZone(*updateAZ))
				}
			}
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
			if *updatePodID != "" {
				podID := types.PodID(*updatePodID)
				if ds.PodID != podID {
					changed = true
					ds.PodID = podID
				}
			}
			if *updateManifest != "" {
				manifest, err := manifest.FromPath(*updateManifest)
				if err != nil {
					return ds, util.Errorf("%s", err)
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

			if !changed {
				return ds, util.Errorf("No changes were made")
			}
			return ds, nil
		}

		_, err := dsstore.MutateDS(id, mutator)
		if err != nil {
			log.Fatalf("err: %v", err)
		}
		fmt.Printf("The daemon set '%s' has been successfully updated in consul", id.String())
		fmt.Println()

	default:
		log.Fatalf("Unrecognized command %v", cmd)
	}
}

func selectorFrom(az pc_fields.AvailabilityZone) labels.Selector {
	return labels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, labels.InOperator, []string{az.String()})
}
