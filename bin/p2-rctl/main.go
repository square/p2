package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/rcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/util/net"
	"github.com/square/p2/pkg/version"
)

const (
	CMD_CREATE   = "create"
	CMD_DELETE   = "delete"
	CMD_REPLICAS = "set-replicas"
	CMD_LIST     = "list"
	CMD_GET      = "get"
)

var (
	consulUrl   = kingpin.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	consulToken = kingpin.Flag("token", "The consul ACL token to use. Empty by default.").String()
	headers     = kingpin.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	https       = kingpin.Flag("https", "Use HTTPS").Bool()

	cmdCreate       = kingpin.Command(CMD_CREATE, "Create a new replication controller")
	createManifest  = cmdCreate.Flag("manifest", "manifest file to use for this replication controller").Short('m').Required().String()
	createNodeSel   = cmdCreate.Flag("node-selector", "node selector that this replication controller should target").Short('n').Required().String()
	createPodLabels = cmdCreate.Flag("pod-label", "a pod label, in LABEL=VALUE form, to add to this replication controller. Can be specified multiple times.").Short('p').StringMap()

	cmdDelete = kingpin.Command(CMD_DELETE, "Delete a replication controller")
	deleteID  = cmdDelete.Arg("id", "replication controller uuid to delete").Required().String()

	cmdReplicas = kingpin.Command(CMD_REPLICAS, "Set desired replica count of a replication controller")
	replicasID  = cmdReplicas.Arg("id", "replication controller uuid to modify").Required().String()
	replicasNum = cmdReplicas.Arg("replicas", "number of replicas desired").Required().Int()

	cmdList  = kingpin.Command(CMD_LIST, "List replication controllers")
	listJSON = cmdList.Flag("json", "output the entire JSON object of each replication controller").Short('j').Bool()

	cmdGet      = kingpin.Command(CMD_GET, "Get replication controller")
	getID       = cmdGet.Arg("id", "replication controller uuid to get").Required().String()
	getManifest = cmdGet.Flag("manifest", "print just the manifest of the replication controller").Short('m').Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	cmd := kingpin.Parse()

	logger := logging.NewLogger(logrus.Fields{})
	logger.Logger.Formatter = &logrus.TextFormatter{}
	opts := kp.Options{
		Address: *consulUrl,
		Token:   *consulToken,
		Client:  net.NewHeaderClient(*headers, http.DefaultTransport),
		HTTPS:   *https,
	}
	client := kp.NewConsulClient(opts)
	rcStore := rcstore.NewConsul(client, 3, logger)
	rctl := RCtl{
		rcs:    rcStore,
		logger: logger,
	}

	switch cmd {
	case CMD_CREATE:
		rctl.Create(*createManifest, *createNodeSel, *createPodLabels)
	case CMD_DELETE:
		rctl.Delete(*deleteID)
	case CMD_REPLICAS:
		rctl.SetReplicas(*replicasID, *replicasNum)
	case CMD_LIST:
		rctl.List(*listJSON)
	case CMD_GET:
		rctl.Get(*getID, *getManifest)
	}
}

// rctl is a struct for the data structures shared between commands
// each member function represents a single command that takes over from main
// and terminates the program on failure
type RCtl struct {
	rcs    rcstore.Store
	logger logging.Logger
}

func (r RCtl) Create(manifestPath, nodeSelector string, podLabels map[string]string) {
	manifest, err := pods.ManifestFromPath(manifestPath)
	if err != nil {
		r.logger.WithErrorAndFields(err, logrus.Fields{
			"manifest": manifestPath,
		}).Fatalln("Could not read pod manifest")
	}

	nodeSel, err := labels.Parse(nodeSelector)
	if err != nil {
		r.logger.WithErrorAndFields(err, logrus.Fields{
			"selector": nodeSelector,
		}).Fatalln("Could not parse node selector")
	}

	newRC, err := r.rcs.Create(manifest, nodeSel, labels.Set(podLabels))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not create replication controller in Consul")
	}
	r.logger.WithField("id", newRC.ID).Infoln("Created new replication controller")
}

func (r RCtl) Delete(id string) {
	err := r.rcs.Delete(fields.ID(id))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not delete replication controller in Consul")
	}
	r.logger.WithField("id", id).Infoln("Deleted replication controller")
}

func (r RCtl) SetReplicas(id string, replicas int) {
	if replicas < 0 {
		r.logger.NoFields().Fatalln("Cannot set negative replica count")
	}

	err := r.rcs.SetDesiredReplicas(fields.ID(id), replicas)
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not set desired replica count in Consul")
	}
	r.logger.WithFields(logrus.Fields{
		"id":       id,
		"replicas": replicas,
	}).Infoln("Set desired replica count of replication controller")
}

func (r RCtl) List(asJSON bool) {
	list, err := r.rcs.List()
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not list replication controllers in Consul")
	}

	for _, listRC := range list {
		if asJSON {
			out, err := json.MarshalIndent(listRC, "", "    ")
			if err != nil {
				r.logger.WithError(err).Fatalln("Could not marshal replication controller to JSON")
			}
			fmt.Printf("%s\n", out)
		} else {
			fmt.Println(listRC.ID)
		}
	}
}

func (r RCtl) Get(id string, manifest bool) {
	getRC, err := r.rcs.Get(fields.ID(id))
	if err != nil {
		r.logger.WithError(err).Fatalln("Could not get replication controller in Consul")
	}

	if manifest {
		out, err := getRC.Manifest.Marshal()
		if err != nil {
			r.logger.WithError(err).Fatalln("Could not marshal replication controller manifest")
		}
		fmt.Printf("%s", out)
	} else {
		out, err := json.MarshalIndent(getRC, "", "    ")
		if err != nil {
			r.logger.WithError(err).Fatalln("Could not marshal replication controller to JSON")
		}
		fmt.Printf("%s\n", out)
	}
}
