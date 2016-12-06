package main

import (
	"encoding/json"
	"fmt"

	"github.com/square/p2/pkg/grpc/podstore/client"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"

	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	cmdScheduleText = "schedule"
	defaultAddress  = "localhost:3000"
)

var (
	address      = kingpin.Flag("address", "Address of the pod store server to talk to.").Default(defaultAddress).String()
	cmdSchedule  = kingpin.Command(cmdScheduleText, "Schedules a pod (as a UUID pod)")
	manifestFile = cmdSchedule.Flag("manifest", "Path to pod manifest file to schedule").Required().ExistingFile()
	node         = cmdSchedule.Flag("node", "Node to schedule pod manifest to").Required().String()
	insecure     = kingpin.Flag("insecure", "Don't use TLS/SSL to contact store").Bool()
	caCert       = kingpin.Flag("cacert", "Certificate file to use to verify server").ExistingFile()
)

func main() {
	cmd := kingpin.Parse()

	logger := logging.DefaultLogger

	client, err := client.New(*address, nil, logger)
	if err != nil {
		logger.Fatalf("Could not set up grpc client: %s", err)
	}

	switch cmd {
	case cmdScheduleText:
		schedule(client, logger)
	}
}

func schedule(client client.Client, logger logging.Logger) {
	m, err := manifest.FromPath(*manifestFile)
	if err != nil {
		logger.Fatalf("Could not read manifest: %s", err)
	}

	podUniqueKey, err := client.Schedule(m, types.NodeName(*node))
	if err != nil {
		logger.Fatalf("Could not schedule: %s", err)
	}

	output := struct {
		PodID        types.PodID        `json:"pod_id"`
		PodUniqueKey types.PodUniqueKey `json:"pod_unique_key"`
	}{
		PodID:        m.ID(),
		PodUniqueKey: podUniqueKey,
	}

	outBytes, err := json.Marshal(output)
	if err != nil {
		logger.Infof("Scheduled pod with key: %s", podUniqueKey)
		return
	}

	fmt.Println(string(outBytes))
}
