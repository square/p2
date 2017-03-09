package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/square/p2/pkg/grpc/podstore/client"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	cmdScheduleText    = "schedule"
	cmdWatchStatusText = "watch-status"

	defaultAddress = "localhost:3000"
)

var (
	address = kingpin.Flag("address", "Address of the pod store server to talk to.").Default(defaultAddress).String()
	caCert  = kingpin.Flag("cacert", "Certificate file to use to verify server").ExistingFile()

	cmdSchedule  = kingpin.Command(cmdScheduleText, "Schedules a pod (as a UUID pod)")
	manifestFile = cmdSchedule.Flag("manifest", "Path to pod manifest file to schedule").Required().ExistingFile()
	node         = cmdSchedule.Flag("node", "Node to schedule pod manifest to").Required().String()

	cmdWatchStatus = kingpin.Command(cmdWatchStatusText, "Watch the status for a pod")
	podUniqueKey   = cmdWatchStatus.Flag("pod-unique-key", "Pod unique key (uuid) to watch status for").Short('k').Required().String()
	numIterations  = cmdWatchStatus.Flag("num-iterations", "Number of status updates to wait for before stopping").Short('n').Default("1").Int()
)

func main() {
	cmd := kingpin.Parse()

	logger := logging.DefaultLogger

	var creds credentials.TransportCredentials
	var err error
	if *caCert != "" {
		creds, err = credentials.NewClientTLSFromFile(*caCert, "")
		if err != nil {
			logger.Fatal(err)
		}
	}

	dialOptions := []grpc.DialOption{grpc.WithBlock(), grpc.WithTimeout(5 * time.Second)}
	if creds != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	} else {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(*address, dialOptions...)
	if err != nil {
		logger.Fatal(err)
	}

	client, err := client.New(conn, logger)
	if err != nil {
		logger.Fatalf("Could not set up grpc client: %s", err)
	}

	switch cmd {
	case cmdScheduleText:
		schedule(client, logger)
	case cmdWatchStatusText:
		watchStatus(client, logger)
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

func watchStatus(client client.Client, logger logging.Logger) {
	key, err := types.ToPodUniqueKey(*podUniqueKey)
	if err != nil {
		logger.Fatalf("Could not parse passed pod unique key %q as uuid: %s", *podUniqueKey, err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	outCh, err := client.WatchStatus(ctx, key, true)
	if err != nil {
		logger.Fatal(err)
	}

	for i := 0; i < *numIterations; i++ {
		val, ok := <-outCh
		if !ok {
			logger.Fatal("Channel closed unexpectedly")
		}

		if val.Error != nil {
			logger.Fatal(val.Error)
		}

		bytes, err := json.Marshal(val)
		if err != nil {
			logger.Fatal(err)
		}

		fmt.Println(string(bytes))
	}
}
