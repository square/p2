package main

import (
	"log"
	"os"

	store_protos "github.com/square/p2/pkg/store_server/protos"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	podID    = kingpin.Flag("pod-id", "pod id for the pod to be retrieved").Required().String()
	nodeName = kingpin.Flag("node", "The node to fetch the manifest for").Required().String()
	intent   = kingpin.Flag("intent", "Specifies desire to fetch the intent manifest. Mutually exclusive with --reality").Bool()
	reality  = kingpin.Flag("reality", "Specifies desire to fetch the reality manifest. Mutually exclusive with --intent").Bool()
	address  = kingpin.Flag("address", "Address of the p2-store-server to talk to. Defaults to localhost:3000").String()
)

const (
	defaultAddress = "localhost:3000"
)

func main() {
	kingpin.Parse()
	logger := log.New(os.Stderr, "", 0)
	if *intent && *reality {
		logger.Fatal("--reality and --intent cannot both be set")
	}

	if !*intent && !*reality {
		logger.Fatal("one of --reality or --intent must be set")
	}

	var podPrefix store_protos.PodPrefix
	if *intent {
		podPrefix = store_protos.PodPrefix_intent
	}

	addr := defaultAddress
	if *address != "" {
		addr = *address
	}

	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := store_protos.NewStoreClient(conn)

	r, err := c.GetPod(context.Background(), &store_protos.GetPodRequest{
		PodPrefix: podPrefix,
		PodId:     *podID,
		NodeName:  *nodeName,
	})
	if err != nil {
		log.Fatalf("could not get pod: %v", err)
	}

	logger.Printf(r.Manifest)
}
