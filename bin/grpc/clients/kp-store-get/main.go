package main

import (
	"log"
	"os"

	kp_protos "github.com/square/p2/pkg/grpc/kpstore/protos"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	podID    = kingpin.Flag("pod-id", "pod id for the pod to be retrieved").Required().String()
	nodeName = kingpin.Flag("node", "The node to fetch the manifest for").Required().String()
	intent   = kingpin.Flag("intent", "Specifies desire to fetch the intent manifest. Mutually exclusive with --reality").Bool()
	reality  = kingpin.Flag("reality", "Specifies desire to fetch the reality manifest. Mutually exclusive with --intent").Bool()
	address  = kingpin.Flag("address", "Address of the p2-store-server to talk to. Defaults to localhost:3000").String()
	insecure = kingpin.Flag("insecure", "Don't use TLS/SSL to contact store").Bool()
	caCert   = kingpin.Flag("cacert", "Certificate file to use to verify server").ExistingFile()
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

	var podPrefix kp_protos.PodPrefix
	if *intent {
		podPrefix = kp_protos.PodPrefix_intent
	}

	addr := defaultAddress
	if *address != "" {
		addr = *address
	}

	var dialOptions []grpc.DialOption
	if *insecure {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	} else {
		if *caCert == "" {
			log.Fatalf("Must provide --cacert if --insecure not specified")
		}
		transportCredentials, err := credentials.NewClientTLSFromFile(*caCert, "")
		if err != nil {
			log.Fatal(err)
		}
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(transportCredentials))
	}
	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, dialOptions...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := kp_protos.NewKPStoreClient(conn)

	r, err := c.GetPod(context.Background(), &kp_protos.GetPodRequest{
		PodPrefix: podPrefix,
		PodId:     *podID,
		NodeName:  *nodeName,
	})
	if err != nil {
		log.Fatalf("could not get pod: %v", err)
	}

	logger.Print(r.Manifest)
}
