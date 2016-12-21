package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/kp/podstore"
	"github.com/square/p2/pkg/schedule"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	manifestPath = kingpin.Arg("manifest", "a manifest file to schedule in the intent store").String()
	nodeName     = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	hookGlobal   = kingpin.Flag("hook", "Schedule as a global hook.").Bool()
	uuidPod      = kingpin.Flag("uuid-pod", "Schedule the pod using the new UUID scheme").Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts, _ := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(opts)
	consulStore := kp.NewConsulStore(client)
	podStore := podstore.NewConsul(client.KV())

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("Could not get the hostname to do scheduling: %s", err)
		}
		*nodeName = hostname
	}

	if *manifestPath == "" {
		kingpin.Usage()
		log.Fatalln("No manifest given")
	}

	podManifest, err := store.FromPath(*manifestPath)
	if err != nil {
		log.Fatalf("Could not read manifest at %s: %s\n", *manifestPath, err)
	}

	out := schedule.Output{
		PodID: podManifest.ID(),
	}
	if *uuidPod {
		out.PodUniqueKey, err = podStore.Schedule(podManifest, types.NodeName(*nodeName))
		if err != nil {
			log.Fatalf("Could not schedule pod: %s", err)
		}
	} else {

		// Legacy pod
		podPrefix := kp.INTENT_TREE
		if *hookGlobal {
			podPrefix = kp.HOOK_TREE
		}
		_, err := consulStore.SetPod(podPrefix, types.NodeName(*nodeName), podManifest)
		if err != nil {
			log.Fatalf("Could not write manifest %s to intent store: %s\n", podManifest.ID(), err)
		}
	}

	outBytes, err := json.Marshal(out)
	if err != nil {
		log.Fatalf("Successfully scheduled manifest but couldn't marshal JSON output")
	}

	fmt.Println(string(outBytes))
}
