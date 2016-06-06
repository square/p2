package main

import (
	"log"
	"os"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	manifests  = kingpin.Arg("manifests", "one or more manifest files to schedule in the intent store").Strings()
	nodeName   = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	hookGlobal = kingpin.Flag("hook", "Schedule as a global hook.").Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(opts)
	store := kp.NewConsulStore(client)

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("Could not get the hostname to do scheduling: %s", err)
		}
		*nodeName = hostname
	}

	if len(*manifests) == 0 {
		kingpin.Usage()
		log.Fatalln("No manifests given")
	}

	for _, manifestPath := range *manifests {
		manifest, err := pods.ManifestFromPath(manifestPath)
		if err != nil {
			log.Fatalf("Could not read manifest at %s: %s\n", manifestPath, err)
		}
		podPrefix := kp.INTENT_TREE
		if *hookGlobal {
			podPrefix = kp.HOOK_TREE
		}
		duration, err := store.SetPod(podPrefix, types.NodeName(*nodeName), manifest)
		if err != nil {
			log.Fatalf("Could not write manifest %s to intent store: %s\n", manifest.ID(), err)
		}
		log.Printf("Scheduling %s took %s\n", manifest.ID(), duration)
	}
}
