package main

import (
	"fmt"
	"log"
	"os"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	manifests = kingpin.Arg("manifests", "one or more manifest files to schedule in the intent store").Strings()
	nodeName  = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	asHook    = kingpin.Flag("hook", "Schedule as a hook, not an intended pod. False by default.").Default("false").Bool()
)

func main() {
	kingpin.Version("0.0.1")
	kingpin.Parse()

	store := kp.NewStore(kp.Options{})

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
		manifest, err := pods.PodManifestFromPath(manifestPath)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Could not read manifest at %s: %s\n", manifestPath, err))
			continue
		}
		path := kp.IntentPath(*nodeName, manifest.ID())
		if *asHook {
			path = kp.HookPath(*nodeName, manifest.ID())
		}
		duration, err := store.SetPod(path, *manifest)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Could not write manifest %s to intent store: %s\n", manifest.ID(), err))
			continue
		}
		log.Printf("Scheduling %s took %s", manifest.ID(), duration)
	}
}
