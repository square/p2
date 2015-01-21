package main

import (
	"fmt"
	"log"
	"os"

	"github.com/square/p2/pkg/kp"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	nodeName     = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	watchReality = kingpin.Flag("reality", "Watch the reality store instead of the intent store. False by default").Default("false").Bool()
	watchHooks   = kingpin.Flag("hooks", "Watch the hooks instead of the intent store. False by default").Default("false").Bool()
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

	path := kp.IntentPath(*nodeName)
	if *watchReality {
		path = kp.RealityPath(*nodeName)
	} else if *watchHooks {
		path = kp.HookPath(*nodeName)
	}
	log.Printf("Watching manifests at %s\n", path)

	quit := make(chan struct{})
	errChan := make(chan error)
	podCh := make(chan kp.ManifestResult)
	go store.WatchPods(path, quit, errChan, podCh)
	for {
		select {
		case result := <-podCh:
			fmt.Println("")
			result.Manifest.Write(os.Stdout)
		case err := <-errChan:
			log.Fatalf("Error occurred while listening to pods: %s", err)
		}
	}
}
