package main

import (
	"fmt"
	"log"
	"os"

	"github.com/square/p2/pkg/intent"
	"github.com/square/p2/pkg/pods"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	nodeName = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
)

func main() {
	kingpin.Version("0.0.1")
	kingpin.Parse()

	store, err := intent.LookupStore(intent.Options{})
	if err != nil {
		log.Fatalf("Could not look up intent store: %s", err)
	}
	node := *nodeName
	if node == "" {
		node, err = os.Hostname()
		if err != nil {
			log.Fatalf("Could not get the hostname to do scheduling: %s", err)
		}
	}
	path := fmt.Sprintf("nodes/%s", node)

	log.Printf("Watching manifests at %s\n", path)

	quit := make(chan struct{})
	errChan := make(chan error)
	podCh := make(chan pods.PodManifest)
	go store.WatchPods(path, quit, errChan, podCh)
	for {
		select {
		case manifest := <-podCh:
			fmt.Println("")
			manifest.Write(os.Stdout)
		case err := <-errChan:
			log.Fatalf("Error occurred while listening to pods: %s", err)
		}
	}
}
