package main

import (
	"fmt"
	"log"
	"os"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"
	"github.com/square/p2/pkg/version"
)

var (
	nodeName     = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	watchReality = kingpin.Flag("reality", "Watch the reality store instead of the intent store. False by default").Default("false").Bool()
	hooks        = kingpin.Flag("hook", "Watch hooks.").Bool()
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

	path := kp.IntentPath(*nodeName)
	if *watchReality {
		path = kp.RealityPath(*nodeName)
	} else if *hooks {
		path = kp.HookPath()
	}
	log.Printf("Watching manifests at %s\n", path)

	quit := make(chan struct{})
	errChan := make(chan error)
	podCh := make(chan []kp.ManifestResult)
	go store.WatchPods(path, quit, errChan, podCh)
	for {
		select {
		case results := <-podCh:
			if len(results) == 0 {
				fmt.Println(fmt.Sprintf("No manifest exists at key %s (it may have been deleted)", path))
			} else {
				for _, result := range results {
					fmt.Println("")
					result.Manifest.Write(os.Stdout)
				}
			}
		case err := <-errChan:
			log.Fatalf("Error occurred while listening to pods: %s", err)
		}
	}
}
