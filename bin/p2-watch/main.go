package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util/net"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"
	"github.com/square/p2/pkg/version"
)

var (
	nodeName      = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	watchReality  = kingpin.Flag("reality", "Watch the reality store instead of the intent store. False by default").Default("false").Bool()
	hookTypeName  = kingpin.Flag("hook-type", "Watch a particular hook type instead of the intent store.").String()
	consulAddress = kingpin.Flag("consul", "The address of the consul node to use. Defaults to 0.0.0.0:8500").String()
	consulToken   = kingpin.Flag("token", "The ACL to use for accessing consul.").String()
	headers       = kingpin.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	https         = kingpin.Flag("https", "Use HTTPS").Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	store := kp.NewConsulStore(kp.Options{
		Address: *consulAddress,
		Token:   *consulToken,
		Client:  net.NewHeaderClient(*headers, http.DefaultTransport),
		HTTPS:   *https,
	})

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
	} else if *hookTypeName != "" {
		hookType, err := hooks.AsHookType(*hookTypeName)
		if err != nil {
			log.Fatalln(err)
		}
		path = kp.HookPath(hookType, *nodeName)
	}
	log.Printf("Watching manifests at %s\n", path)

	quit := make(chan struct{})
	errChan := make(chan error)
	podCh := make(chan []kp.ManifestResult)
	go store.WatchPods(path, quit, errChan, podCh)
	for {
		select {
		case results := <-podCh:
			for _, result := range results {
				fmt.Println("")
				result.Manifest.Write(os.Stdout)
			}
		case err := <-errChan:
			log.Fatalf("Error occurred while listening to pods: %s", err)
		}
	}
}
