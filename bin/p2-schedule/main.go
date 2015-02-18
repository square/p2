package main

import (
	"fmt"
	"log"
	"os"

	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	manifests     = kingpin.Arg("manifests", "one or more manifest files to schedule in the intent store").Strings()
	nodeName      = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	hookTypeName  = kingpin.Flag("hook-type", "Schedule as a hook, not an intended pod, as the given hook type. Can be one of the hooks listed in hooks.go").String()
	consulAddress = kingpin.Flag("consul", "The address of the consul node to use. Defaults to 0.0.0.0:8500").String()
	consulToken   = kingpin.Flag("token", "The ACL to use for accessing consul.").String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	store := kp.NewStore(kp.Options{
		Address: *consulAddress,
		Token:   *consulToken,
	})

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
		if *hookTypeName != "" {
			hookType, err := hooks.AsHookType(*hookTypeName)
			if err != nil {
				log.Fatalln(err)
			}
			path = kp.HookPath(hookType, manifest.ID())
		}
		duration, err := store.SetPod(path, *manifest)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Could not write manifest %s to intent store: %s\n", manifest.ID(), err))
			continue
		}
		log.Printf("Scheduling %s took %s", manifest.ID(), duration)
	}
}
