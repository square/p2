package main

import (
	"log"
	"os"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/version"
)

var (
	manifests    = kingpin.Arg("manifests", "one or more manifest files to schedule in the intent store").Strings()
	nodeName     = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	hookTypeName = kingpin.Flag("hook-type", "Schedule as a hook, not an intended pod, as the given hook type. Can be one of the hooks listed in hooks.go, or the word \"global\"").String()
	// unhide this flag when typed hooks are removed
	hookGlobal = kingpin.Flag("hook", "Schedule as a global hook.").Hidden().Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts := flags.ParseWithConsulOptions()
	store := kp.NewConsulStore(opts)

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
		path := kp.IntentPath(*nodeName, manifest.ID())
		if *hookTypeName == "global" || *hookGlobal {
			path = kp.HookPath(manifest.ID())
		} else if *hookTypeName != "" {
			hookType, err := hooks.AsHookType(*hookTypeName)
			if err != nil {
				log.Fatalln(err)
			}
			path = kp.HookPath(hookType.String(), manifest.ID())
		}
		duration, err := store.SetPod(path, manifest)
		if err != nil {
			log.Fatalf("Could not write manifest %s to intent store: %s\n", manifest.ID(), err)
		}
		log.Printf("Scheduling %s took %s\n", manifest.ID(), duration)
	}
}
