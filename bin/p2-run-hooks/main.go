package main

import (
	"log"
	"path"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/version"
)

var (
	PodDir    = kingpin.Arg("pod", "A path to a pod that exists on disk already.").Required().String()
	Lifecycle = kingpin.Arg("hook-type", "Execute one of the given hook types").Required().String()
	HookDir   = kingpin.Flag("hook-dir", "The root of the hooks").Default(hooks.DEFAULT_PATH).String()
	Manifest  = kingpin.Flag("manifest", "The manifest to use (this is useful when we are in the before_install phase)").ExistingFile()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	dir := hooks.Hooks(*HookDir, &logging.DefaultLogger)

	hookType, err := hooks.AsHookType(*Lifecycle)
	if err != nil {
		log.Fatalln(err)
	}

	pod := pods.NewPod(path.Base(*PodDir), *PodDir)

	var manifest pods.Manifest
	if *Manifest != "" {
		manifest, err = pods.ManifestFromPath(*Manifest)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		manifest, err = pod.CurrentManifest()
		if err != nil {
			log.Fatalln(err)
		}
	}

	log.Printf("About to run %s hooks for pod %s\n", hookType, pod.Path())
	err = dir.RunHookType(hookType, pod, manifest)
	if err != nil {
		log.Fatalln(err)
	}
}
