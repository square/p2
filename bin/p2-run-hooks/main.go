package main

import (
	"log"
	"path"

	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	podDir       = kingpin.Arg("pod", "A path to a pod that exists on disk already.").Required().String()
	hookType     = kingpin.Arg("hook-type", "Execute one of the given hook types").Required().String()
	hookDir      = kingpin.Flag("hook-dir", "The root of the hooks").Default(hooks.DEFAULT_PATH).String()
	manifestPath = kingpin.Flag("manifest", "The manifest to use (this is useful when we are in the before_install phase)").ExistingFile()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	dir := hooks.Hooks(*hookDir, &logging.DefaultLogger)

	hookType, err := hooks.AsHookType(*hookType)
	if err != nil {
		log.Fatalln(err)
	}

	pod := pods.NewPod(types.PodID(path.Base(*podDir)), *podDir)

	var podManifest manifest.Manifest
	if *manifestPath != "" {
		podManifest, err = manifest.ManifestFromPath(*manifestPath)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		podManifest, err = pod.CurrentManifest()
		if err != nil {
			log.Fatalln(err)
		}
	}

	log.Printf("About to run %s hooks for pod %s\n", hookType, pod.Path())
	err = dir.RunHookType(hookType, pod, podManifest)
	if err != nil {
		log.Fatalln(err)
	}
}
