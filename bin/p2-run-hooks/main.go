package main

import (
	"log"
	"os"

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
	nodeName     = kingpin.Flag("node-name", "The name of this node (default: hostname)").String()
	podRoot      = kingpin.Flag("pod-root", "The system root for pods").Default(pods.DefaultPath).String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("error getting node name: %v", err)
		}
		*nodeName = hostname
	}

	dir := hooks.Hooks(*hookDir, *podRoot, &logging.DefaultLogger)

	hookType, err := hooks.AsHookType(*hookType)
	if err != nil {
		log.Fatalln(err)
	}

	pod, err := pods.PodFromPodHome(types.NodeName(*nodeName), *podDir)
	if err != nil {
		log.Fatalln(err)
	}

	var podManifest manifest.Manifest
	if *manifestPath != "" {
		podManifest, err = manifest.FromPath(*manifestPath)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		podManifest, err = pod.CurrentManifest()
		if err != nil {
			log.Fatalln(err)
		}
	}

	log.Printf("About to run %s hooks for pod %s\n", hookType, pod.Home())
	err = dir.RunHookType(hookType, pod, podManifest)
	if err != nil {
		log.Fatalln(err)
	}
}
