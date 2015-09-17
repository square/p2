package main

import (
	"log"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/version"
)

var (
	manifestURI = kingpin.Arg("manifest", "a path or url to a pod manifest that will be installed and launched immediately.").String()
	podRoot     = kingpin.Flag("pod-root", "the root of the pods directory").Default(pods.DEFAULT_PATH).String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	manifest, err := pods.ManifestFromURI(*manifestURI)
	if err != nil {
		log.Fatalf("%s", err)
	}

	pod := pods.NewPod(manifest.ID(), pods.PodPath(*podRoot, manifest.ID()))
	err = pod.Install(manifest)
	if err != nil {
		log.Fatalf("Could not install manifest %s: %s", manifest.ID(), err)
	}

	success, err := pod.Launch(manifest)
	if err != nil {
		log.Fatalf("Could not launch manifest %s: %s", manifest.ID(), err)
	}
	if !success {
		log.Fatalln("Unsuccessful launch of one or more things in the manifest")
	}
}
