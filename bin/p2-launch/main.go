package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/version"
)

var (
	manifestURI = kingpin.Arg("manifest", "a path or url to a pod manifest that will be installed and launched immediately.").String()
	podRoot     = kingpin.Flag("pod-root", "the root of the pods directory").Default(pods.DEFAULT_PATH).String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()
	localMan, err := ioutil.TempFile("", "tempmanifest")
	defer os.Remove(localMan.Name())
	if err != nil {
		log.Fatalln("Couldn't create tempfile")
	}

	err = uri.URICopy(*manifestURI, localMan.Name())
	if err != nil {
		log.Fatalf("Could not fetch manifest: %s", err)
	}
	manifest, err := pods.ManifestFromPath(localMan.Name())
	if err != nil {
		log.Fatalf("Invalid manifest: %s", err)
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
