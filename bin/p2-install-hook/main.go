package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/logging"
	p2manifest "github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/version"
)

var (
	manifestURI        = kingpin.Arg("manifest", "a path or url to a pod manifest that will be installed and launched immediately").URL()
	registryURI        = kingpin.Arg("registry", "a URL to the registry to download artifacts from").URL()
	isPreparerManifest = kingpin.Arg("isPreparerManifest", "to install hooks from a P2-Preparer manifest").Bool()
	nodeName           = kingpin.Flag("node-name", "the name of this node (default: hostname)").String()
	podRoot            = kingpin.Flag("pod-root", "the root of the pods directory").Default(pods.DefaultPath).String()
	hookRoot           = kingpin.Flag("hook-root", "the root of the hook scripts directory").Default(hooks.DefaultPath).String()
	hookType           = kingpin.Flag("hook-type", "the type of the hook (if unspecified, defaults to global)").String()
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

	manifest, err := p2manifest.FromURI(*manifestURI)
	if err != nil {
		log.Fatalf("%s", err)
	}

	if *isPreparerManifest {
		config := manifest.GetConfig()
		hooksManifest, ok := config["hooks_manifest"]
		if ok {
			manifest = hooksManifest.(p2manifest.Manifest)
		} else {
			fmt.Printf("Error to read hooks manifest from P2-Preparer manifest: %v", manifest)
		}
	}

	// TODO: Configure fetcher?
	hookFactory := pods.NewHookFactory(filepath.Join(*podRoot, "hooks", *hookType), types.NodeName(*nodeName), uri.DefaultFetcher)

	// /data/pods/hooks/<event>/<id>
	// if the event is the empty string (global hook), then that path segment
	// will be cleaned out
	pod := hookFactory.NewHookPod(manifest.ID())

	// for now use noop verifier in this CLI
	err = pod.Install(manifest, auth.NopVerifier(), artifact.NewRegistry(*registryURI, uri.DefaultFetcher, osversion.DefaultDetector), "", []string{})
	if err != nil {
		log.Fatalf("Could not install manifest %s: %s", manifest.ID(), err)
	}
	// hooks write their current manifest manually since it's normally done at
	// launch time
	_, err = pod.WriteCurrentManifest(manifest)
	if err != nil {
		log.Fatalf("Could not write current manifest for %s: %s", manifest.ID(), err)
	}

	err = hooks.InstallHookScripts(*hookRoot, pod, manifest, logging.DefaultLogger)
	if err != nil {
		log.Fatalf("Could not write hook scripts: %s", err)
	}
}
