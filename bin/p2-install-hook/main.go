package main

import (
	"log"
	"path/filepath"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/version"
)

var (
	manifestURI = kingpin.Arg("manifest", "a path or url to a pod manifest that will be installed and launched immediately.").URL()
	registryURI = kingpin.Arg("registry", "A URL to the registry to download artifacts from").URL()
	podRoot     = kingpin.Flag("pod-root", "the root of the pods directory").Default(pods.DEFAULT_PATH).String()
	hookRoot    = kingpin.Flag("hook-root", "the root of the hook scripts directory").Default(hooks.DEFAULT_PATH).String()
	hookType    = kingpin.Flag("hook-type", "the type of the hook (if unspecified, defaults to global)").String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	manifest, err := manifest.FromURI(*manifestURI)
	if err != nil {
		log.Fatalf("%s", err)
	}

	// /data/pods/hooks/<event>/<id>
	// if the event is the empty string (global hook), then that path segment
	// will be cleaned out
	pod := pods.NewPod(manifest.ID(), pods.PodPath(filepath.Join(*podRoot, "hooks", *hookType), manifest.ID()))

	// for now use noop verifier in this CLI
	err = pod.Install(manifest, auth.NopVerifier(), artifact.NewRegistry(*registryURI, uri.DefaultFetcher, osversion.DefaultDetector))
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
