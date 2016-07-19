package main

import (
	"log"
	"net/url"
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/version"
)

var (
	consulManifestPath = kingpin.Flag("consul-pod", "A path to the manifest that will be used to boot Consul.").ExistingFile()
	existingConsul     = kingpin.Flag("existing-consul-pod", "A path to an existing Consul pod that will be supplied to the base agent's configuration.").ExistingDir()
	agentManifestPath  = kingpin.Flag("agent-pod", "A path to the manifest that will used to boot the base agent.").ExistingFile()
	timeout            = kingpin.Flag("consul-timeout", "How long to wait for consul to begin serving. 0 will skip the consul check altogether.").Default("10s").String()
	consulToken        = kingpin.Flag("consul-token", "The ACL token to pass to consul when registering the bootstrapped pods").String()
	podRoot            = kingpin.Flag("pod-root", "The root of where pods will be installed").Default(pods.DEFAULT_PATH).String()
	registryURL        = kingpin.Flag("registry", "The URL of the registry to download artifacts from").URL()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()
	log.Println("Starting bootstrap")
	agentManifest, err := manifest.FromPath(*agentManifestPath)
	if err != nil {
		log.Fatalln("Could not get agent manifest: %s", err)
	}
	log.Println("Installing and launching consul")

	var consulPod *pods.Pod
	var consulManifest manifest.Manifest
	if *existingConsul == "" {
		consulManifest, err = manifest.FromPath(*consulManifestPath)
		if err != nil {
			log.Fatalf("Could not get consul manifest: %s", err)
		}
		consulPod = pods.NewPod(consulManifest.ID(), pods.PodPath(*podRoot, consulManifest.ID()))
		err = installConsul(consulPod, consulManifest, *registryURL)
		if err != nil {
			log.Fatalf("Could not install consul: %s", err)
		}
	} else {
		log.Printf("Using existing Consul at %s\n", *existingConsul)

		consulPod, err = pods.ExistingPod(*existingConsul)
		if err != nil {
			log.Fatalf("The existing consul pod is invalid: %s", err)
		}
		consulManifest, err = consulPod.CurrentManifest()
		if err != nil {
			log.Fatalf("Cannot get the current consul manifest: %s", err)
		}
	}

	if err = verifyConsulUp(*timeout); err != nil {
		log.Fatalln(err)
	}
	time.Sleep(500 * time.Millisecond)
	// schedule consul in the reality store as well, to ensure the preparers do
	// not all restart their consul agents simultaneously after bootstrapping
	err = scheduleForThisHost(consulManifest, true)
	if err != nil {
		log.Fatalf("Could not register consul in the intent store: %s", err)
	}

	log.Println("Registering base agent in consul")
	err = scheduleForThisHost(agentManifest, false)
	if err != nil {
		log.Fatalf("Could not register base agent with consul: %s", err)
	}
	log.Println("Installing and launching base agent")
	err = installBaseAgent(agentManifest, *registryURL)
	if err != nil {
		log.Fatalf("Could not install base agent: %s", err)
	}
	if err := verifyReality(30*time.Second, consulManifest.ID(), agentManifest.ID()); err != nil {
		log.Fatalln(err)
	}
	log.Println("Bootstrapping complete")
}

func installConsul(consulPod *pods.Pod, consulManifest manifest.Manifest, registryURL *url.URL) error {
	// Inject servicebuilder?
	err := consulPod.Install(consulManifest, auth.NopVerifier(), artifact.NewRegistry(registryURL, uri.DefaultFetcher))
	if err != nil {
		return util.Errorf("Can't install Consul, aborting: %s", err)
	}
	ok, err := consulPod.Launch(consulManifest)
	if err != nil || !ok {
		return util.Errorf("Can't launch Consul, aborting: %s", err)
	}
	return nil
}

func verifyConsulUp(timeout string) error {
	timeoutDur, err := time.ParseDuration(timeout)
	if err != nil {
		return err
	}
	if timeoutDur == 0 {
		return nil
	}

	store := kp.NewConsulStore(kp.NewConsulClient(kp.Options{
		Token: *consulToken, // not actually necessary because this endpoint is unauthenticated
	}))
	consulIsUp := make(chan struct{})
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			err := store.Ping()
			if err == nil {
				consulIsUp <- struct{}{}
				return
			}
		}
	}()
	select {
	case <-time.After(timeoutDur):
		return util.Errorf("Consul did not start or was not available after %v", timeoutDur)
	case <-consulIsUp:
		return nil
	}
}

func verifyReality(waitTime time.Duration, consulID types.PodID, agentID types.PodID) error {
	quit := make(chan struct{})
	defer close(quit)
	store := kp.NewConsulStore(kp.NewConsulClient(kp.Options{
		Token: *consulToken,
	}))
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	waitChan := time.After(waitTime)
	hasConsul := false
	hasPreparer := false
	for {
		select {
		case <-waitChan:
			return util.Errorf(
				"Consul and/or Preparer weren't in the reality store within %s (consul=%t, preparer=%t)",
				waitTime, hasConsul, hasPreparer)
		case <-time.After(100 * time.Millisecond):
			results, _, err := store.ListPods(kp.REALITY_TREE, types.NodeName(hostname))
			if err != nil {
				log.Printf("Error looking for pods: %s\n", err)
				continue
			}
			for _, res := range results {
				if res.Manifest.ID() == consulID {
					hasConsul = true
				} else if res.Manifest.ID() == agentID {
					hasPreparer = true
				}
			}
			if hasConsul && hasPreparer {
				return nil
			}
		}
	}
}

func scheduleForThisHost(manifest manifest.Manifest, alsoReality bool) error {
	store := kp.NewConsulStore(kp.NewConsulClient(kp.Options{
		Token: *consulToken,
	}))
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	_, err = store.SetPod(kp.INTENT_TREE, types.NodeName(hostname), manifest)
	if err != nil {
		return err
	}

	if alsoReality {
		_, err = store.SetPod(kp.REALITY_TREE, types.NodeName(hostname), manifest)
		return err
	}
	return nil
}

func installBaseAgent(agentManifest manifest.Manifest, registryURL *url.URL) error {
	agentPod := pods.NewPod(agentManifest.ID(), pods.PodPath(*podRoot, agentManifest.ID()))
	err := agentPod.Install(agentManifest, auth.NopVerifier(), artifact.NewRegistry(registryURL, uri.DefaultFetcher))
	if err != nil {
		return err
	}
	_, err = agentPod.Launch(agentManifest)
	return err
}
