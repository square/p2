package main

import (
	"log"
	"net/url"
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/store"
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
	podRoot            = kingpin.Flag("pod-root", "The root of where pods will be installed").Default(pods.DefaultPath).String()
	registryURL        = kingpin.Flag("registry", "The URL of the registry to download artifacts from").URL()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()
	log.Println("Starting bootstrap")
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("error getting node name: %v", err)
	}
	nodeName := store.NodeName(hostname)
	agentManifest, err := store.FromPath(*agentManifestPath)
	if err != nil {
		log.Fatalln("Could not get agent manifest: %s", err)
	}
	log.Println("Installing and launching consul")

	podFactory := pods.NewFactory(*podRoot, nodeName)

	var consulPod *pods.Pod
	var consulManifest store.Manifest
	if *existingConsul == "" {
		consulManifest, err = store.FromPath(*consulManifestPath)
		if err != nil {
			log.Fatalf("Could not get consul manifest: %s", err)
		}

		// Consul will never have a uuid (for now)
		consulPod = podFactory.NewLegacyPod(consulManifest.ID())
		err = installConsul(consulPod, consulManifest, *registryURL)
		if err != nil {
			log.Fatalf("Could not install consul: %s", err)
		}
	} else {
		log.Printf("Using existing Consul at %s\n", *existingConsul)

		consulPod, err = pods.PodFromPodHome(nodeName, *existingConsul)
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
	err = installBaseAgent(podFactory, agentManifest, *registryURL)
	if err != nil {
		log.Fatalf("Could not install base agent: %s", err)
	}
	if err := verifyReality(30*time.Second, consulManifest.ID(), agentManifest.ID()); err != nil {
		log.Fatalln(err)
	}
	log.Println("Bootstrapping complete")
}

func installConsul(consulPod *pods.Pod, consulManifest store.Manifest, registryURL *url.URL) error {
	// Inject servicebuilder?
	err := consulPod.Install(consulManifest, auth.NopVerifier(), artifact.NewRegistry(registryURL, uri.DefaultFetcher, osversion.DefaultDetector))
	if err != nil {
		return util.Errorf("Can't install Consul, aborting: %s", err)
	}
	ok, err := consulPod.Launch(consulManifest)
	if err != nil || !ok {
		return util.Errorf("Can't launch Consul, aborting: %s", err)
	}
	return nil
}

// Ping confirms that Consul can be reached and it has a leader. If the return
// is nil, then consul should be ready to accept requests.
//
// If the return is non-nil, this typically indicates that either Consul is
// unreachable (eg the agent is not listening on the target port) or has not
// found a leader (in which case Consul returns a 500 to all endpoints, except
// the status store).
//
// If a cluster is starting for the first time, it may report a leader just
// before beginning raft replication, thus rejecting requests made at that
// exact moment.
func Ping(client *api.Client) error {
	_, qm, err := client.Catalog().Nodes(&api.QueryOptions{RequireConsistent: true})
	if err != nil {
		return consulutil.NewKVError("ping", "/catalog/nodes", err)
	}
	if qm == nil || !qm.KnownLeader {
		return util.Errorf("No known leader")
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

	config := api.DefaultConfig()
	config.Token = *consulToken
	client, err := api.NewClient(config)
	if err != nil {
		return util.Errorf("Could not construct consul client: '%s'", err)
	}
	consulIsUp := make(chan struct{})
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			err := Ping(client)
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

func verifyReality(waitTime time.Duration, consulID store.PodID, agentID store.PodID) error {
	quit := make(chan struct{})
	defer close(quit)
	consulStore := kp.NewConsulStore(kp.NewConsulClient(kp.Options{
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
			results, _, err := consulStore.ListPods(kp.REALITY_TREE, store.NodeName(hostname))
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

func scheduleForThisHost(manifest store.Manifest, alsoReality bool) error {
	consulStore := kp.NewConsulStore(kp.NewConsulClient(kp.Options{
		Token: *consulToken,
	}))
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	_, err = consulStore.SetPod(kp.INTENT_TREE, store.NodeName(hostname), manifest)
	if err != nil {
		return err
	}

	if alsoReality {
		_, err = consulStore.SetPod(kp.REALITY_TREE, store.NodeName(hostname), manifest)
		return err
	}
	return nil
}

func installBaseAgent(podFactory pods.Factory, agentManifest store.Manifest, registryURL *url.URL) error {
	// preparer will never have a uuid (for now)
	agentPod := podFactory.NewLegacyPod(agentManifest.ID())
	err := agentPod.Install(agentManifest, auth.NopVerifier(), artifact.NewRegistry(registryURL, uri.DefaultFetcher, osversion.DefaultDetector))
	if err != nil {
		return err
	}
	_, err = agentPod.Launch(agentManifest)
	return err
}
