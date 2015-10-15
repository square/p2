package main

import (
	"log"
	"os"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/version"
)

var (
	consulManifestPath      = kingpin.Flag("consul-pod", "A path to the manifest that will be used to boot Consul.").ExistingFile()
	existingConsul          = kingpin.Flag("existing-consul-pod", "A path to an existing Consul pod that will be supplied to the base agent's configuration.").ExistingDir()
	agentManifestPath       = kingpin.Flag("agent-pod", "A path to the manifest that will used to boot the base agent.").ExistingFile()
	additionalManifestsPath = kingpin.Flag("additional-pods", "(Optional) a directory of additional pods that will be launched and added to the intent store immediately").ExistingDir()
	timeout                 = kingpin.Flag("consul-timeout", "How long to wait for consul to begin serving. 0 will skip the consul check altogether.").Default("10s").String()
	consulToken             = kingpin.Flag("consul-token", "The ACL token to pass to consul when registering the bootstrapped pods").String()
	podRoot                 = kingpin.Flag("pod-root", "The root of where pods will be installed").Default(pods.DEFAULT_PATH).String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()
	log.Println("Starting bootstrap")
	agentManifest, err := pods.ManifestFromPath(*agentManifestPath)
	if err != nil {
		log.Fatalln("Could not get agent manifest: %s", err)
	}
	log.Println("Installing and launching consul")

	var consulPod *pods.Pod
	var consulManifest pods.Manifest
	if *existingConsul == "" {
		consulManifest, err = pods.ManifestFromPath(*consulManifestPath)
		if err != nil {
			log.Fatalf("Could not get consul manifest: %s", err)
		}
		consulPod = pods.NewPod(consulManifest.ID(), pods.PodPath(*podRoot, consulManifest.ID()))
		err = InstallConsul(consulPod, consulManifest)
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

	if err = VerifyConsulUp(*timeout); err != nil {
		log.Fatalln(err)
	}
	time.Sleep(500 * time.Millisecond)
	// schedule consul in the reality store as well, to ensure the preparers do
	// not all restart their consul agents simultaneously after bootstrapping
	err = ScheduleForThisHost(consulManifest, true)
	if err != nil {
		log.Fatalf("Could not register consul in the intent store: %s", err)
	}

	log.Println("Registering base agent in consul")
	err = ScheduleForThisHost(agentManifest, false)
	if err != nil {
		log.Fatalf("Could not register base agent with consul: %s", err)
	}
	log.Println("Installing and launching base agent")
	err = InstallBaseAgent(agentManifest)
	if err != nil {
		log.Fatalf("Could not install base agent: %s", err)
	}
	if err := VerifyReality(30*time.Second, consulManifest.ID(), agentManifest.ID()); err != nil {
		log.Fatalln(err)
	}
	log.Println("Bootstrapping complete")
}

func InstallConsul(consulPod *pods.Pod, consulManifest pods.Manifest) error {
	// Inject servicebuilder?
	err := consulPod.Install(consulManifest)
	if err != nil {
		return util.Errorf("Can't install Consul, aborting: %s", err)
	}
	ok, err := consulPod.Launch(consulManifest)
	if err != nil || !ok {
		return util.Errorf("Can't launch Consul, aborting: %s", err)
	}
	return nil
}

func VerifyConsulUp(timeout string) error {
	timeoutDur, err := time.ParseDuration(timeout)
	if err != nil {
		return err
	}
	if timeoutDur == 0 {
		return nil
	}

	store := kp.NewConsulStore(kp.Options{
		Token: *consulToken, // not actually necessary because this endpoint is unauthenticated
	})
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

func VerifyReality(waitTime time.Duration, consulID, agentID string) error {
	quit := make(chan struct{})
	defer close(quit)
	store := kp.NewConsulStore(kp.Options{
		Token: *consulToken,
	})
	hostname, _ := os.Hostname()
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
			results, _, err := store.ListPods(kp.RealityPath(hostname))
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

func ScheduleForThisHost(manifest pods.Manifest, alsoReality bool) error {
	store := kp.NewConsulStore(kp.Options{
		Token: *consulToken,
	})
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	_, err = store.SetPod(kp.IntentPath(hostname, manifest.ID()), manifest)
	if err != nil {
		return err
	}

	if alsoReality {
		_, err = store.SetPod(kp.RealityPath(hostname, manifest.ID()), manifest)
		if err != nil {
			return err
		}
		return store.RegisterService(manifest, "")
	}
	return nil
}

func InstallBaseAgent(agentManifest pods.Manifest) error {
	agentPod := pods.NewPod(agentManifest.ID(), pods.PodPath(*podRoot, agentManifest.ID()))
	err := agentPod.Install(agentManifest)
	if err != nil {
		return err
	}
	_, err = agentPod.Launch(agentManifest)
	return err
}
