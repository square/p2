package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/kp/pcstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/types"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"
	"github.com/square/p2/pkg/version"
)

var (
	nodeName     = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	watchReality = kingpin.Flag("reality", "Watch the reality store instead of the intent store. False by default").Default("false").Bool()
	hooks        = kingpin.Flag("hook", "Watch hooks.").Bool()
	podClusters  = kingpin.Flag("pod-clusters", "Watch pod clusters and their labeled pods").Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(opts)
	store := kp.NewConsulStore(client)

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("Could not get the hostname to do scheduling: %s", err)
		}
		*nodeName = hostname
	}
	if *podClusters {
		watchPodClusters(client)
	} else {
		podPrefix := kp.INTENT_TREE
		if *watchReality {
			podPrefix = kp.REALITY_TREE
		} else if *hooks {
			podPrefix = kp.HOOK_TREE
		}
		log.Printf("Watching manifests at %s/%s/\n", podPrefix, *nodeName)

		quit := make(chan struct{})
		errChan := make(chan error)
		podCh := make(chan []kp.ManifestResult)
		go store.WatchPods(podPrefix, types.NodeName(*nodeName), quit, errChan, podCh)
		for {
			select {
			case results := <-podCh:
				if len(results) == 0 {
					fmt.Println(fmt.Sprintf("No manifests exist for %s under %s (they may have been deleted)", *nodeName, podPrefix))
				} else {
					for _, result := range results {
						fmt.Println("")
						result.Manifest.Write(os.Stdout)
					}
				}
			case err := <-errChan:
				log.Fatalf("Error occurred while listening to pods: %s", err)
			}
		}
	}
}

type printSyncer struct {
	logger *logging.Logger
}

func (p *printSyncer) SyncCluster(cluster *fields.PodCluster, pods []labels.Labeled) error {
	p.logger.WithFields(logrus.Fields{
		"id":   cluster.ID,
		"pod":  cluster.PodID,
		"name": cluster.Name,
		"az":   cluster.AvailabilityZone,
		"sel":  cluster.PodSelector.String(),
		"pods": fmt.Sprintf("%v", pods),
	}).Infoln("SyncCluster")
	return nil
}

func (p *printSyncer) DeleteCluster(id fields.ID) error {
	p.logger.WithField("id", id).Warnln("DeleteCluster")
	return nil
}

func (p *printSyncer) GetInitialClusters() ([]fields.ID, error) {
	return []fields.ID{}, nil
}

func watchPodClusters(client *api.Client) {
	logger := &logging.DefaultLogger
	logger.Infoln("Beginning pod cluster watch")

	pcStore := pcstore.NewConsul(client, 0, logger)
	quitCh := make(chan struct{})
	go func() {
		signalCh := make(chan os.Signal, 2)
		signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)
		received := <-signalCh
		logger.Warnf("Received %v, shutting down", received)
		close(quitCh)
	}()

	pcStore.WatchAndSync(&printSyncer{logger}, quitCh)
}
