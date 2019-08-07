package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/flags"
	"github.com/square/p2/pkg/store/consul/pcstore"
	"github.com/square/p2/pkg/types"

	"sort"

	"github.com/sirupsen/logrus"
	"github.com/square/p2/pkg/health/checker"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	nodeName      = kingpin.Flag("node", "The node to do the scheduling on. Uses the hostname by default.").String()
	watchReality  = kingpin.Flag("reality", "Watch the reality store instead of the intent store. False by default").Default("false").Bool()
	hooks         = kingpin.Flag("hook", "Watch hooks.").Bool()
	podClusters   = kingpin.Flag("pod-clusters", "Watch pod clusters and their labeled pods").Bool()
	watchHealthF  = kingpin.Flag("health", "Watch health using HealthChecker").Bool()
	healthService = kingpin.Arg("health-pod", "Pod to watch. Required if --health is passed").String()
)

func main() {
	kingpin.Version(version.VERSION)
	_, opts, applicator := flags.ParseWithConsulOptions()
	client := consul.NewConsulClient(opts)
	store := consul.NewConsulStore(client)

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("Could not get the hostname to do scheduling: %s", err)
		}
		*nodeName = hostname
	}
	if *podClusters {
		watchPodClusters(client, applicator)
	} else if *watchHealthF {
		if *healthService == "" {
			log.Fatal("Refusing to watch entire health tree, please set a pod ID with --health-pod")
		}

		watchHealth(*healthService, client)
		return
	} else {
		podPrefix := consul.INTENT_TREE
		if *watchReality {
			podPrefix = consul.REALITY_TREE
		} else if *hooks {
			podPrefix = consul.HOOK_TREE
		}
		log.Printf("Watching manifests at %s/%s/\n", podPrefix, *nodeName)

		quit := make(chan struct{})
		errChan := make(chan error)
		podCh := make(chan []consul.ManifestResult)
		go store.WatchPods(podPrefix, types.NodeName(*nodeName), quit, errChan, podCh)
		for {
			select {
			case results := <-podCh:
				if len(results) == 0 {
					fmt.Println(fmt.Sprintf("No manifests exist for %s under %s (they may have been deleted)", *nodeName, podPrefix))
				} else {
					for _, result := range results {
						if err := result.Manifest.Write(os.Stdout); err != nil {
							log.Fatalf("write error: %v", err)
						}
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

func (p *printSyncer) Type() pcstore.ConcreteSyncerType {
	return "print_syncer"
}

func watchPodClusters(client consulutil.ConsulClient, applicator labels.ApplicatorWithoutWatches) {
	logger := &logging.DefaultLogger
	logger.Infoln("Beginning pod cluster watch")

	pcStore := pcstore.NewConsul(client, applicator, labels.DefaultAggregationRate, labels.NewConsulApplicator(client, 0, 0), logger)
	quitCh := make(chan struct{})
	go func() {
		signalCh := make(chan os.Signal, 2)
		signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)
		received := <-signalCh
		logger.Warnf("Received %v, shutting down", received)
		close(quitCh)
	}()

	if err := pcStore.WatchAndSync(&printSyncer{logger}, quitCh); err != nil {
		log.Fatalf("error watching pod cluster: %v", err)
	}
}

func watchHealth(service string, client consulutil.ConsulClient) {
	hc := checker.NewHealthChecker(client)
	healthResults := make(chan map[types.NodeName]health.Result)
	errCh := make(chan error)
	quitCh := make(chan struct{})
	ctx, cancelFunc := context.WithCancel(context.Background())
	watchDelay := 1 * time.Second
	go hc.WatchService(ctx, *healthService, healthResults, errCh, watchDelay)
	defer cancelFunc()

	go func() {
		signalCh := make(chan os.Signal, 2)
		signal.Notify(signalCh, syscall.SIGTERM, os.Interrupt)
		received := <-signalCh
		log.Printf("[WARN] Received %v, shutting down", received)
		close(quitCh)
	}()

	for {
		select {
		case hr := <-healthResults:
			var sortedHealthResults nodeHealthResults
			for _, r := range hr {
				sortedHealthResults = append(sortedHealthResults, r)
			}
			sort.Sort(sortedHealthResults)
			for _, r := range sortedHealthResults {
				fmt.Printf("%s %s\n", r.Node.String(), r.Status)
			}
			fmt.Printf("\n")
		case err := <-errCh:
			log.Printf("[ERROR] %v\n", err)
		case <-quitCh:
			os.Exit(0)
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

type nodeHealthResults []health.Result

func (hrs nodeHealthResults) Len() int {
	return len(hrs)
}

func (hrs nodeHealthResults) Less(i, j int) bool {
	return hrs[i].Node.String() < hrs[j].Node.String()
}

func (hrs nodeHealthResults) Swap(i, j int) {
	hrs[i], hrs[j] = hrs[j], hrs[i]
}
