package consultest

import (
	"sync"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
)

const (
	checkRate = time.Duration(100 * time.Millisecond)
)

type FakePreparer struct {
	podStore PodStore

	enabled        bool
	enableLock     sync.Mutex
	preparerQuitCh chan struct{}
	logger         logging.Logger
}

type PodStore interface {
	AllPods(podPrefix consul.PodPrefix) ([]consul.ManifestResult, time.Duration, error)
	SetPod(podPrefix consul.PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
	DeletePod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (time.Duration, error)
}

func NewFakePreparer(podStore PodStore, logger logging.Logger) *FakePreparer {
	return &FakePreparer{
		podStore: podStore,
		enabled:  false,
		logger:   logger,
	}
}

func (f *FakePreparer) Enable() {
	f.enableLock.Lock()
	defer f.enableLock.Unlock()

	if f.enabled {
		return
	}
	f.enabled = true
	f.preparerQuitCh = make(chan struct{})

	go func() {
		for {
			select {
			case <-f.preparerQuitCh:
				return
			case <-time.Tick(checkRate):
				// Get all pods
				allPods, _, err := f.podStore.AllPods(consul.INTENT_TREE)
				if err != nil {
					f.logger.Errorf("Error getting all pods: %v", err)
					continue
				}

				intentPods := make(map[types.PodLocation]manifest.Manifest)
				for _, pod := range allPods {
					intentPods[pod.PodLocation] = pod.Manifest
				}

				allReality, _, err := f.podStore.AllPods(consul.REALITY_TREE)
				if err != nil {
					f.logger.Errorf("error getting all reality pods: %v", err)
					continue
				}

				realityPods := make(map[types.PodLocation]manifest.Manifest)
				for _, pod := range allReality {
					realityPods[pod.PodLocation] = pod.Manifest
				}

				// Set pods that are in intent
				for podLocation, intentManifest := range intentPods {
					intentSHA, err := intentManifest.SHA()
					if err != nil {
						f.logger.Errorf("error computing intent sha: %v", err)
						continue
					}

					var realitySHA string
					if realityManifest, ok := realityPods[podLocation]; ok {
						realitySHA, err = realityManifest.SHA()
						if err != nil {
							f.logger.Errorf("error computing reality sha: %v", err)
							continue
						}
					}

					if realitySHA != intentSHA {
						_, err = f.podStore.SetPod(
							consul.REALITY_TREE,
							podLocation.Node,
							intentManifest,
						)
						if err != nil {
							f.logger.Errorf("Error setting pod: %v", err)
						}
					}
				}

				// Delete pods that are missing from intent
				for _, manifestResult := range allReality {
					found := false
					for _, intentPod := range allPods {
						if intentPod.PodLocation.Node == manifestResult.PodLocation.Node &&
							intentPod.PodLocation.PodID == manifestResult.PodLocation.PodID {
							found = true
							break
						}
					}

					if !found {
						_, err = f.podStore.DeletePod(
							consul.REALITY_TREE,
							manifestResult.PodLocation.Node,
							manifestResult.PodLocation.PodID,
						)
						if err != nil {
							f.logger.Errorf("Error deleting pod: %v", err)
						}
					}
				}
			}
		}
	}()
}

func (f *FakePreparer) Disable() {
	f.enableLock.Lock()
	defer f.enableLock.Unlock()

	if !f.enabled {
		return
	}

	f.enabled = false
	close(f.preparerQuitCh)
}
