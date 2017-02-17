package consultest

import (
	"sync"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul"
)

const (
	checkRate = time.Duration(100 * time.Millisecond)
)

type FakePreparer struct {
	podStore *FakePodStore

	enabled        bool
	enableLock     sync.Mutex
	preparerQuitCh chan struct{}
	logger         logging.Logger
}

func NewFakePreparer(podStore *FakePodStore, logger logging.Logger) *FakePreparer {
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

				// Set pods that are in intent
				for _, manifestResult := range allPods {
					_, err = f.podStore.SetPod(
						consul.REALITY_TREE,
						manifestResult.PodLocation.Node,
						manifestResult.Manifest,
					)
					if err != nil {
						f.logger.Errorf("Error setting pod: %v", err)
					}
				}

				allReality, _, err := f.podStore.AllPods(consul.REALITY_TREE)
				if err != nil {
					f.logger.Errorf("error getting all reality pods: %v", err)
					continue
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
