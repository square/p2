// Package replication provides logic for safely creating and updating
// pod manifests for multiple nodes as specified by an allocation.Allocation.
// Healthchecks and version tags are used to establish safety. Replicators will
// try forever until either the pods are shown to be updated or a signal is sent
// on the passed stop channel.
package replication

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/allocation"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
)

type Replicator struct {
	MinimumNodes  int
	NodePauseTime time.Duration
	timeout       chan struct{}
	Manifest      pods.Manifest
	Allocation    allocation.Allocation
	Logger        logging.Logger
}

type ServiceChecker interface {
	LookupHealth(serviceID string) (*health.ServiceStatus, error)
}

type IntentStore interface {
	SetPod(node string, manifest pods.Manifest) (time.Duration, error)
}

func NewReplicator(manifest pods.Manifest, allocated allocation.Allocation) *Replicator {
	sha, _ := manifest.SHA()
	replicator := Replicator{
		MinimumNodes: 1,
		Manifest:     manifest,
		Allocation:   allocated,
		Logger: logging.NewLogger(logrus.Fields{
			"pod": manifest.ID(),
			"sha": sha,
		}),
		NodePauseTime: 10 * time.Second,
	}
	return &replicator
}

// Enact will try forever to upgrade the given nodes to the version specified and ensure that the minimum
// number of healthy nodes is met or exceeded. If an error occurs while attempting to roll out, it will
// log the error and retry. This loop can be terminated by closing the provided stop channel.
func (repl *Replicator) Enact(store IntentStore, serviceChecker ServiceChecker, stop <-chan struct{}) {
	if repl.MinimumNodes >= len(repl.Allocation.Nodes) {
		repl.Logger.WithFields(logrus.Fields{
			"minimum":   repl.MinimumNodes,
			"allocated": len(repl.Allocation.Nodes),
		}).Errorln("Minimum nuber of nodes cannot meet or exceed allocated")
		return
	}
	repl.Logger.NoFields().Infoln("Enacting replication")
	status, err := serviceChecker.LookupHealth(repl.Manifest.ID())
	if err != nil {
		repl.Logger.WithField("err", err).Errorln("Couldn't communicate with health checker on first run")
		return
	}

	// see rollout_order.go
	toUpdatechannel := getRolloutOrder(repl.Allocation, status)

	healthCh := make(chan health.ServiceStatus)
	errCh := make(chan error)
	quitCh := make(chan struct{})
	defer close(quitCh)
	go repl.healthAggregateStream(serviceChecker, healthCh, errCh, quitCh)

	currentlyUpdating := make([]allocation.Node, len(repl.Allocation.Nodes)-repl.MinimumNodes)

	for i := 0; i < len(currentlyUpdating); i++ {
		currentlyUpdating[i] = <-toUpdatechannel
	}

	leftToUpdate := len(repl.Allocation.Nodes)
	for {
		select {
		case <-stop:
			repl.Logger.WithField("remaining", leftToUpdate).Warnln("Replicator canceled")
			return
		case err := <-errCh:
			repl.Logger.WithField("err", err).Errorln("encountered error while polling health checks")
		case serviceStatus := <-healthCh:
			for i, node := range currentlyUpdating {
				if !node.Valid() {
					// we've run out of nodes to process because the channel
					// is now returning empty nodes
					continue
				}
				status, err := serviceStatus.ForNode(node.Name)

				healthy, current := repl.isHealthyAndCurrent(status, err)
				if healthy && current {
					leftToUpdate--
					currentlyUpdating[i] = <-toUpdatechannel
					repl.Logger.WithFields(logrus.Fields{
						"node":           node.Name,
						"next":           currentlyUpdating[i].Name,
						"left_to_update": leftToUpdate,
					}).Infoln("Node is now current and healthy")
				} else if !current {
					// node is out of date, update the store with the new manifest.
					// this will almost definitely result in duplicate Sets in the
					// preparer, which is expected to ignore duplicate updates.
					repl.Logger.WithField("node", node.Name).Infoln("Updating node")
					dur, err := store.SetPod(kp.IntentPath(node.Name, repl.Manifest.ID()), repl.Manifest)
					if err != nil {
						repl.Logger.WithFields(logrus.Fields{
							"err":      err,
							"node":     node.Name,
							"duration": dur,
						}).Errorln("Could not update intent store with manifest")
					}
				} else if current && !healthy {
					repl.Logger.WithField("node", node.Name).Infoln("Up to date, waiting for health checks to pass")
				}
			}
			if leftToUpdate == 0 {
				repl.Logger.NoFields().Infoln("Replication completed successfully")
				return
			}
		}
	}
}

func (repl *Replicator) isHealthyAndCurrent(status *health.ServiceNodeStatus, err error) (bool, bool) {
	if err == health.NoStatusGiven {
		repl.Logger.WithFields(logrus.Fields{
			"node": status.Node,
		}).Infoln("No status found for node, treating as unhealthy")
		return false, false
	} else if err == nil {
		sha, _ := repl.Manifest.SHA()
		repl.Logger.WithField("status", status).Debugln("Comparing")
		return status.Healthy, status.IsCurrentVersion(sha)
	} else {
		repl.Logger.WithFields(logrus.Fields{
			"err":  err,
			"node": status.Node,
		}).Errorln("Error looking up health for node, treating as unhealthy")
		return false, false
	}
}

func (repl *Replicator) healthAggregateStream(serviceChecker ServiceChecker, healthCh chan health.ServiceStatus, errCh chan error, quitCh chan struct{}) {
	lookupCount := 0
	for {
		status, err := serviceChecker.LookupHealth(repl.Manifest.ID())
		if err == nil {
			select {
			case <-quitCh:
				return
			case healthCh <- *status:
			}
		} else {
			select {
			case <-quitCh:
				return
			case errCh <- err:
			}
		}
		repl.Logger.WithField("lookup_count", lookupCount).Debugln("Sent health, sleeping")
		lookupCount++
		time.Sleep(repl.NodePauseTime)
	}
}
