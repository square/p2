package preparer

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/constants"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/statusstore"
	"github.com/square/p2/pkg/kp/statusstore/podstatus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/size"
)

// The Pod ID of the preparer.
// Used because the preparer special-cases itself in a few places.
const (
	minimumBackoffTime = 1 * time.Second
)

// slice literals are not const
var svlogdExec = []string{"svlogd", "-tt", "./main"}

type Pod interface {
	hooks.Pod
	Launch(store.Manifest) (bool, error)
	Install(store.Manifest, auth.ArtifactVerifier, artifact.Registry) error
	Uninstall() error
	Verify(store.Manifest, auth.Policy) error
	Halt(store.Manifest) (bool, error)
	Prune(size.ByteCount, store.Manifest)
}

type Hooks interface {
	RunHookType(hookType hooks.HookType, pod hooks.Pod, manifest store.Manifest) error
}

type Store interface {
	ListPods(podPrefix kp.PodPrefix, nodeName types.NodeName) ([]kp.ManifestResult, time.Duration, error)
	SetPod(podPrefix kp.PodPrefix, nodeName types.NodeName, podManifest store.Manifest) (time.Duration, error)
	Pod(podPrefix kp.PodPrefix, nodeName types.NodeName, podId types.PodID) (store.Manifest, time.Duration, error)
	DeletePod(podPrefix kp.PodPrefix, nodeName types.NodeName, podId types.PodID) (time.Duration, error)
	WatchPods(
		podPrefix kp.PodPrefix,
		nodeName types.NodeName,
		quitChan <-chan struct{},
		errorChan chan<- error,
		podChan chan<- []kp.ManifestResult,
	)
}

// Identifies a pod which will be serviced by a goroutine. This struct is used
// in maps that store goroutine-specific resources such as channels for
// interaction
type podWorkerID struct {
	// Expected to be "" for legacy pods
	podUniqueKey types.PodUniqueKey

	podID types.PodID
}

// Useful in logging messages
func (p podWorkerID) String() string {
	if p.podUniqueKey == "" {
		return p.podID.String()
	}

	return fmt.Sprintf("%s-%s", p.podID.String(), p.podUniqueKey)
}

func (p *Preparer) WatchForPodManifestsForNode(quitAndAck chan struct{}) {
	pods.Log = p.Logger

	// This allows us to signal the goroutine watching consul to quit
	quitChan := make(chan struct{})
	errChan := make(chan error)
	podChan := make(chan []kp.ManifestResult)

	go p.store.WatchPods(kp.INTENT_TREE, p.node, quitChan, errChan, podChan)

	podChanMap := make(map[podWorkerID]chan ManifestPair)
	quitChanMap := make(map[podWorkerID]chan struct{})

	for {
		select {
		case err := <-errChan:
			p.Logger.WithError(err).
				Errorln("there was an error reading the manifest")
		case intentResults := <-podChan:
			realityResults, _, err := p.store.ListPods(kp.REALITY_TREE, p.node)
			if err != nil {
				p.Logger.WithError(err).Errorln("Could not check reality")
			} else {
				// if the preparer's own ID is missing from the intent set, we
				// assume it was damaged and discard it
				if !checkResultsForID(intentResults, constants.PreparerPodID) {
					p.Logger.NoFields().Errorln("Intent results set did not contain p2-preparer pod ID, consul data may be corrupted")
				} else {
					pairs := p.ZipResultSets(intentResults, realityResults)

					for _, pair := range pairs {
						workerID := podWorkerID{
							podID:        pair.ID,
							podUniqueKey: pair.PodUniqueKey,
						}
						if _, ok := podChanMap[workerID]; !ok {
							// spin goroutine for this pod
							podChanMap[workerID] = make(chan ManifestPair)
							quitChanMap[workerID] = make(chan struct{})
							go p.handlePods(podChanMap[workerID], quitChanMap[workerID])
						}
						// It is possible for the goroutine responsible for performing the installation
						// of a particular pod ID to be stalled or mid-deploy. This should not cause
						// this loop to block. Intent results will be re-sent within the watch expiration
						// loop time.
						select {
						case podChanMap[workerID] <- pair:
						case <-time.After(5 * time.Second):
							p.Logger.WithField("pod", pair.ID).Warnln("Missed possible manifest update, will wait for next watch.")
						}
					}

				}
			}
		case <-quitAndAck:
			for podToQuit, quitCh := range quitChanMap {
				p.Logger.WithFields(logrus.Fields{
					"pod":        podToQuit.podID,
					"unique_key": podToQuit.podUniqueKey,
				}).Infof("p2-preparer quitting, ceasing to watch for updates to %s", podToQuit.String())
				quitCh <- struct{}{}
			}
			close(quitChan)
			p.Logger.NoFields().Infoln("Done, acknowledging quit")
			quitAndAck <- struct{}{} // acknowledge quit
			return
		}

	}
}

func (p *Preparer) tryRunHooks(hookType hooks.HookType, pod hooks.Pod, manifest store.Manifest, logger logging.Logger) {
	err := p.hooks.RunHookType(hookType, pod, manifest)
	if err != nil {
		logger.WithErrorAndFields(err, logrus.Fields{
			"hooks": hookType}).Warnln("Could not run hooks")
	}
}

// no return value, no output channels. This should do everything it needs to do
// without outside intervention (other than being signalled to quit)
func (p *Preparer) handlePods(podChan <-chan ManifestPair, quit <-chan struct{}) {
	// install new launchables
	var nextLaunch ManifestPair

	// used to track if we have work to do (i.e. pod manifest came through channel
	// and we have yet to operate on it)
	working := false
	var manifestLogger logging.Logger

	// The design of p2-preparer is to continuously retry installation
	// failures, for example downloading of the launchable. An exponential
	// backoff is important to avoid putting undue load on the artifact
	// server, for example.
	backoffTime := minimumBackoffTime
	for {
		select {
		case <-quit:
			return
		case nextLaunch = <-podChan:
			backoffTime = minimumBackoffTime
			var sha string

			// TODO: handle errors appropriately from SHA().
			if nextLaunch.Intent != nil {
				sha, _ = nextLaunch.Intent.SHA()
			} else {
				sha, _ = nextLaunch.Reality.SHA()
			}
			manifestLogger = p.Logger.SubLogger(logrus.Fields{
				"pod":            nextLaunch.ID,
				"sha":            sha,
				"pod_unique_key": nextLaunch.PodUniqueKey,
			})
			manifestLogger.NoFields().Debugln("New manifest received")

			working = true
		case <-time.After(backoffTime):
			if working {
				var pod *pods.Pod
				var err error
				if nextLaunch.PodUniqueKey == "" {
					pod = p.podFactory.NewLegacyPod(nextLaunch.ID)
				} else {
					pod, err = p.podFactory.NewUUIDPod(nextLaunch.ID, nextLaunch.PodUniqueKey)
					if err != nil {
						manifestLogger.WithError(err).Errorln("Could not initialize pod")
						break
					}
				}

				// TODO better solution: force the preparer to have a 0s default timeout, prevent KILLs
				if pod.Id == constants.PreparerPodID {
					pod.DefaultTimeout = time.Duration(0)
				}

				effectiveLogBridgeExec := p.logExec
				// pods that are in the blacklist for this preparer shall not use the
				// preparer's log exec. Instead, they will use the default svlogd logexec.
				for _, podID := range p.logBridgeBlacklist {
					if pod.Id.String() == podID {
						effectiveLogBridgeExec = svlogdExec
						break
					}
				}
				pod.SetLogBridgeExec(effectiveLogBridgeExec)

				pod.SetFinishExec(p.finishExec)

				// podChan is being fed values gathered from a kp.Watch() in
				// WatchForPodManifestsForNode(). If the watch returns a new pair of
				// intent/reality values before the previous change has finished
				// processing in resolvePair(), the reality value will be stale. This
				// leads to a bug where the preparer will appear to update a package
				// and when that is finished, "update" it again.
				//
				// Example ordering of bad events:
				// 1) update to /intent for pod A comes in, /reality is read and
				// resolvePair() handles it
				// 2) before resolvePair() finishes, another /intent update comes in,
				// and /reality is read but hasn't been changed. This update cannot
				// be processed until the previous resolvePair() call finishes, and
				// updates /reality. Now the reality value used here is stale. We
				// want to refresh our /reality read so we don't restart the pod if
				// intent didn't change between updates.
				//
				// The correct solution probably involves watching reality and intent
				// and feeding updated pairs to a control loop.
				//
				// This is a quick fix to ensure that the reality value being used is
				// up-to-date. The de-bouncing logic in this method should ensure that the
				// intent value is fresh (to the extent that Consul is timely). Fetching
				// the reality value again ensures its freshness too.
				if nextLaunch.PodUniqueKey == "" {
					// legacy pod, get reality manifest from reality tree
					reality, _, err := p.store.Pod(kp.REALITY_TREE, p.node, nextLaunch.ID)
					if err == pods.NoCurrentManifest {
						nextLaunch.Reality = nil
					} else if err != nil {
						manifestLogger.WithError(err).Errorln("Error getting reality manifest")
						break
					} else {
						nextLaunch.Reality = reality
					}
				} else {
					// uuid pod, get reality manifest from pod status
					status, _, err := p.podStatusStore.Get(nextLaunch.PodUniqueKey)
					switch {
					case err != nil && !statusstore.IsNoStatus(err):
						manifestLogger.WithError(err).Errorln("Error getting reality manifest from pod status")
						break
					case statusstore.IsNoStatus(err):
						nextLaunch.Reality = nil
					default:
						manifest, err := store.FromBytes([]byte(status.Manifest))
						if err != nil {
							manifestLogger.WithError(err).Errorln("Error parsing reality manifest from pod status")
							break
						}
						nextLaunch.Reality = manifest
					}
				}

				ok := p.resolvePair(nextLaunch, pod, manifestLogger)
				if ok {
					nextLaunch = ManifestPair{}
					working = false

					// Reset the backoff time
					backoffTime = minimumBackoffTime
				} else {
					// Double the backoff time with a maximum of 1 minute
					backoffTime = backoffTime * 2
					if backoffTime > 1*time.Minute {
						backoffTime = 1 * time.Minute
					}
				}
			}
		}
	}
}

// check if a manifest satisfies the authorization requirement of this preparer
func (p *Preparer) authorize(manifest store.Manifest, logger logging.Logger) bool {
	err := p.authPolicy.AuthorizeApp(manifest, logger)
	if err != nil {
		if err, ok := err.(auth.Error); ok {
			logger.WithFields(err.Fields).Errorln(err)
		} else {
			logger.NoFields().Errorln(err)
		}
		return false
	}
	return true
}

func (p *Preparer) resolvePair(pair ManifestPair, pod Pod, logger logging.Logger) bool {
	// do not remove the logger argument, it's not the same as p.Logger
	var oldSHA, newSHA string
	if pair.Reality != nil {
		oldSHA, _ = pair.Reality.SHA()
	}
	if pair.Intent != nil {
		newSHA, _ = pair.Intent.SHA()
	}

	if oldSHA == "" && newSHA != "" {
		logger.NoFields().Infoln("manifest is new, will update")
		authorized := p.authorize(pair.Intent, logger)
		if !authorized {
			p.tryRunHooks(
				hooks.AFTER_AUTH_FAIL,
				pod,
				pair.Intent,
				logger,
			)
			// prevent future unnecessary loops, we don't need to check again.
			return true
		}
		return p.installAndLaunchPod(pair, pod, logger)
	}

	if newSHA == "" {
		logger.NoFields().Infoln("manifest was deleted from intent, will remove")
		return p.stopAndUninstallPod(pair, pod, logger)
	}

	if oldSHA == newSHA {
		logger.NoFields().Debugln("manifest is unchanged, no action required")
		return true
	}

	authorized := p.authorize(pair.Intent, logger)
	if !authorized {
		p.tryRunHooks(
			hooks.AFTER_AUTH_FAIL,
			pod,
			pair.Intent,
			logger,
		)
		// prevent future unnecessary loops, we don't need to check again.
		return true
	}

	logger.WithField("old_sha", oldSHA).Infoln("manifest SHA has changed, will update")
	return p.installAndLaunchPod(pair, pod, logger)

}

func (p *Preparer) installAndLaunchPod(pair ManifestPair, pod Pod, logger logging.Logger) bool {
	p.tryRunHooks(hooks.BEFORE_INSTALL, pod, pair.Intent, logger)

	logger.NoFields().Infoln("Installing pod and launchables")

	err := pod.Install(pair.Intent, p.artifactVerifier, p.artifactRegistry)
	if err != nil {
		// install failed, abort and retry
		logger.WithError(err).Errorln("Install failed")
		return false
	}

	err = pod.Verify(pair.Intent, p.authPolicy)
	if err != nil {
		logger.WithError(err).
			Errorln("Pod digest verification failed")
		p.tryRunHooks(hooks.AFTER_AUTH_FAIL, pod, pair.Intent, logger)
		return false
	}

	p.tryRunHooks(hooks.AFTER_INSTALL, pod, pair.Intent, logger)

	if pair.Reality != nil {
		logger.NoFields().Infoln("Invoking the disable hook and halting runit services")
		success, err := pod.Halt(pair.Reality)
		if err != nil {
			logger.WithError(err).
				Errorln("Pod halt failed")
		} else if !success {
			logger.NoFields().Warnln("One or more launchables did not halt successfully")
		}
	}

	p.tryRunHooks(hooks.BEFORE_LAUNCH, pod, pair.Intent, logger)

	logger.NoFields().Infoln("Setting up new runit services and running the enable hook")

	ok, err := pod.Launch(pair.Intent)
	if err != nil {
		logger.WithError(err).
			Errorln("Launch failed")
	} else {
		if pair.PodUniqueKey == "" {
			// legacy pod, write the manifest back to reality tree
			duration, err := p.store.SetPod(kp.REALITY_TREE, p.node, pair.Intent)
			if err != nil {
				logger.WithErrorAndFields(err, logrus.Fields{
					"duration": duration}).
					Errorln("Could not set pod in reality store")
			}
		} else {
			// TODO: do this in a transaction
			err = p.podStore.WriteRealityIndex(pair.PodUniqueKey, p.node)
			if err != nil {
				logger.WithError(err).
					Errorln("Could not write uuid index to reality store")
			}

			// uuid pod, write the manifest to the pod status tree.
			mutator := func(ps podstatus.PodStatus) (podstatus.PodStatus, error) {
				manifestBytes, err := pair.Intent.Marshal()
				if err != nil {
					return ps, util.Errorf("Could not convert manifest to string to update pod status")
				}

				ps.PodStatus = podstatus.PodLaunched
				ps.Manifest = string(manifestBytes)
				return ps, nil
			}
			err := p.podStatusStore.MutateStatus(pair.PodUniqueKey, mutator)
			if err != nil {
				logger.WithError(err).Errorln("Could not update manifest in pod status")
			}
		}

		p.tryRunHooks(hooks.AFTER_LAUNCH, pod, pair.Intent, logger)

		pod.Prune(p.maxLaunchableDiskUsage, pair.Intent) // errors are logged internally
	}
	return err == nil && ok
}

func (p *Preparer) stopAndUninstallPod(pair ManifestPair, pod Pod, logger logging.Logger) bool {
	success, err := pod.Halt(pair.Reality)
	if err != nil {
		logger.WithError(err).Errorln("Pod halt failed")
	} else if !success {
		logger.NoFields().Warnln("One or more launchables did not halt successfully")
	}

	p.tryRunHooks(hooks.BEFORE_UNINSTALL, pod, pair.Reality, logger)

	err = pod.Uninstall()
	if err != nil {
		logger.WithError(err).Errorln("Uninstall failed")
		return false
	}
	logger.NoFields().Infoln("Successfully uninstalled")

	if pair.PodUniqueKey == "" {
		dur, err := p.store.DeletePod(kp.REALITY_TREE, p.node, pair.ID)
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{"duration": dur}).
				Errorln("Could not delete pod from reality store")
		}
	} else {
		// We don't delete so that the exit status of the pod's
		// processes can be viewed for some time after installation.
		// It is the responsibility of external systems to delete pod
		// status entries when they are no longer needed.
		err := p.podStatusStore.MutateStatus(pair.PodUniqueKey, func(podStatus podstatus.PodStatus) (podstatus.PodStatus, error) {
			podStatus.PodStatus = podstatus.PodRemoved
			return podStatus, nil
		})
		if err != nil {
			logger.WithError(err).
				Errorln("Could not update pod status to reflect removal")
		}

		err = p.podStore.DeleteRealityIndex(pair.PodUniqueKey, p.node)
		if err != nil {
			logger.WithError(err).
				Errorln("Could not remove reality index for uninstalled pod")
		}
	}
	return true
}

// Close() releases any resources held by a Preparer.
func (p *Preparer) Close() {
	p.authPolicy.Close()
	p.authPolicy = nil
}
