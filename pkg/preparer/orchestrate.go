package preparer

import (
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
)

// The Pod ID of the preparer.
// Used because the preparer special-cases itself in a few places.
const POD_ID = "p2-preparer"

type Pod interface {
	hooks.Pod
	Launch(*pods.Manifest) (bool, error)
	Install(*pods.Manifest) error
	Uninstall() error
	Verify(*pods.Manifest, auth.Policy) error
	Halt(*pods.Manifest) (bool, error)
}

type Hooks interface {
	RunHookType(hookType hooks.HookType, pod hooks.Pod, manifest *pods.Manifest) error
}

type Store interface {
	ListPods(keyPrefix string) ([]kp.ManifestResult, time.Duration, error)
	SetPod(string, pods.Manifest) (time.Duration, error)
	DeletePod(key string) (time.Duration, error)
	RegisterService(pods.Manifest, string) error
	WatchPods(string, <-chan struct{}, chan<- error, chan<- []kp.ManifestResult)
}

type Preparer struct {
	node         string
	store        Store
	hooks        Hooks
	hookListener HookListener
	Logger       logging.Logger
	podRoot      string
	caFile       string
	authPolicy   auth.Policy
	consulHealth bool
}

func (p *Preparer) WatchForHooks(quit chan struct{}) {
	hookErrCh := make(chan error)
	hookQuitCh := make(chan struct{})

	go p.hookListener.Sync(hookQuitCh, hookErrCh)
	for {
		select {
		case <-quit:
			hookQuitCh <- struct{}{}
			return
		case err := <-hookErrCh:
			p.Logger.WithError(err).Errorln("Error updating hooks")
		}
	}
}

func (p *Preparer) WatchForPodManifestsForNode(quitAndAck chan struct{}) {
	pods.Log = p.Logger
	path := kp.IntentPath(p.node)

	// This allows us to signal the goroutine watching consul to quit
	quitChan := make(chan struct{})
	errChan := make(chan error)
	podChan := make(chan []kp.ManifestResult)

	go p.store.WatchPods(path, quitChan, errChan, podChan)

	// we will have one long running goroutine for each app installed on this
	// host. We keep a map of podId => podChan so we can send the new manifests
	// that come in to the appropriate goroutine
	podChanMap := make(map[string]chan ManifestPair)
	// we can't use a shared quit channel for all the goroutines - otherwise,
	// we would exit the program before the goroutines actually accepted the
	// quit signal. to be sure that each goroutine is done, we have to block and
	// wait for it to receive the signal
	quitChanMap := make(map[string]chan struct{})

	for {
		select {
		case err := <-errChan:
			p.Logger.WithError(err).
				Errorln("there was an error reading the manifest")
		case intentResults := <-podChan:
			realityResults, _, err := p.store.ListPods(kp.RealityPath(p.node))
			if err != nil {
				errChan <- err
			} else {
				// if the preparer's own ID is missing from the intent set, we
				// assume it was damaged and discard it
				if !checkResultsForID(intentResults, POD_ID) {
					p.Logger.NoFields().Errorln("Intent results set did not contain p2-preparer pod ID, consul data may be corrupted")
				} else {
					resultPairs := ZipResultSets(intentResults, realityResults)
					for _, pair := range resultPairs {
						if _, ok := podChanMap[pair.ID]; !ok {
							// spin goroutine for this pod
							podChanMap[pair.ID] = make(chan ManifestPair)
							quitChanMap[pair.ID] = make(chan struct{})
							go p.handlePods(podChanMap[pair.ID], quitChanMap[pair.ID])
						}
						podChanMap[pair.ID] <- pair
					}
				}
			}
		case <-quitAndAck:
			for podToQuit, quitCh := range quitChanMap {
				p.Logger.WithField("pod", podToQuit).Infoln("Quitting...")
				quitCh <- struct{}{}
			}
			close(quitChan)
			p.Logger.NoFields().Infoln("Done, acknowledging quit")
			quitAndAck <- struct{}{} // acknowledge quit
			return
		}

	}
}

func (p *Preparer) tryRunHooks(hookType hooks.HookType, pod hooks.Pod, manifest *pods.Manifest, logger logging.Logger) {
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
	for {
		select {
		case <-quit:
			return
		case nextLaunch = <-podChan:
			var sha string
			if nextLaunch.Intent != nil {
				sha, _ = nextLaunch.Intent.SHA()
			} else {
				sha, _ = nextLaunch.Reality.SHA()
			}
			manifestLogger = p.Logger.SubLogger(logrus.Fields{
				"pod": nextLaunch.ID,
				"sha": sha,
			})
			manifestLogger.NoFields().Debugln("New manifest received")

			if nextLaunch.Intent == nil {
				// if intent=nil then reality!=nil and we need to delete the pod
				// therefore we must set working=true here
				working = true
			} else {
				// non-nil intent manifests need to be authorized first
				working = p.authorize(*nextLaunch.Intent, manifestLogger)
				if !working {
					p.tryRunHooks(hooks.AFTER_AUTH_FAIL, pods.NewPod(nextLaunch.ID, pods.PodPath(p.podRoot, nextLaunch.ID)), nextLaunch.Intent, manifestLogger)
				}
			}
		case <-time.After(1 * time.Second):
			if working {
				pod := pods.NewPod(nextLaunch.ID, pods.PodPath(p.podRoot, nextLaunch.ID))

				// TODO better solution: force the preparer to have a 0s default timeout, prevent KILLs
				if pod.Id == POD_ID {
					pod.DefaultTimeout = time.Duration(0)
				}

				ok := p.resolvePair(nextLaunch, pod, manifestLogger)
				if ok {
					nextLaunch = ManifestPair{}
					working = false
				}
			}
		}
	}
}

// check if a manifest satisfies the authorization requirement of this preparer
func (p *Preparer) authorize(manifest pods.Manifest, logger logging.Logger) bool {
	err := p.authPolicy.AuthorizeApp(&manifest, logger)
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

	if oldSHA == "" {
		logger.NoFields().Infoln("manifest is new, will update")
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

	logger.WithField("old_sha", oldSHA).Infoln("manifest SHA has changed, will update")
	return p.installAndLaunchPod(pair, pod, logger)

}

func (p *Preparer) installAndLaunchPod(pair ManifestPair, pod Pod, logger logging.Logger) bool {
	p.tryRunHooks(hooks.BEFORE_INSTALL, pod, pair.Intent, logger)

	err := pod.Install(pair.Intent)
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

	if p.consulHealth {
		err = p.store.RegisterService(*pair.Intent, p.caFile)
		if err != nil {
			logger.WithError(err).Errorln("Service registration failed")
			return false
		}
	}

	if pair.Reality != nil {
		success, err := pod.Halt(pair.Reality)
		if err != nil {
			logger.WithError(err).
				Errorln("Pod halt failed")
		} else if !success {
			logger.NoFields().Warnln("One or more launchables did not halt successfully")
		}
	}

	p.tryRunHooks(hooks.BEFORE_LAUNCH, pod, pair.Intent, logger)

	ok, err := pod.Launch(pair.Intent)
	if err != nil {
		logger.WithError(err).
			Errorln("Launch failed")
	} else {
		duration, err := p.store.SetPod(kp.RealityPath(p.node, pair.ID), *pair.Intent)
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{
				"duration": duration}).
				Errorln("Could not set pod in reality store")
		}

		p.tryRunHooks(hooks.AFTER_LAUNCH, pod, pair.Intent, logger)
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

	dur, err := p.store.DeletePod(kp.RealityPath(p.node, pair.ID))
	if err != nil {
		logger.WithErrorAndFields(err, logrus.Fields{"duration": dur}).
			Errorln("Could not delete pod from reality store")
	}
	return true
}

// Close() releases any resources held by a Preparer.
func (p *Preparer) Close() {
	p.authPolicy.Close()
	// The same verifier is shared twice internally
	p.hookListener.authPolicy = nil
	p.authPolicy = nil
}
