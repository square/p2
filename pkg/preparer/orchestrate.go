package preparer

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"golang.org/x/crypto/openpgp"
)

// The Pod ID of the preparer.
// Used because the preparer special-cases itself in a few places.
const POD_ID = "p2-preparer"

type Pod interface {
	hooks.Pod
	EnsureHome() error
	Launch(*pods.Manifest) (bool, error)
	Install(*pods.Manifest) error
	Verify(*pods.Manifest, openpgp.KeyRing) error
	Halt(*pods.Manifest) (bool, error)
}

type Hooks interface {
	RunHookType(hookType hooks.HookType, pod hooks.Pod, manifest *pods.Manifest) error
}

type Store interface {
	Pod(string) (*pods.Manifest, time.Duration, error)
	SetPod(string, pods.Manifest) (time.Duration, error)
	RegisterService(pods.Manifest, string) error
	WatchPods(string, <-chan struct{}, chan<- error, chan<- kp.ManifestResult)
}

type Preparer struct {
	node                string
	store               Store
	hooks               Hooks
	hookListener        HookListener
	Logger              logging.Logger
	keyring             openpgp.KeyRing
	podRoot             string
	authorizedDeployers []string
	forbiddenPodIds     map[string]struct{}
	caPath              string
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
			p.Logger.WithField("err", err).Errorln("Error updating hooks")
		}
	}
}

func (p *Preparer) WatchForPodManifestsForNode(quitAndAck chan struct{}) {
	pods.Log = p.Logger
	path := kp.IntentPath(p.node)

	// This allows us to signal the goroutine watching consul to quit
	watcherQuit := make(<-chan struct{})
	errChan := make(chan error)
	podChan := make(chan kp.ManifestResult)

	go p.store.WatchPods(path, watcherQuit, errChan, podChan)

	// we will have one long running goroutine for each app installed on this
	// host. We keep a map of podId => podChan so we can send the new manifests
	// that come in to the appropriate goroutine
	podChanMap := make(map[string]chan pods.Manifest)
	quitChanMap := make(map[string]chan struct{})

	for {
		select {
		case err := <-errChan:
			p.Logger.WithFields(logrus.Fields{
				"inner_err": err,
			}).Errorln("there was an error reading the manifest")
		case result := <-podChan:
			podId := result.Manifest.ID()
			if podChanMap[podId] == nil {
				// No goroutine is servicing this app currently, let's start one
				podChanMap[podId] = make(chan pods.Manifest)
				quitChanMap[podId] = make(chan struct{})
				go p.handlePods(podChanMap[podId], quitChanMap[podId])
			}
			podChanMap[podId] <- result.Manifest
		case <-quitAndAck:
			for podToQuit, quitCh := range quitChanMap {
				p.Logger.WithField("pod", podToQuit).Infoln("Quitting...")
				quitCh <- struct{}{}
			}
			p.Logger.NoFields().Infoln("Done, acknowledging quit")
			quitAndAck <- struct{}{} // acknowledge quit
			return
		}

	}
}

func (p *Preparer) podIdForbidden(id string) bool {
	_, forbidden := p.forbiddenPodIds[id]
	return forbidden
}

func (p *Preparer) tryRunHooks(hookType hooks.HookType, pod hooks.Pod, manifest *pods.Manifest, logger logging.Logger) {
	err := p.hooks.RunHookType(hookType, pod, manifest)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":   err,
			"hooks": hookType,
		}).Warnln("Could not run hooks")
	}
}

// no return value, no output channels. This should do everything it needs to do
// without outside intervention (other than being signalled to quit)
func (p *Preparer) handlePods(podChan <-chan pods.Manifest, quit <-chan struct{}) {
	// install new launchables
	var manifestToLaunch pods.Manifest

	// used to track if we have work to do (i.e. pod manifest came through channel
	// and we have yet to operate on it)
	working := false
	var manifestLogger logging.Logger
	for {
		select {
		case <-quit:
			return
		case manifestToLaunch = <-podChan:
			sha, err := manifestToLaunch.SHA()
			manifestLogger = p.Logger.SubLogger(logrus.Fields{
				"manifest": manifestToLaunch.ID(),
				"sha":      sha,
				"sha_err":  err,
			})
			manifestLogger.NoFields().Debugln("New manifest received")

			working = p.verifySignature(manifestToLaunch, manifestLogger)
			if !working {
				p.tryRunHooks(hooks.AFTER_AUTH_FAIL, pods.NewPod(manifestToLaunch.ID(), pods.PodPath(p.podRoot, manifestToLaunch.ID())), &manifestToLaunch, manifestLogger)
			}
		case <-time.After(1 * time.Second):
			if working {
				pod := pods.NewPod(manifestToLaunch.ID(), pods.PodPath(p.podRoot, manifestToLaunch.ID()))

				// HACK ZONE. When we have better authz, rewrite.
				// Still need to ensure that preparer launches correctly
				// as root
				if pod.Id == POD_ID {
					pod.RunAs = "root"
				}

				ok := p.installAndLaunchPod(&manifestToLaunch, pod, manifestLogger)
				if ok {
					manifestToLaunch = pods.Manifest{}
					working = false
				}
			}
		}
	}
}

// check if a manifest satisfies the signature requirement of this preparer
func (p *Preparer) verifySignature(manifest pods.Manifest, logger logging.Logger) bool {
	// do not remove the logger argument, it's not the same as p.Logger
	if p.keyring == nil {
		// signature is fine if the preparer has not been required to have a keyring
		return true
	}

	signer, err := manifest.Signer(p.keyring)
	if signer != nil {
		signerId := signer.PrimaryKey.KeyIdShortString()
		logger.WithField("signer_key", signerId).Debugln("Resolved manifest signature")

		// Hmm, some hacks here.
		if manifest.Id == POD_ID && len(p.authorizedDeployers) > 0 {
			foundAuthorized := false
			for _, authorized := range p.authorizedDeployers {
				if authorized == signerId {
					foundAuthorized = true
				}
			}
			if !foundAuthorized {
				logger.WithField("signer_key", signerId).Errorln("Not an authorized deployer of the preparer")
				return false
			}
		}

		return true
	}

	if err == nil {
		logger.NoFields().Warnln("Received unsigned manifest (expected signature)")
	} else {
		logger.WithField("inner_err", err).Warnln("Error while resolving manifest signature")
	}
	return false
}

func (p *Preparer) installAndLaunchPod(newManifest *pods.Manifest, pod Pod, logger logging.Logger) bool {
	// do not remove the logger argument, it's not the same as p.Logger

	if p.podIdForbidden(newManifest.ID()) {
		logger.WithField("manifest", newManifest.ID()).Errorln("Cannot use this pod ID")
		p.tryRunHooks(hooks.AFTER_AUTH_FAIL, pod, newManifest, logger)
		return false
	}

	// get currently running pod to compare with the new pod
	realityPath := kp.RealityPath(p.node, newManifest.ID())
	currentManifest, _, err := p.store.Pod(realityPath)
	currentSHA := ""
	if currentManifest != nil {
		currentSHA, _ = currentManifest.SHA()
	}
	newSHA, _ := newManifest.SHA()

	// if new or the manifest is different, launch
	newOrDifferent := err == pods.NoCurrentManifest || currentSHA != newSHA
	if newOrDifferent {
		logger.WithFields(logrus.Fields{
			"old_sha":  currentSHA,
			"sha":      newSHA,
			"manifest": newManifest.ID(),
		}).Infoln("SHA is new or different from old, will update")
	}

	// if the old manifest is corrupted somehow, re-launch since we don't know if this is an update.
	problemReadingCurrentManifest := (err != nil && err != pods.NoCurrentManifest)
	if problemReadingCurrentManifest {
		logger.WithFields(logrus.Fields{
			"sha":       newSHA,
			"inner_err": err,
		}).Errorln("Current manifest not readable, will relaunch")
	}

	if newOrDifferent || problemReadingCurrentManifest {
		// Ensure that directories exist before executing hooks that may
		// write to those directories
		err = pod.EnsureHome()
		if err != nil {
			logger.WithField("err", err).Errorln("Could not set up pod home")
			return false
		}

		p.tryRunHooks(hooks.BEFORE_INSTALL, pod, newManifest, logger)

		err = pod.Install(newManifest)
		if err != nil {
			// install failed, abort and retry
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorln("Install failed")
			return false
		}

		err = pod.Verify(newManifest, p.keyring)
		if err != nil {
			logger.WithField("err", err).Errorln("Pod digest verification failed")
			p.tryRunHooks(hooks.AFTER_AUTH_FAIL, pod, newManifest, logger)
			return false
		}

		p.tryRunHooks(hooks.AFTER_INSTALL, pod, newManifest, logger)

		err = p.store.RegisterService(*newManifest, p.caPath)
		if err != nil {
			logger.WithField("err", err).Errorln("Service registration failed")
			return false
		}

		if currentManifest != nil {
			success, err := pod.Halt(currentManifest)
			if err != nil {
				logger.WithField("err", err).Errorln("Pod halt failed")
			} else if !success {
				logger.NoFields().Warnln("One or more launchables did not halt successfully")
			}
		}

		ok, err := pod.Launch(newManifest)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorln("Launch failed")
		} else {
			duration, err := p.store.SetPod(realityPath, *newManifest)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"err":      err,
					"duration": duration,
				}).Errorln("Could not set pod in reality store")
			}

			p.tryRunHooks(hooks.AFTER_LAUNCH, pod, newManifest, logger)
		}
		return err == nil && ok
	}

	// TODO: shut down removed launchables between pod versions.
	return true
}
