package preparer

import (
	"time"

	"github.com/Sirupsen/logrus"
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
	Verify(*pods.Manifest, auth.Policy) error
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
	node         string
	store        Store
	hooks        Hooks
	hookListener HookListener
	Logger       logging.Logger
	podRoot      string
	caPath       string
	authPolicy   auth.Policy
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
				"pod":     manifestToLaunch.ID(),
				"sha":     sha,
				"sha_err": err,
			})
			manifestLogger.NoFields().Debugln("New manifest received")

			working = p.authorize(manifestToLaunch, manifestLogger)
			if !working {
				p.tryRunHooks(hooks.AFTER_AUTH_FAIL, pods.NewPod(manifestToLaunch.ID(), pods.PodPath(p.podRoot, manifestToLaunch.ID())), &manifestToLaunch, manifestLogger)
			}
		case <-time.After(1 * time.Second):
			if working {
				pod := pods.NewPod(manifestToLaunch.ID(), pods.PodPath(p.podRoot, manifestToLaunch.ID()))

				ok := p.installAndLaunchPod(&manifestToLaunch, pod, manifestLogger)
				if ok {
					manifestToLaunch = pods.Manifest{}
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

func (p *Preparer) installAndLaunchPod(newManifest *pods.Manifest, pod Pod, logger logging.Logger) bool {
	// do not remove the logger argument, it's not the same as p.Logger

	// get currently running pod to compare with the new pod
	realityPath := kp.RealityPath(p.node, newManifest.ID())
	currentManifest, _, err := p.store.Pod(realityPath)
	currentSHA := ""
	if currentManifest != nil {
		currentSHA, _ = currentManifest.SHA()
	}
	newSHA, _ := newManifest.SHA()

	// if new or the manifest is different, launch
	newOrDifferent := (err == pods.NoCurrentManifest) || (currentSHA != newSHA)
	if newOrDifferent {
		logger.WithFields(logrus.Fields{
			"old_sha": currentSHA,
			"sha":     newSHA,
			"pod":     newManifest.ID(),
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
		p.tryRunHooks(hooks.BEFORE_INSTALL, pod, newManifest, logger)

		err = pod.Install(newManifest)
		if err != nil {
			// install failed, abort and retry
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorln("Install failed")
			return false
		}

		err = pod.Verify(newManifest, p.authPolicy)
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

// Close() releases any resources held by a Preparer.
func (p *Preparer) Close() {
	p.authPolicy.Close()
	// The same verifier is shared twice internally
	p.hookListener.authPolicy = nil
	p.authPolicy = nil
}
