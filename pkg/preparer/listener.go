package preparer

import (
	"path/filepath"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
)

type IntentStore interface {
	WatchPods(watchPath string, quit <-chan struct{}, errCh chan<- error, manifests chan<- []kp.ManifestResult)
}

type HookListener struct {
	Intent         IntentStore
	HookPrefix     string // The prefix in the intent store to watch
	DestinationDir string // The destination directory for downloaded pods that will act as hooks
	ExecDir        string // The directory that will actually be executed by the HookDir
	Logger         logging.Logger
	authPolicy     auth.Policy
}

// Sync keeps manifests located at the hook pods in the intent store.
// This function will open a new Pod watch on the given prefix and install
// any pods listed there in a hook pod directory. Following that, it will
// remove old links named by the same pod in the same event directory and
// symlink in the new pod's launchables.
// It returns a channel that will be signalled when the first update occurs, so
// that other routines may wait until hooks have synced at least once
func (l *HookListener) Sync(quit <-chan struct{}, errCh chan<- error) chan struct{} {

	watchPath := l.HookPrefix

	watcherQuit := make(chan struct{})
	watcherErrCh := make(chan error)
	podChan := make(chan []kp.ManifestResult)
	hooksSyncedCh := make(chan struct{})

	go l.Intent.WatchPods(watchPath, watcherQuit, watcherErrCh, podChan)

	hooksSynced := false
	go func() {
		for {
			select {
			case <-quit:
				l.Logger.NoFields().Infoln("Terminating hook listener")
				close(watcherQuit)
				return
			case err := <-watcherErrCh:
				l.Logger.WithError(err).Errorln("Error while watching pods")
				errCh <- err
			case results := <-podChan:
				// results could be empty, but we don't support hook deletion yet.
				for _, result := range results {
					err := l.installHook(result)
					if err != nil {
						errCh <- err
					}
				}
				if !hooksSynced {
					hooksSynced = true
					hooksSyncedCh <- struct{}{}
					close(hooksSyncedCh)
				}
			}
		}
	}()
	return hooksSyncedCh
}

func (l *HookListener) installHook(result kp.ManifestResult) error {
	sub := l.Logger.SubLogger(logrus.Fields{
		"pod":  result.Manifest.ID(),
		"dest": l.DestinationDir,
	})

	err := l.authPolicy.AuthorizeHook(result.Manifest, sub)
	if err != nil {
		if err, ok := err.(auth.Error); ok {
			sub.WithFields(err.Fields).Errorln(err)
		} else {
			sub.NoFields().Errorln(err)
		}
		return err
	}

	hookPod := pods.NewPod(result.Manifest.ID(), filepath.Join(l.DestinationDir, result.Manifest.ID()))

	// Figure out if we even need to install anything.
	// Hooks aren't running services and so there isn't a need
	// to write the current manifest to the reality store. Instead
	// we just compare to the manifest on disk.
	current, err := hookPod.CurrentManifest()
	if err != nil && err != pods.NoCurrentManifest {
		l.Logger.WithError(err).Errorln("Could not check current manifest")
		return err
	}

	var currentSHA string
	if current != nil {
		currentSHA, _ = current.SHA()
	}
	newSHA, _ := result.Manifest.SHA()

	if err != pods.NoCurrentManifest && currentSHA == newSHA {
		// we are up-to-date, continue
		return nil
	}

	// The manifest is new, go ahead and install
	err = hookPod.Install(result.Manifest)
	if err != nil {
		sub.WithError(err).Errorln("Could not install hook")
		return err
	}

	_, err = hookPod.WriteCurrentManifest(result.Manifest)
	if err != nil {
		sub.WithError(err).Errorln("Could not write current manifest")
		return err
	}

	// Now that the pod is installed, link it up to the exec dir.
	err = hooks.InstallHookScripts(l.ExecDir, hookPod, result.Manifest, sub)
	if err != nil {
		sub.WithError(err).Errorln("Could not write hook link")
		return err
	}
	sub.NoFields().Infoln("Updated hook")
	return nil
}
