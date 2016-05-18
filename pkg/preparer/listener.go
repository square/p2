package preparer

import (
	"path/filepath"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
)

type IntentStore interface {
	WatchPods(
		podPrefix kp.PodPrefix,
		hostname types.NodeName,
		quit <-chan struct{},
		errCh chan<- error,
		manifests chan<- []kp.ManifestResult,
	)
	ListPods(
		podPrefix kp.PodPrefix,
		hostname types.NodeName,
	) ([]kp.ManifestResult, time.Duration, error)
}

type HookListener struct {
	Intent     IntentStore
	HookPrefix kp.PodPrefix // The prefix in the intent store to watch

	// Note: node is currently unused because hooks are not scheduled by
	// hostname, however there are future plans to change this behavior, so
	// the hostname is passed to WatchPods and ListPods even though it is
	// ignored
	Node             types.NodeName // The host to watch for hooks for
	DestinationDir   string         // The destination directory for downloaded pods that will act as hooks
	ExecDir          string         // The directory that will actually be executed by the HookDir
	Logger           logging.Logger
	authPolicy       auth.Policy
	artifactVerifier auth.ArtifactVerifier
}

// Sync keeps manifests located at the hook pods in the intent store.
// This function will open a new Pod watch on the given prefix and install
// any pods listed there in a hook pod directory. Following that, it will
// remove old links named by the same pod in the same event directory and
// symlink in the new pod's launchables.
func (l *HookListener) Sync(quit <-chan struct{}, errCh chan<- error) {

	watcherQuit := make(chan struct{})
	watcherErrCh := make(chan error)
	podChan := make(chan []kp.ManifestResult)

	go l.Intent.WatchPods(l.HookPrefix, l.Node, watcherQuit, watcherErrCh, podChan)

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
		}
	}
}

// Sync the hooks just once. This is useful at preparer startup so that hooks
// will be synced before normal pod management begins
func (l *HookListener) SyncOnce() error {
	pods, _, err := l.Intent.ListPods(l.HookPrefix, l.Node)
	if err != nil {
		return err
	}

	// pods could be empty, but we don't support hook deletion yet.
	// NOTE: the error handling is slightly different than in the Sync()
	// case. Any error aborts proceeding. This is intentional because a
	// hook error when starting the preparer likely means degraded service,
	// whereas a hook error in an already-running preparer probably means a
	// temporary breakage in the hook
	for _, result := range pods {
		err := l.installHook(result)
		if err != nil {
			return err
		}
	}
	return nil
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

	hookPod := pods.NewPod(
		result.Manifest.ID(),
		filepath.Join(l.DestinationDir, string(result.Manifest.ID())),
	)

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
	err = hookPod.Install(result.Manifest, l.artifactVerifier)
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
