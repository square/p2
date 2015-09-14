package preparer

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
)

var eventPrefix = regexp.MustCompile("/?([a-zA-Z\\_]+)\\/.+")

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
func (l *HookListener) Sync(quit <-chan struct{}, errCh chan<- error) {

	watchPath := l.HookPrefix

	watcherQuit := make(chan struct{})
	watcherErrCh := make(chan error)
	podChan := make(chan []kp.ManifestResult)

	go l.Intent.WatchPods(watchPath, watcherQuit, watcherErrCh, podChan)

	for {
		select {
		case <-quit:
			l.Logger.NoFields().Infoln("Terminating hook listener")
			watcherQuit <- struct{}{}
			return
		case err := <-watcherErrCh:
			l.Logger.WithError(err).Errorln("Error while watching pods")
			errCh <- err
		case results := <-podChan:
			for _, result := range results {
				err := l.installHook(result)
				if err != nil {
					errCh <- err
				}
			}
		}
	}
}

func (l *HookListener) installHook(result kp.ManifestResult) error {
	sub := l.Logger.SubLogger(logrus.Fields{
		"pod":  result.Manifest.ID(),
		"dest": l.DestinationDir,
	})

	err := l.authPolicy.AuthorizeHook(&result.Manifest, sub)
	if err != nil {
		if err, ok := err.(auth.Error); ok {
			sub.WithFields(err.Fields).Errorln(err)
		} else {
			sub.NoFields().Errorln(err)
		}
		return err
	}

	// Figure out what event we're setting a hook pod for. For example,
	// if we find a pod at /hooks/before_install/usercreate, then the
	// event is called "before_install"
	event, err := l.determineEvent(result.Path)
	if err != nil {
		sub.WithError(err).Errorln("Couldn't determine hook path")
		return err
	}

	hookPod := pods.NewPod(result.Manifest.ID(), path.Join(l.DestinationDir, event, result.Manifest.ID()))

	// Figure out if we even need to install anything.
	// Hooks aren't running services and so there isn't a need
	// to write the current manifest to the reality store. Instead
	// we just compare to the manifest on disk.
	current, err := hookPod.CurrentManifest()
	if err != nil && err != pods.NoCurrentManifest {
		l.Logger.WithError(err).Errorln("Could not check current manifest")
		return err
	}

	currentSHA, _ := current.SHA()
	newSHA, _ := result.Manifest.SHA()

	if err != pods.NoCurrentManifest && currentSHA == newSHA {
		// we are up-to-date, continue
		return nil
	}

	// The manifest is new, go ahead and install
	err = hookPod.Install(&result.Manifest)
	if err != nil {
		sub.WithError(err).Errorln("Could not install hook")
		return err
	}

	_, err = hookPod.WriteCurrentManifest(&result.Manifest)
	if err != nil {
		sub.WithError(err).Errorln("Could not write current manifest")
		return err
	}

	// Now that the pod is installed, link it up to the exec dir.
	err = l.writeHook(event, hookPod, &result.Manifest)
	if err != nil {
		sub.WithError(err).Errorln("Could not write hook link")
		return err
	}
	sub.NoFields().Infoln("Updated hook")
	return nil
}

func (l *HookListener) determineEvent(pathInIntent string) (string, error) {
	// The structure of a path in the hooks that we'll
	// accept from consul is {prefix}/{event}/{podID}
	split := strings.Split(pathInIntent, l.HookPrefix)
	if len(split) == 0 {
		return "", util.Errorf("The intent path %s didn't start with %s as expected", pathInIntent, l.HookPrefix)
	}
	tail := strings.Join(split[1:], "")
	matches := eventPrefix.FindStringSubmatch(tail)
	if len(matches) == 0 {
		return "", util.Errorf("%s did not match regex %s", tail, eventPrefix.String())
	}
	return matches[1], nil
}

func (l *HookListener) writeHook(event string, hookPod *pods.Pod, manifest *pods.Manifest) error {
	eventExecDir := path.Join(l.ExecDir, event)
	err := os.MkdirAll(eventExecDir, 0755)
	if err != nil {
		return util.Errorf("Couldn't make event exec dir %s", eventExecDir)
	}
	launchables, err := hookPod.Launchables(manifest)
	if err != nil {
		return err
	}

	// First remove any pre-existing hooks for that pod. Note that this is gross
	// and that we should have hooks recurse into subfolders.
	podHookPattern := path.Join(eventExecDir, fmt.Sprintf("%s__*", hookPod.Id))
	matches, err := filepath.Glob(podHookPattern)
	if err != nil {
		return util.Errorf("Couldn't find files using pattern %s in %s: %s", podHookPattern, eventExecDir, err)
	}
	for _, match := range matches {
		err = os.Remove(match)
		if err != nil {
			l.Logger.WithError(err).Warnln("Could not remove old hook")
		}
	}

	// For every launchable in the manifest, link its executable to the hook directory.
	for _, launchable := range launchables {
		// warn if a hook has a cgroup configured - it will be ignored
		emptyCgroup := cgroups.Config{Name: launchable.CgroupConfig.Name}
		if launchable.CgroupConfig != emptyCgroup {
			l.Logger.WithField("hook_launchable_id", launchable.Id).Warnln("Hook cgroup will be ignored")
		}

		executables, err := launchable.Executables(runit.DefaultBuilder)
		if err != nil {
			return err
		}

		for _, executable := range executables {
			// Write a script to the event directory that executes the pod's executables
			// with the correct environment for that pod.
			scriptPath := path.Join(eventExecDir, executable.Service.Name)
			file, err := os.OpenFile(scriptPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0744)
			defer file.Close()
			if err != nil {
				l.Logger.WithError(err).Errorln("Could not write to event dir path")
			}
			err = executable.WriteExecutor(file)
			if err != nil {
				l.Logger.WithError(err).Errorln("Could not install new hook")
			}
		}
		// for convenience as we do with regular launchables, make these ones
		// current under the launchable directory
		err = launchable.MakeCurrent()
		if err != nil {
			l.Logger.WithError(err).Errorln("Could not update the current hook")
		}
	}
	return nil
}
