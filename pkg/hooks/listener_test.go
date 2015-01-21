package hooks

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type fakeIntentStore struct {
	manifests   []kp.ManifestResult
	quit        chan struct{}
	errToSend   error
	watchedPath string
}

func fakeStoreWithManifests(manifests ...kp.ManifestResult) *fakeIntentStore {
	return &fakeIntentStore{
		manifests: manifests,
		quit:      make(chan struct{}),
	}
}

func (f *fakeIntentStore) WatchPods(watchedPath string, quitCh <-chan struct{}, errCh chan<- error, podCh chan<- kp.ManifestResult) {
	f.watchedPath = watchedPath
	go func() {
		for _, manifest := range f.manifests {
			podCh <- manifest
		}
		if f.errToSend != nil {
			errCh <- f.errToSend
		} else {
			f.quit <- struct{}{}
		}
	}()
	<-quitCh
}

func TestHookPodsInstallAndLinkCorrectly(t *testing.T) {
	hookPrefix := "hooks"
	destDir, _ := ioutil.TempDir("", "pods")
	defer os.RemoveAll(destDir)
	execDir, err := ioutil.TempDir("", "exec")
	defer os.RemoveAll(execDir)
	Assert(t).IsNil(err, "should not have erred creating a tempdir")

	fakeIntent := fakeStoreWithManifests(kp.ManifestResult{
		Path: path.Join(hookPrefix, "before_install/users"),
		Manifest: pods.PodManifest{
			Id: "users",
			LaunchableStanzas: map[string]pods.LaunchableStanza{
				"create": pods.LaunchableStanza{
					Location:       util.From(runtime.Caller(0)).ExpandPath("hoisted-hello_def456.tar.gz"),
					LaunchableType: "hoist",
					LaunchableId:   "create",
				},
			},
		},
	})

	listener := Listener{
		Intent:         fakeIntent,
		HookPrefix:     hookPrefix,
		ExecDir:        execDir,
		DestinationDir: destDir,
		Logger:         logging.DefaultLogger,
	}

	errCh := make(chan error, 1)
	listener.Sync(fakeIntent.quit, errCh)
	select {
	case err := <-errCh:
		Assert(t).IsNil(err, "There should not have been an error in the call to sync")
	default:
	}

	currentAlias := path.Join(destDir, "before_install", "users", "create", "current", "bin", "launch")
	_, err = os.Stat(currentAlias)
	Assert(t).IsNil(err, fmt.Sprintf("%s should have been created", currentAlias))

	realLaunchLocation := path.Join(destDir, "before_install", "users", "create", "installs", "hoisted-hello_def456", "bin", "launch")
	readSymlinkLocation, err := os.Readlink(path.Join(execDir, "before_install", "users__create__launch"))
	Assert(t).IsNil(err, "Could not read symlink on the exec dir")
	Assert(t).AreEqual(realLaunchLocation, readSymlinkLocation, "The read symlink location should have been the install's launch location")
}
