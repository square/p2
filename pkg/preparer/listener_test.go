package preparer

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/clearsign"
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

	current, err := user.Current()
	Assert(t).IsNil(err, "test setup: could not get the current user")

	testConfig := make(map[interface{}]interface{})
	testConfig["hook"] = map[string]interface{}{
		"run_as": current.Username,
	}

	manifest := &pods.PodManifest{
		Id: "users",
		LaunchableStanzas: map[string]pods.LaunchableStanza{
			"create": pods.LaunchableStanza{
				Location:       util.From(runtime.Caller(0)).ExpandPath("hoisted-hello_def456.tar.gz"),
				LaunchableType: "hoist",
				LaunchableId:   "create",
			},
		},
		Config: testConfig,
	}
	manifestBytes, err := manifest.Bytes()
	Assert(t).IsNil(err, "manifest bytes error should have been nil")

	fakeSigner, err := openpgp.NewEntity("p2", "p2-test", "p2@squareup.com", nil)
	Assert(t).IsNil(err, "NewEntity error should have been nil")

	var buf bytes.Buffer
	sigWriter, err := clearsign.Encode(&buf, fakeSigner.PrivateKey, nil)
	Assert(t).IsNil(err, "clearsign encode error should have been nil")

	sigWriter.Write(manifestBytes)
	sigWriter.Close()

	manifest, err = pods.PodManifestFromBytes(buf.Bytes())
	Assert(t).IsNil(err, "should have generated manifest from signed bytes")

	fakeIntent := fakeStoreWithManifests(kp.ManifestResult{
		Path:     path.Join(hookPrefix, "before_install/users"),
		Manifest: *manifest,
	})

	listener := HookListener{
		Intent:         fakeIntent,
		HookPrefix:     hookPrefix,
		ExecDir:        execDir,
		DestinationDir: destDir,
		Logger:         logging.DefaultLogger,
		Keyring:        openpgp.EntityList{fakeSigner},
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

	hookFile := path.Join(execDir, "before_install", "users__create__launch")
	_, err = os.Stat(hookFile)
	Assert(t).IsNil(err, "should have created the user launch script")
}
