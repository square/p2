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
	"time"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp/clearsign"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type fakeIntentStore struct {
	manifests []kp.ManifestResult
	quit      chan struct{}
	errToSend error
}

func fakeStoreWithManifests(manifests ...kp.ManifestResult) *fakeIntentStore {
	return &fakeIntentStore{
		manifests: manifests,
		quit:      make(chan struct{}),
	}
}

func (f *fakeIntentStore) WatchPods(podPrefix kp.PodPrefix, nodeName string, quitCh <-chan struct{}, errCh chan<- error, podCh chan<- []kp.ManifestResult) {
	go func() {
		podCh <- f.manifests
		if f.errToSend != nil {
			errCh <- f.errToSend
		} else {
			f.quit <- struct{}{}
		}
	}()
	<-quitCh
}

func (f *fakeIntentStore) ListPods(podPrefix kp.PodPrefix, nodeName string) ([]kp.ManifestResult, time.Duration, error) {
	return f.manifests, 0, nil
}

func testHookListener(t *testing.T) (HookListener, <-chan struct{}) {
	hookPrefix := kp.HOOK_TREE
	destDir, _ := ioutil.TempDir("", "pods")
	defer os.RemoveAll(destDir)
	execDir, err := ioutil.TempDir("", "exec")
	defer os.RemoveAll(execDir)
	Assert(t).IsNil(err, "should not have erred creating a tempdir")

	current, err := user.Current()
	Assert(t).IsNil(err, "test setup: could not get the current user")
	builder := pods.NewManifestBuilder()
	builder.SetID("users")
	builder.SetRunAsUser(current.Username)
	builder.SetLaunchables(map[string]pods.LaunchableStanza{
		"create": {
			Location:       util.From(runtime.Caller(0)).ExpandPath("hoisted-hello_def456.tar.gz"),
			LaunchableType: "hoist",
			LaunchableId:   "create",
		},
	})
	manifest := builder.GetManifest()
	manifestBytes, err := manifest.Marshal()
	Assert(t).IsNil(err, "manifest bytes error should have been nil")

	fakeSigner, err := openpgp.NewEntity("p2", "p2-test", "p2@squareup.com", nil)
	Assert(t).IsNil(err, "NewEntity error should have been nil")

	var buf bytes.Buffer
	sigWriter, err := clearsign.Encode(&buf, fakeSigner.PrivateKey, nil)
	Assert(t).IsNil(err, "clearsign encode error should have been nil")

	sigWriter.Write(manifestBytes)
	sigWriter.Close()

	manifest, err = pods.ManifestFromBytes(buf.Bytes())
	Assert(t).IsNil(err, "should have generated manifest from signed bytes")

	fakeIntent := fakeStoreWithManifests(kp.ManifestResult{
		Path:     path.Join(string(hookPrefix), "users"),
		Manifest: manifest,
	})

	listener := HookListener{
		Intent:         fakeIntent,
		HookPrefix:     hookPrefix,
		ExecDir:        execDir,
		DestinationDir: destDir,
		Logger:         logging.DefaultLogger,
		authPolicy:     auth.FixedKeyringPolicy{openpgp.EntityList{fakeSigner}, nil},
	}

	return listener, fakeIntent.quit
}

func TestHookPodsInstallAndLinkCorrectly(t *testing.T) {
	listener, quit := testHookListener(t)

	errCh := make(chan error, 1)
	listener.Sync(quit, errCh)
	select {
	case err := <-errCh:
		Assert(t).IsNil(err, "There should not have been an error in the call to Sync()")
	default:
	}

	currentAlias := path.Join(listener.DestinationDir, "users", "create", "current", "bin", "launch")
	_, err := os.Stat(currentAlias)
	Assert(t).IsNil(err, fmt.Sprintf("%s should have been created", currentAlias))

	hookFile := path.Join(listener.ExecDir, "users__create__launch")
	_, err = os.Stat(hookFile)
	Assert(t).IsNil(err, "should have created the user launch script")
}

func TestSyncHooksOnce(t *testing.T) {
	listener, _ := testHookListener(t)
	err := listener.SyncOnce()
	Assert(t).IsNil(err, "There should not have been an error in the call to SyncOnce()")
	currentAlias := path.Join(listener.DestinationDir, "users", "create", "current", "bin", "launch")
	_, err = os.Stat(currentAlias)
	Assert(t).IsNil(err, fmt.Sprintf("%s should have been created", currentAlias))

	hookFile := path.Join(listener.ExecDir, "users__create__launch")
	_, err = os.Stat(hookFile)
	Assert(t).IsNil(err, "should have created the user launch script")
}
