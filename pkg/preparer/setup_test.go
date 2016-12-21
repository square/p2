package preparer

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

func TestLoadConfigWillMarshalYaml(t *testing.T) {
	configPath := util.From(runtime.Caller(0)).ExpandPath("test_preparer_config.yaml")
	preparerConfig, err := LoadConfig(configPath)
	Assert(t).IsNil(err, "should have read config correctly")

	Assert(t).AreEqual("foohost", preparerConfig.NodeName.String(), "did not read the node name correctly")
	Assert(t).AreEqual("0.0.0.0", preparerConfig.ConsulAddress, "did not read the consul address correctly")
	Assert(t).IsTrue(preparerConfig.ConsulHttps, "did not read consul HTTPS correctly (should be true)")
	Assert(t).AreEqual("/etc/p2/hooks", preparerConfig.HooksDirectory, "did not read the hooks directory correctly")
	Assert(t).AreEqual("/etc/p2.keyring", preparerConfig.Auth["keyring"], "did not read the keyring path correctly")
	Assert(t).AreEqual(1, len(preparerConfig.ExtraLogDestinations), "should have picked up 1 log destination")

	destination := preparerConfig.ExtraLogDestinations[0]
	Assert(t).AreEqual(logging.OutSocket, destination.Type, "should have been the socket type")
	Assert(t).AreEqual("/var/log/p2-socket.out", destination.Path, "should have parsed path correctly")
}

func TestInstallHooks(t *testing.T) {
	destDir, _ := ioutil.TempDir("", "pods")
	defer os.RemoveAll(destDir)
	execDir, err := ioutil.TempDir("", "exec")
	defer os.RemoveAll(execDir)
	Assert(t).IsNil(err, "should not have erred creating a tempdir")

	current, err := user.Current()
	Assert(t).IsNil(err, "test setup: could not get the current user")
	builder := store.NewBuilder()
	builder.SetID("users")
	builder.SetRunAsUser(current.Username)
	builder.SetLaunchables(map[launch.LaunchableID]launch.LaunchableStanza{
		"create": {
			Location:       util.From(runtime.Caller(0)).ExpandPath("testdata/hoisted-hello_def456.tar.gz"),
			LaunchableType: "hoist",
		},
	})

	podManifest := builder.GetManifest()

	hookFactory := pods.NewHookFactory(destDir, "testNode")
	hooksPod := hookFactory.NewHookPod(podManifest.ID())

	preparer := Preparer{
		hooksManifest:    podManifest,
		hooksPod:         hooksPod,
		hooksExecDir:     execDir,
		Logger:           logging.DefaultLogger,
		artifactRegistry: artifact.NewRegistry(nil, uri.DefaultFetcher, osversion.DefaultDetector),
		artifactVerifier: auth.NopVerifier(),
	}

	err = preparer.InstallHooks()
	Assert(t).IsNil(err, "There should not have been an error in the call to SyncOnce()")
	currentAlias := filepath.Join(destDir, "users", "create", "current", "bin", "launch")
	_, err = os.Stat(currentAlias)
	Assert(t).IsNil(err, fmt.Sprintf("%s should have been created", currentAlias))

	hookFile := filepath.Join(execDir, "users__create__launch")
	_, err = os.Stat(hookFile)
	Assert(t).IsNil(err, "should have created the user launch script")
}
