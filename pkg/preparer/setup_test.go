package preparer

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/pborman/uuid"
	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/podstore"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

func TestLoadConfigWillMarshalYaml(t *testing.T) {
	configPath := util.From(runtime.Caller(0)).ExpandPath("test_preparer_config.yaml")
	preparerConfig, err := LoadConfig(configPath)
	Assert(t).IsNil(err, "should have read config correctly")

	Assert(t).AreEqual("/dev/shm/p2-may-run", preparerConfig.RequireFile, "did not read the require file correctly")
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
	builder := manifest.NewBuilder()
	builder.SetID("users")
	builder.SetRunAsUser(current.Username)
	builder.SetLaunchables(map[launch.LaunchableID]launch.LaunchableStanza{
		"create": {
			Location:       util.From(runtime.Caller(0)).ExpandPath("testdata/hoisted-hello_def456.tar.gz"),
			LaunchableType: "hoist",
		},
	})

	podManifest := builder.GetManifest()

	hookFactory := pods.NewHookFactory(destDir, "testNode", uri.DefaultFetcher)
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

func TestBuildRealityAtLaunch(t *testing.T) {
	podID := "some-app"
	testDir := "./tmp"
	podRoot := filepath.Join(testDir, "data/pods/")
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := consul.NewConsulStore(fixture.Client)
	podStore := podstore.NewConsul(fixture.Client.KV())
	preparer := Preparer{
		node:     "test.local",
		client:   fixture.Client,
		Logger:   logging.DefaultLogger,
		store:    store,
		podStore: podStore,
		podRoot:  podRoot,
	}

	from, err := os.Open("./testdata/test_current_manifest.yaml")
	if err != nil {
		t.Fatalf("Error opening ./testdata/test_current_manifest.yaml: %s", err)
	}
	defer from.Close()

	uuidStr := uuid.New()
	apps := []string{podID, podID + "-" + uuidStr}
	for _, app := range apps {
		err := os.MkdirAll(filepath.Join(podRoot, app), 0755)
		if err != nil {
			t.Fatalf("Error making tmp directory: %s", err)
		}

		to, err := os.OpenFile(filepath.Join(podRoot, app, "current_manifest.yaml"), os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			t.Fatalf("Error opening destination current_manifest.yaml: %s", err)
		}
		defer to.Close()

		_, err = io.Copy(to, from)
		if err != nil {
			t.Fatalf("Error copying ./testdata/test_current_manifest.yaml to tmp: %s", err)
		}
	}
	defer func() {
		err := os.RemoveAll(testDir)
		if err != nil {
			t.Fatalf("Unable to remove tmp directory used for testing: %s", err)
		}
	}()

	err = preparer.BuildRealityAtLaunch()
	if err != nil {
		t.Fatalf("Error in BuildRealityAtLaunch: %s", err)
	}

	reality, _, err := store.ListPods(consul.REALITY_TREE, preparer.node)
	if err != nil {
		t.Fatalf("Error reading reality tree: %s", err)
	}
	match := false
	for _, result := range reality {
		if result.Manifest.ID().String() == podID {
			match = true
			break
		}
	}
	if !match {
		t.Fatalf("Did not find podID %s in reality tree", podID)
	}
	realityIndexPath := fmt.Sprintf("reality/%s/%s", preparer.node, uuidStr)
	pair, _, err := preparer.client.KV().Get(realityIndexPath, nil)
	if err != nil {
		t.Fatalf("Unable to fetch the key (%s): %s", realityIndexPath, err)
	}
	if pair == nil {
		t.Fatalf("%s should have been written but it wasn't", realityIndexPath)
	}
}
