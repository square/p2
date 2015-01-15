package preparer

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/intent"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type TestPod struct {
	currentManifest                                         *pods.PodManifest
	installed, launched, launchSuccess, halted, haltSuccess bool
	installErr, launchErr, haltError, currentManifestError  error
	configDir, envDir                                       string
}

func (t *TestPod) ManifestSHA() (string, error) {
	return "abc123", nil
}

func (t *TestPod) Launch(manifest *pods.PodManifest) (bool, error) {
	t.currentManifest = manifest
	t.launched = true
	if t.launchErr != nil {
		return false, t.launchErr
	}
	return t.launchSuccess, nil
}

func (t *TestPod) Install(manifest *pods.PodManifest) error {
	t.installed = true
	return t.installErr
}

func (t *TestPod) Halt() (bool, error) {
	t.halted = true
	return t.haltSuccess, t.haltError
}

func (t *TestPod) ConfigDir() string {
	if t.configDir != "" {
		return t.configDir
	}
	return os.TempDir()
}

func (t *TestPod) EnvDir() string {
	if t.envDir != "" {
		return t.envDir
	}
	return os.TempDir()
}

func (t *TestPod) Path() string {
	return os.TempDir()
}

type fakeHooks struct {
	beforeInstallErr, afterInstallErr error
	ranBeforeInstall, ranAfterLaunch  bool
}

func (f *fakeHooks) RunBeforeInstall(pod hooks.Pod, manifest *pods.PodManifest) error {
	f.ranBeforeInstall = true
	return f.beforeInstallErr
}

func (f *fakeHooks) RunAfterLaunch(pod hooks.Pod, manifest *pods.PodManifest) error {
	f.ranAfterLaunch = true
	return f.afterInstallErr
}

func testManifest(t *testing.T) *pods.PodManifest {
	manifestPath := util.From(runtime.Caller(0)).ExpandPath("test_manifest.yaml")
	manifest, err := pods.PodManifestFromPath(manifestPath)
	if err != nil {
		t.Fatal("No test manifest found, failing\n")
	}
	return manifest
}

type FakeStore struct {
	currentManifest      *pods.PodManifest
	currentManifestError error
}

func (f *FakeStore) Pod(string, string) (*pods.PodManifest, error) {
	if f.currentManifest == nil {
		return nil, pods.NoCurrentManifest
	}
	if f.currentManifestError != nil {
		return nil, f.currentManifestError
	}
	return f.currentManifest, nil
}

func (f *FakeStore) SetPod(string, pods.PodManifest) (time.Duration, error) {
	return 0, nil
}

func (f *FakeStore) RegisterPodService(pods.PodManifest) error {
	return nil
}

func (f *FakeStore) WatchPods(string, <-chan struct{}, chan<- error, chan<- intent.ManifestResult) error {
	return nil
}

func testPreparer(f *FakeStore) (*Preparer, *fakeHooks) {
	p, _ := New("hostname", "0.0.0.0", util.From(runtime.Caller(0)).ExpandPath("test_hooks"), logging.DefaultLogger)
	hooks := &fakeHooks{}
	p.hooks = hooks
	p.iStore = f
	p.rStore = f
	return p, hooks
}

func TestPreparerLaunchesNewPodsThatArentInstalledYet(t *testing.T) {
	testPod := &TestPod{
		launchSuccess: true,
	}
	newManifest := testManifest(t)

	p, hooks := testPreparer(&FakeStore{})
	success := p.installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have succeeded")
	Assert(t).IsTrue(testPod.launched, "Should have launched")
	Assert(t).IsTrue(hooks.ranAfterLaunch, "after launch hooks should have ran")
	Assert(t).IsFalse(testPod.halted, "Should not have tried to halt anything")
	Assert(t).AreEqual(testPod.currentManifest, newManifest, "The manifest should be the new one")
}

func TestPreparerLaunchesPodsThatHaveDifferentSHAs(t *testing.T) {
	existing := &pods.PodManifest{}
	existing.Id = "different"

	testPod := &TestPod{
		launchSuccess:   true,
		haltSuccess:     true,
		currentManifest: existing,
	}
	newManifest := testManifest(t)

	p, hooks := testPreparer(&FakeStore{
		currentManifest: existing,
	})
	success := p.installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have succeeded")
	Assert(t).IsTrue(testPod.installed, "should have installed")
	Assert(t).IsTrue(testPod.launched, "should have launched")
	Assert(t).IsTrue(hooks.ranBeforeInstall, "before install should have ran")
	Assert(t).IsTrue(hooks.ranAfterLaunch, "after launch should have ran")
	Assert(t).AreEqual(newManifest, testPod.currentManifest, "the current manifest should now be the new manifest")
}

func TestPreparerFailsIfInstallFails(t *testing.T) {
	testPod := &TestPod{
		installErr: fmt.Errorf("There was an error installing"),
	}
	newManifest := testManifest(t)

	p, hooks := testPreparer(&FakeStore{})
	success := p.installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsFalse(success, "The deploy should have failed")
	Assert(t).IsTrue(hooks.ranBeforeInstall, "should have ran before_install hooks")
	Assert(t).IsTrue(testPod.installed, "Install should have been attempted")
	Assert(t).IsFalse(testPod.launched, "Launch should not have happened")
	Assert(t).IsFalse(hooks.ranAfterLaunch, "should not have run after_launch hooks")
}

func TestPreparerWillNotInstallOrLaunchIfSHAIsTheSame(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		currentManifest: testManifest,
	}

	p, hooks := testPreparer(&FakeStore{
		currentManifest: testManifest,
	})
	success := p.installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "Should have been a success to prevent retries")
	Assert(t).IsFalse(hooks.ranBeforeInstall, "Should not have run hooks prior to install")
	Assert(t).IsFalse(testPod.installed, "Should not have installed")
	Assert(t).IsFalse(testPod.launched, "Should not have attempted to launch")
	Assert(t).IsFalse(hooks.ranAfterLaunch, "Should not have launched")
}

func TestPreparerWillLaunchIfRealityErrsOnRead(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		launchSuccess: true,
	}

	p, hooks := testPreparer(&FakeStore{
		currentManifestError: fmt.Errorf("it erred"),
	})
	success := p.installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have attempted to install following corrupt current manifest")
	Assert(t).IsTrue(testPod.launched, "Should have launched the new manifest")
	Assert(t).IsTrue(hooks.ranAfterLaunch, "Should have run after_launch hooks")
	Assert(t).AreEqual(testManifest, testPod.currentManifest, "The manifest passed was wrong")
}
