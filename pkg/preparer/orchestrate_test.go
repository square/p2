package preparer

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/intent"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type TestPod struct {
	currentManifest                                         *pods.PodManifest
	installed, launched, launchSuccess, halted, haltSuccess bool
	installErr, launchErr, haltError, currentManifestError  error
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

func testPreparer(f *FakeStore) *Preparer {
	p, _ := New("hostname", "0.0.0.0", "/hooks/dir", logging.DefaultLogger)
	p.iStore = f
	p.rStore = f
	return p
}

func TestPreparerLaunchesNewPodsThatArentInstalledYet(t *testing.T) {
	testPod := &TestPod{
		launchSuccess: true,
	}
	newManifest := testManifest(t)

	p := testPreparer(&FakeStore{})
	success := p.installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have succeeded")
	Assert(t).IsTrue(testPod.launched, "Should have launched")
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

	p := testPreparer(&FakeStore{
		currentManifest: existing,
	})
	success := p.installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have succeeded")
	Assert(t).IsTrue(testPod.launched, "should have launched")
	Assert(t).AreEqual(newManifest, testPod.currentManifest, "the current manifest should now be the new manifest")
}

func TestPreparerFailsIfInstallFails(t *testing.T) {
	testPod := &TestPod{
		installErr: fmt.Errorf("There was an error installing"),
	}
	newManifest := testManifest(t)

	p := testPreparer(&FakeStore{})
	success := p.installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsFalse(success, "The deploy should have failed")
	Assert(t).IsTrue(testPod.installed, "Install should have been attempted")
	Assert(t).IsFalse(testPod.launched, "Launch should not have happened")
}

func TestPreparerWillNotLaunchIfSHAIsTheSame(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		currentManifest: testManifest,
	}

	p := testPreparer(&FakeStore{
		currentManifest: testManifest,
	})
	success := p.installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "Should have been a success to prevent retries")
	Assert(t).IsFalse(testPod.launched, "Should not have attempted to launch")
	Assert(t).IsTrue(testPod.installed, "Should have installed")
}

func TestPreparerWillLaunchIfRealityErrsOnRead(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		launchSuccess: true,
	}

	p := testPreparer(&FakeStore{
		currentManifestError: fmt.Errorf("it erred"),
	})
	success := p.installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have attempted to install following corrupt current manifest")
	Assert(t).IsTrue(testPod.launched, "Should have launched the new manifest")
	Assert(t).AreEqual(testManifest, testPod.currentManifest, "The manifest passed was wrong")
}
