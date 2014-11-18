package preparer

import (
	"fmt"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
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

func (t *TestPod) CurrentManifest() (*pods.PodManifest, error) {
	if t.currentManifestError != nil {
		return nil, t.currentManifestError
	}
	if t.currentManifest == nil {
		return t.currentManifest, pods.NoCurrentManifest
	}
	return t.currentManifest, nil
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

func TestPreparerLaunchesNewPodsThatArentInstalledYet(t *testing.T) {
	testPod := &TestPod{
		launchSuccess: true,
	}
	newManifest := testManifest(t)
	success := installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

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
	success := installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have succeeded")
	Assert(t).IsTrue(testPod.launched, "should have launched")
	Assert(t).AreEqual(newManifest, testPod.currentManifest, "the current manifest should now be the new manifest")
}

func TestPreparerFailsIfInstallFails(t *testing.T) {
	testPod := &TestPod{
		installErr: fmt.Errorf("There was an error installing"),
	}
	newManifest := testManifest(t)
	success := installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsFalse(success, "The deploy should have failed")
	Assert(t).IsTrue(testPod.installed, "Install should have been attempted")
	Assert(t).IsFalse(testPod.launched, "Launch should not have happened")
}

func TestPreparerWillNotLaunchIfSHAIsTheSame(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		currentManifest: testManifest,
	}

	success := installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "Should have been a success to prevent retries")
	Assert(t).IsFalse(testPod.launched, "Should not have attempted to launch")
	Assert(t).IsTrue(testPod.installed, "Should have installed")
}

func TestInstallReturnsFalseIfManifestErrsOnRead(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		currentManifestError: fmt.Errorf("it erred"),
		launchSuccess:        true,
	}

	success := installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have attempted to install following corrupt current manifest")
	Assert(t).IsTrue(testPod.launched, "Should have launched the new manifest")
	Assert(t).AreEqual(testManifest, testPod.currentManifest, "The manifest passed was wrong")
}
