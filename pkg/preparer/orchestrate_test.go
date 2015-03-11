package preparer

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/clearsign"
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

func (t *TestPod) Verify(manifest *pods.PodManifest, keyring openpgp.KeyRing) error {
	return nil
}

func (t *TestPod) Disable(manifest *pods.PodManifest) (bool, error) {
	return true, nil
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
	beforeInstallErr, afterInstallErr, afterLaunchErr error
	ranBeforeInstall, ranAfterLaunch, runAfterInstall bool
}

func (f *fakeHooks) RunHookType(hookType hooks.HookType, pod hooks.Pod, manifest *pods.PodManifest) error {
	switch hookType {
	case hooks.BEFORE_INSTALL:
		f.ranBeforeInstall = true
		return f.beforeInstallErr
	case hooks.AFTER_INSTALL:
		f.runAfterInstall = true
		return f.afterInstallErr
	case hooks.AFTER_LAUNCH:
		f.ranAfterLaunch = true
		return f.afterLaunchErr
	}
	return util.Errorf("Invalid hook type configured in test: %s", hookType)
}

func testManifest(t *testing.T) *pods.PodManifest {
	manifestPath := util.From(runtime.Caller(0)).ExpandPath("test_manifest.yaml")
	manifest, err := pods.PodManifestFromPath(manifestPath)
	if err != nil {
		t.Fatal("No test manifest found, failing\n")
	}
	return manifest
}

// Use the same signer for all SignedManifests.
// Otherwise, Travis may time out waiting for entropy.
var cachedSigner *openpgp.Entity

func testSignedManifest(t *testing.T, modify func(*pods.PodManifest, *openpgp.Entity)) (*pods.PodManifest, *openpgp.Entity) {
	testManifest := testManifest(t)

	if cachedSigner == nil {
		var err error
		cachedSigner, err = openpgp.NewEntity("p2", "p2-test", "p2@squareup.com", nil)
		Assert(t).IsNil(err, "NewEntity error should have been nil")
	}
	fakeSigner := cachedSigner

	if modify != nil {
		modify(testManifest, fakeSigner)
	}

	manifestBytes, err := testManifest.Bytes()
	Assert(t).IsNil(err, "manifest bytes error should have been nil")

	var buf bytes.Buffer
	sigWriter, err := clearsign.Encode(&buf, fakeSigner.PrivateKey, nil)
	Assert(t).IsNil(err, "clearsign Encode error should have been nil")

	sigWriter.Write(manifestBytes)
	sigWriter.Close()

	manifest, err := pods.PodManifestFromBytes(buf.Bytes())
	Assert(t).IsNil(err, "should have generated manifest from signed bytes")

	return manifest, fakeSigner
}

type FakeStore struct {
	currentManifest      *pods.PodManifest
	currentManifestError error
}

func (f *FakeStore) Pod(string) (*pods.PodManifest, time.Duration, error) {
	if f.currentManifest == nil {
		return nil, 0, pods.NoCurrentManifest
	}
	if f.currentManifestError != nil {
		return nil, 0, f.currentManifestError
	}
	return f.currentManifest, 0, nil
}

func (f *FakeStore) SetPod(string, pods.PodManifest) (time.Duration, error) {
	return 0, nil
}

func (f *FakeStore) RegisterService(pods.PodManifest, string) error {
	return nil
}

func (f *FakeStore) WatchPods(string, <-chan struct{}, chan<- error, chan<- kp.ManifestResult) {}

func testPreparer(t *testing.T, f *FakeStore) (*Preparer, *fakeHooks, string) {
	podRoot, _ := ioutil.TempDir("", "pod_root")
	cfg := &PreparerConfig{
		NodeName:       "hostname",
		ConsulAddress:  "0.0.0.0",
		HooksDirectory: util.From(runtime.Caller(0)).ExpandPath("test_hooks"),
		PodRoot:        podRoot,
	}
	p, err := New(cfg, logging.DefaultLogger)
	Assert(t).IsNil(err, "Test setup error: should not have erred when trying to load a fake preparer")
	hooks := &fakeHooks{}
	p.hooks = hooks
	p.store = f
	return p, hooks, podRoot
}

func TestPreparerLaunchesNewPodsThatArentInstalledYet(t *testing.T) {
	testPod := &TestPod{
		launchSuccess: true,
	}
	newManifest := testManifest(t)

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
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

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{
		currentManifest: existing,
	})
	defer os.RemoveAll(fakePodRoot)
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

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
	success := p.installAndLaunchPod(newManifest, testPod, logging.DefaultLogger)

	Assert(t).IsFalse(success, "The deploy should have failed")
	Assert(t).IsTrue(hooks.ranBeforeInstall, "should have ran before_install hooks")
	Assert(t).IsTrue(testPod.installed, "Install should have been attempted")
	Assert(t).IsFalse(testPod.launched, "Launch should not have happened")
	Assert(t).IsFalse(hooks.ranAfterLaunch, "should not have run after_launch hooks")
}

func TestPreparerWillNotInstallOrLaunchIfIdIsForbidden(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		launchSuccess:   true,
		currentManifest: testManifest,
	}

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
	p.forbiddenPodIds = make(map[string]struct{})
	p.forbiddenPodIds[testManifest.ID()] = struct{}{}

	success := p.installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsFalse(success, "Installing a forbidden ID should not succeed")
	Assert(t).IsFalse(hooks.ranBeforeInstall, "Should not have run hooks prior to install")
	Assert(t).IsFalse(testPod.installed, "Should not have installed")
	Assert(t).IsFalse(testPod.launched, "Should not have attempted to launch")
	Assert(t).IsFalse(hooks.ranAfterLaunch, "Should not have run after_launch hooks")
}

func TestPreparerWillNotInstallOrLaunchIfSHAIsTheSame(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		currentManifest: testManifest,
	}

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{
		currentManifest: testManifest,
	})
	defer os.RemoveAll(fakePodRoot)
	success := p.installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "Should have been a success to prevent retries")
	Assert(t).IsFalse(hooks.ranBeforeInstall, "Should not have run hooks prior to install")
	Assert(t).IsFalse(testPod.installed, "Should not have installed")
	Assert(t).IsFalse(testPod.launched, "Should not have attempted to launch")
	Assert(t).IsFalse(hooks.ranAfterLaunch, "Should not have run after_launch hooks")
}

func TestPreparerWillLaunchIfRealityErrsOnRead(t *testing.T) {
	testManifest := testManifest(t)
	testPod := &TestPod{
		launchSuccess: true,
	}

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{
		currentManifestError: fmt.Errorf("it erred"),
	})
	defer os.RemoveAll(fakePodRoot)
	success := p.installAndLaunchPod(testManifest, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have attempted to install following corrupt current manifest")
	Assert(t).IsTrue(testPod.launched, "Should have launched the new manifest")
	Assert(t).IsTrue(hooks.ranAfterLaunch, "Should have run after_launch hooks")
	Assert(t).AreEqual(testManifest, testPod.currentManifest, "The manifest passed was wrong")
}

func TestPreparerWillRequireSignatureWithKeyring(t *testing.T) {
	manifest := testManifest(t)

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
	p.keyring = openpgp.EntityList{}

	Assert(t).IsFalse(p.verifySignature(*manifest, logging.DefaultLogger), "should have accepted unsigned manifest")
}

func TestPreparerWillAcceptSignatureFromKeyring(t *testing.T) {
	manifest, fakeSigner := testSignedManifest(t, nil)

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
	p.keyring = openpgp.EntityList{fakeSigner}

	Assert(t).IsTrue(p.verifySignature(*manifest, logging.DefaultLogger), "should have accepted signed manifest")
}

func TestPreparerWillAcceptSignatureForPreparerWithoutAuthorizedDeployers(t *testing.T) {
	manifest, fakeSigner := testSignedManifest(t, func(m *pods.PodManifest, _ *openpgp.Entity) {
		m.Id = POD_ID
	})

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
	p.keyring = openpgp.EntityList{fakeSigner}

	Assert(t).IsTrue(p.verifySignature(*manifest, logging.DefaultLogger), "expected preparer to accept manifest (empty authorized deployers)")
}

func TestPreparerWillRejectUnauthorizedSignatureForPreparer(t *testing.T) {
	manifest, fakeSigner := testSignedManifest(t, func(m *pods.PodManifest, _ *openpgp.Entity) {
		m.Id = POD_ID
	})

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
	p.keyring = openpgp.EntityList{fakeSigner}
	p.authorizedDeployers = []string{"nobodylol"}

	Assert(t).IsFalse(p.verifySignature(*manifest, logging.DefaultLogger), "expected preparer to reject manifest (unauthorized deployer)")
}

func TestPreparerWillAcceptAuthorizedSignatureForPreparer(t *testing.T) {
	sig := ""
	manifest, fakeSigner := testSignedManifest(t, func(m *pods.PodManifest, e *openpgp.Entity) {
		m.Id = POD_ID
		sig = e.PrimaryKey.KeyIdShortString()
	})

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
	p.keyring = openpgp.EntityList{fakeSigner}
	p.authorizedDeployers = []string{sig}

	Assert(t).IsTrue(p.verifySignature(*manifest, logging.DefaultLogger), "expected preparer to accept manifest (authorized deployer)")
}

func TestPreparerWillAcceptSignatureWhenKeyringIsNil(t *testing.T) {
	manifest := testManifest(t)
	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer os.RemoveAll(fakePodRoot)
	p.keyring = nil
	Assert(t).IsTrue(p.verifySignature(*manifest, logging.DefaultLogger), "expected the preparer to verify the signature when no keyring given")
}
