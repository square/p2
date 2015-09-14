package preparer

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp/clearsign"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp/packet"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

type TestPod struct {
	currentManifest                                                      *pods.Manifest
	installed, uninstalled, launched, launchSuccess, halted, haltSuccess bool
	installErr, uninstallErr, launchErr, haltError, currentManifestError error
	configDir, envDir                                                    string
}

func (t *TestPod) ManifestSHA() (string, error) {
	return "abc123", nil
}

func (t *TestPod) Launch(manifest *pods.Manifest) (bool, error) {
	t.currentManifest = manifest
	t.launched = true
	if t.launchErr != nil {
		return false, t.launchErr
	}
	return t.launchSuccess, nil
}

func (t *TestPod) Install(manifest *pods.Manifest) error {
	t.installed = true
	return t.installErr
}

func (t *TestPod) Uninstall() error {
	t.uninstalled = true
	return t.uninstallErr
}

func (t *TestPod) Verify(manifest *pods.Manifest, authPolicy auth.Policy) error {
	return nil
}

func (t *TestPod) Halt(manifest *pods.Manifest) (bool, error) {
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
	beforeInstallErr, afterInstallErr, afterLaunchErr, afterAuthFailErr error
	ranBeforeInstall, ranAfterLaunch, ranAfterInstall, ranAfterAuthFail bool
}

func (f *fakeHooks) RunHookType(hookType hooks.HookType, pod hooks.Pod, manifest *pods.Manifest) error {
	switch hookType {
	case hooks.BEFORE_INSTALL:
		f.ranBeforeInstall = true
		return f.beforeInstallErr
	case hooks.AFTER_INSTALL:
		f.ranAfterInstall = true
		return f.afterInstallErr
	case hooks.AFTER_LAUNCH:
		f.ranAfterLaunch = true
		return f.afterLaunchErr
	case hooks.AFTER_AUTH_FAIL:
		f.ranAfterAuthFail = true
		return f.afterAuthFailErr
	}
	return util.Errorf("Invalid hook type configured in test: %s", hookType)
}

func testManifest(t *testing.T) *pods.Manifest {
	manifestPath := util.From(runtime.Caller(0)).ExpandPath("test_manifest.yaml")
	manifest, err := pods.ManifestFromPath(manifestPath)
	if err != nil {
		t.Fatal("No test manifest found, failing\n")
	}
	return manifest
}

var fakeSigner *openpgp.Entity

func testSignedManifest(t *testing.T, modify func(*pods.Manifest, *openpgp.Entity)) (*pods.Manifest, *openpgp.Entity) {
	testManifest := testManifest(t)

	if fakeSigner == nil {
		var err error
		fakeSigner, err = openpgp.ReadEntity(packet.NewReader(bytes.NewReader(fakeEntity)))
		Assert(t).IsNil(err, "should have read entity")
	}

	if modify != nil {
		modify(testManifest, fakeSigner)
	}

	manifestBytes, err := testManifest.Marshal()
	Assert(t).IsNil(err, "manifest bytes error should have been nil")

	var buf bytes.Buffer
	sigWriter, err := clearsign.Encode(&buf, fakeSigner.PrivateKey, nil)
	Assert(t).IsNil(err, "clearsign Encode error should have been nil")

	sigWriter.Write(manifestBytes)
	sigWriter.Close()

	manifest, err := pods.ManifestFromBytes(buf.Bytes())
	Assert(t).IsNil(err, "should have generated manifest from signed bytes")

	return manifest, fakeSigner
}

type FakeStore struct {
	currentManifest      *pods.Manifest
	currentManifestError error
}

func (f *FakeStore) ListPods(string) ([]kp.ManifestResult, time.Duration, error) {
	if f.currentManifest == nil {
		return nil, 0, nil
	}
	if f.currentManifestError != nil {
		return nil, 0, f.currentManifestError
	}
	return []kp.ManifestResult{
		{Manifest: *f.currentManifest},
	}, 0, nil
}

func (f *FakeStore) SetPod(string, pods.Manifest) (time.Duration, error) {
	return 0, nil
}

func (f *FakeStore) DeletePod(string) (time.Duration, error) {
	return 0, nil
}

func (f *FakeStore) RegisterService(pods.Manifest, string) error {
	return nil
}

func (f *FakeStore) WatchPods(string, <-chan struct{}, chan<- error, chan<- []kp.ManifestResult) {}

func testPreparer(t *testing.T, f *FakeStore) (*Preparer, *fakeHooks, string) {
	podRoot, _ := ioutil.TempDir("", "pod_root")
	cfg := &PreparerConfig{
		NodeName:       "hostname",
		ConsulAddress:  "0.0.0.0",
		HooksDirectory: util.From(runtime.Caller(0)).ExpandPath("test_hooks"),
		PodRoot:        podRoot,
		Auth:           map[string]interface{}{"type": "none"},
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
	newPair := ManifestPair{
		ID:     newManifest.ID(),
		Intent: newManifest,
	}

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	success := p.resolvePair(newPair, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "should have succeeded")
	Assert(t).IsTrue(testPod.launched, "Should have launched")
	Assert(t).IsTrue(hooks.ranAfterLaunch, "after launch hooks should have ran")
	Assert(t).IsFalse(testPod.halted, "Should not have tried to halt anything")
	Assert(t).AreEqual(testPod.currentManifest, newManifest, "The manifest should be the new one")
}

func TestPreparerLaunchesPodsThatHaveDifferentSHAs(t *testing.T) {
	existing := &pods.Manifest{}
	existing.Id = "hello"

	testPod := &TestPod{
		launchSuccess:   true,
		haltSuccess:     true,
		currentManifest: existing,
	}
	newManifest := testManifest(t)
	newPair := ManifestPair{
		ID:      newManifest.ID(),
		Reality: existing,
		Intent:  newManifest,
	}

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	success := p.resolvePair(newPair, testPod, logging.DefaultLogger)

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
	newPair := ManifestPair{
		ID:     newManifest.ID(),
		Intent: newManifest,
	}

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	success := p.resolvePair(newPair, testPod, logging.DefaultLogger)

	Assert(t).IsFalse(success, "The deploy should have failed")
	Assert(t).IsTrue(hooks.ranBeforeInstall, "should have ran before_install hooks")
	Assert(t).IsTrue(testPod.installed, "Install should have been attempted")
	Assert(t).IsFalse(testPod.launched, "Launch should not have happened")
	Assert(t).IsFalse(hooks.ranAfterLaunch, "should not have run after_launch hooks")
}

func TestPreparerWillLaunchPreparerAsRoot(t *testing.T) {
	illegalManifest := &pods.Manifest{Id: POD_ID, RunAs: "root"}
	newPair := ManifestPair{
		ID:     illegalManifest.ID(),
		Intent: illegalManifest,
	}
	testPod := &TestPod{
		launchSuccess:   true,
		currentManifest: illegalManifest,
	}

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)

	success := p.resolvePair(newPair, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "Running preparer as root should succeed")
	Assert(t).IsTrue(hooks.ranBeforeInstall, "Should have run hooks prior to install")
	Assert(t).IsTrue(testPod.installed, "Should have installed")
	Assert(t).IsTrue(testPod.launched, "Should have attempted to launch")
	Assert(t).IsTrue(hooks.ranAfterLaunch, "Should have run after_launch hooks")
	Assert(t).IsFalse(hooks.ranAfterAuthFail, "Should not have run after_auth_fail hooks")
}

func TestPreparerWillNotInstallOrLaunchIfSHAIsTheSame(t *testing.T) {
	testManifest := testManifest(t)
	newPair := ManifestPair{
		ID:      testManifest.ID(),
		Intent:  testManifest,
		Reality: testManifest,
	}
	testPod := &TestPod{
		currentManifest: testManifest,
	}

	p, hooks, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	success := p.resolvePair(newPair, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "Should have been a success to prevent retries")
	Assert(t).IsFalse(hooks.ranBeforeInstall, "Should not have run hooks prior to install")
	Assert(t).IsFalse(testPod.installed, "Should not have installed")
	Assert(t).IsFalse(testPod.launched, "Should not have attempted to launch")
	Assert(t).IsFalse(hooks.ranAfterLaunch, "Should not have run after_launch hooks")
}

func TestPreparerWillRemoveIfManifestDisappears(t *testing.T) {
	testManifest := testManifest(t)
	newPair := ManifestPair{
		ID:      testManifest.ID(),
		Reality: testManifest,
	}
	testPod := &TestPod{
		currentManifest: testManifest,
	}

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	success := p.resolvePair(newPair, testPod, logging.DefaultLogger)

	Assert(t).IsTrue(success, "Should have successfully removed pod")
	Assert(t).IsTrue(testPod.uninstalled, "Should have uninstalled pod")
	Assert(t).IsTrue(testPod.halted, "Should have halted pod")
}

func TestPreparerWillRequireSignatureWithKeyring(t *testing.T) {
	manifest := testManifest(t)

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	p.authPolicy = auth.FixedKeyringPolicy{}

	Assert(t).IsFalse(
		p.authorize(*manifest, logging.DefaultLogger),
		"should have accepted unsigned manifest",
	)
}

func TestPreparerWillAcceptSignatureFromKeyring(t *testing.T) {
	manifest, fakeSigner := testSignedManifest(t, nil)

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	p.authPolicy = auth.FixedKeyringPolicy{openpgp.EntityList{fakeSigner}, nil}

	Assert(t).IsTrue(
		p.authorize(*manifest, logging.DefaultLogger),
		"should have accepted signed manifest",
	)
}

func TestPreparerWillAcceptSignatureForPreparerWithoutAuthorizedDeployers(t *testing.T) {
	manifest, fakeSigner := testSignedManifest(t, func(m *pods.Manifest, _ *openpgp.Entity) {
		m.Id = POD_ID
	})

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	p.authPolicy = auth.FixedKeyringPolicy{openpgp.EntityList{fakeSigner}, nil}

	Assert(t).IsTrue(
		p.authorize(*manifest, logging.DefaultLogger),
		"expected preparer to accept manifest (empty authorized deployers)",
	)
}

func TestPreparerWillRejectUnauthorizedSignatureForPreparer(t *testing.T) {
	manifest, fakeSigner := testSignedManifest(t, func(m *pods.Manifest, _ *openpgp.Entity) {
		m.Id = POD_ID
	})

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	p.authPolicy = auth.FixedKeyringPolicy{
		Keyring:             openpgp.EntityList{fakeSigner},
		AuthorizedDeployers: map[string][]string{POD_ID: {"nobodylol"}},
	}

	Assert(t).IsFalse(
		p.authorize(*manifest, logging.DefaultLogger),
		"expected preparer to reject manifest (unauthorized deployer)",
	)
}

func TestPreparerWillAcceptAuthorizedSignatureForPreparer(t *testing.T) {
	sig := ""
	manifest, fakeSigner := testSignedManifest(t, func(m *pods.Manifest, e *openpgp.Entity) {
		m.Id = POD_ID
		sig = fmt.Sprintf("%X", e.PrimaryKey.Fingerprint)
	})

	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	p.authPolicy = auth.FixedKeyringPolicy{
		Keyring:             openpgp.EntityList{fakeSigner},
		AuthorizedDeployers: map[string][]string{POD_ID: {sig}},
	}

	Assert(t).IsTrue(
		p.authorize(*manifest, logging.DefaultLogger),
		"expected preparer to accept manifest (authorized deployer)",
	)
}

func TestPreparerWillAcceptSignatureWhenKeyringIsNil(t *testing.T) {
	manifest := testManifest(t)
	p, _, fakePodRoot := testPreparer(t, &FakeStore{})
	defer p.Close()
	defer os.RemoveAll(fakePodRoot)
	// Use default p.authPolicy when no keyfile path is given

	Assert(t).IsTrue(
		p.authorize(*manifest, logging.DefaultLogger),
		"expected the preparer to verify the signature when no keyring given",
	)
}
