package auth

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp/armor"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"

	"github.com/square/p2/pkg/logging"
)

// TestSigned is a stand-in for an auth.Manifest or auth.Digest.
type TestSigned struct {
	Id        string
	User      string
	Plaintext []byte
	Signature []byte
}

func (s TestSigned) ID() string {
	return s.Id
}

func (s TestSigned) RunAsUser() string {
	return s.User
}

func (s TestSigned) SignatureData() ([]byte, []byte) {
	return s.Plaintext, s.Signature
}

// Constructs a new UserPolicy with fixed preparer names
func NewTestUserPolicy(keyringPath string, deployPolicyPath string) (Policy, error) {
	return NewUserPolicy(keyringPath, deployPolicyPath, "preparer", "preparer")
}

// The testHarness groups setup functions together to reduce error
// handling logic at their call site.
type testHarness struct {
	Err error
}

func (h *testHarness) loadEntities() []*openpgp.Entity {
	if h.Err != nil {
		return nil
	}
	keyring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(testUsers))
	if err != nil {
		h.Err = err
		return nil
	}
	return keyring
}

func (h *testHarness) signMessage(msg []byte, ents []*openpgp.Entity) [][]byte {
	if h.Err != nil {
		return nil
	}
	sigs := make([][]byte, len(ents))
	for i, ent := range ents {
		buf := bytes.NewBuffer([]byte{})
		err := openpgp.DetachSign(buf, ent, bytes.NewReader(msg), nil)
		if err != nil {
			h.Err = fmt.Errorf("signing message: %s", err)
			return nil
		}
		sigs[i] = buf.Bytes()
	}
	return sigs
}

func (h *testHarness) tempFile() string {
	if h.Err != nil {
		return ""
	}
	f, err := ioutil.TempFile("", "policy_test")
	if err != nil {
		h.Err = fmt.Errorf("creating temp keyring: %s", err)
		return ""
	}
	defer f.Close()
	return f.Name()
}

func rm(t *testing.T, filename string) {
	if filename != "" {
		err := os.Remove(filename)
		if err != nil {
			t.Log(err)
		}
	}
}

func (h *testHarness) saveKeys(ents []*openpgp.Entity, filename string) {
	if h.Err != nil {
		return
	}
	f, err := os.OpenFile(filename, os.O_WRONLY, 0)
	if err != nil {
		h.Err = fmt.Errorf("opening temp file: %s", err)
		return
	}
	defer f.Close()
	armorWriter, err := armor.Encode(f, openpgp.PublicKeyType, map[string]string{})
	if err != nil {
		h.Err = fmt.Errorf("creating ASCII Armor encoder: %s", err)
		return
	}
	defer armorWriter.Close()
	for _, ent := range ents {
		err = ent.SerializePrivate(armorWriter, nil)
		if err != nil {
			h.Err = fmt.Errorf("writing keyring: %s", err)
			return
		}
	}
}

func (h *testHarness) backdate(filename string, backBy time.Duration) {
	if h.Err != nil {
		return
	}
	back := time.Now().Add(-backBy)
	err := os.Chtimes(filename, back, back)
	if err != nil {
		h.Err = err
	}
}

func (h *testHarness) saveYaml(obj interface{}, filename string) {
	if h.Err != nil {
		return
	}
	objBytes, err := yaml.Marshal(obj)
	if err != nil {
		h.Err = fmt.Errorf("marshalling yaml: %s", err)
		return
	}
	err = ioutil.WriteFile(filename, objBytes, 0)
	if err != nil {
		h.Err = fmt.Errorf("writing yaml: %s", err)
		return
	}
}

// Test that the KeyringPolicy can load a keyring
func TestKeyring(t *testing.T) {
	// Setup three entities, two of which are saved to a keyring file
	h := testHarness{}
	msg := []byte("Hello World!")
	ents := h.loadEntities()
	sigs := h.signMessage(msg, ents)
	keyfile := h.tempFile()
	defer rm(t, keyfile)
	h.saveKeys(ents[:2], keyfile)
	if h.Err != nil {
		t.Error(h.Err)
		return
	}

	policy, err := LoadKeyringPolicy(
		keyfile,
		map[string][]string{
			"restricted": {fmt.Sprintf("%X", ents[1].PrimaryKey.Fingerprint)},
		},
	)
	if err != nil {
		t.Error("creating keyring policy:", err)
		return
	}
	logger := logging.TestLogger()

	// Key in keyring signs the message
	err = policy.AuthorizeApp(TestSigned{"foo", "foo", msg, sigs[0]}, logger)
	if err != nil {
		t.Error("error authorizing pod manifest:", err)
	}

	// Key not in keyring signs the message
	err = policy.AuthorizeApp(TestSigned{"foo", "foo", msg, sigs[2]}, logger)
	if err == nil {
		t.Error("accepted unauthorized signature")
	}

	// Verify preparer authorization policy
	err = policy.AuthorizeApp(TestSigned{"restricted", "restricted", msg, sigs[1]}, logger)
	if err != nil {
		t.Error("error authorizing pod manifest:", err)
	}
	err = policy.AuthorizeApp(TestSigned{"restricted", "restricted", msg, sigs[0]}, logger)
	if err == nil {
		t.Error("accepted unauthorized signature")
	}
}

// Test that FileKeyringPolicy can reload a keyring file when it
// changes.
func TestKeyAddition(t *testing.T) {
	h := testHarness{}
	msg := []byte("Testing 1, 2, 3")
	// two entities
	ents := h.loadEntities()[:2]
	// that both sign the message
	sigs := h.signMessage(msg, ents)
	// and a temporary file to hold the keyring
	keyfile := h.tempFile()
	defer rm(t, keyfile)
	h.saveKeys(ents[:1], keyfile)
	h.backdate(keyfile, time.Minute)
	if h.Err != nil {
		t.Error(h.Err)
		return
	}

	policy, err := NewFileKeyringPolicy(keyfile, nil)
	if err != nil {
		t.Errorf("%s: error loading keyring: %s", keyfile, err)
		return
	}
	logger := logging.TestLogger()

	// Keyring contains only ents[0]
	err = policy.AuthorizeApp(TestSigned{"test", "test", msg, sigs[0]}, logger)
	if err != nil {
		t.Error("expected authorized, got error:", err)
	}
	err = policy.AuthorizeApp(TestSigned{"test", "test", msg, sigs[1]}, logger)
	if err == nil {
		t.Error("expected failure, got authorization")
	}

	// Update the keyring file with both keys. The mtime is updated
	// because we backdated the previous version.
	h.saveKeys(ents, keyfile)
	err = policy.AuthorizeApp(TestSigned{"test", "test", msg, sigs[0]}, logger)
	if err != nil {
		t.Error("expected authorized, got error:", err)
	}
	err = policy.AuthorizeApp(TestSigned{"test", "test", msg, sigs[1]}, logger)
	if err != nil {
		t.Error("expected authorized, got error:", err)
	}
}

// Test that deploy policy changes will be picked up.
func TestDpolChanges(t *testing.T) {
	h := testHarness{}
	msg := []byte("Oh dear! Oh dear! I shall be too late!")
	ents := h.loadEntities()
	sigs := h.signMessage(msg, ents)
	keyfile := h.tempFile()
	defer rm(t, keyfile)
	h.saveKeys(ents, keyfile)
	polfile := h.tempFile()
	defer rm(t, polfile)

	// Initial policy
	h.saveYaml(
		RawDeployPol{
			Groups: map[DpGroup][]DpUserEmail{
				"a": {"test1@testing"},
				"b": {"test1@testing", "test2@testing"},
				"c": {"test3@testing"},
			},
			Apps: map[string][]DpGroup{
				"foo": {"a"},
				"bar": {"b"},
				"baz": {"a", "c"},
			},
		},
		polfile,
	)
	h.backdate(polfile, time.Minute)

	if h.Err != nil {
		t.Error(h.Err)
		return
	}

	policy, err := NewTestUserPolicy(keyfile, polfile)
	if err != nil {
		t.Error("error creating user policy: ", err)
		return
	}
	logger := logging.TestLogger()

	// Test every combination of app/user and check it against the expectation matrix
	tester := func(expected map[string][3]bool) {
		for app, results := range expected {
			for signer := range results {
				err = policy.AuthorizeApp(TestSigned{app, app, msg, sigs[signer]}, logger)
				if err != nil && results[signer] {
					t.Errorf(
						"app %s signer %d: expected authorized, got error: %s",
						app,
						signer,
						err,
					)
				} else if err == nil && !results[signer] {
					t.Errorf(
						"app %s signer %d: expected failure, got authorization",
						app,
						signer,
					)
				}
			}
		}
	}

	tester(map[string][3]bool{
		"foo": {true, false, false},
		"bar": {true, true, false},
		"baz": {true, false, true},
		"ohi": {false, false, false},
	})

	// Change the policy
	h.saveYaml(
		RawDeployPol{
			Groups: map[DpGroup][]DpUserEmail{
				"a": {"test1@testing", "test2@testing"},
				"b": {"test1@testing"},
				"c": {"test3@testing"},
			},
			Apps: map[string][]DpGroup{
				"foo": {"a"},
				"bar": {"b"},
				"baz": {"c"},
				"ohi": {"a", "b", "c"},
			},
		},
		polfile,
	)

	tester(map[string][3]bool{
		"foo": {true, true, false},
		"bar": {true, false, false},
		"baz": {false, false, true},
		"ohi": {true, true, true},
	})
}

// Check that the authorization policy of hooks follows that of the
// preparer, not their own name
func TestDpolHooks(t *testing.T) {
	h := testHarness{}
	msg := []byte("Once, I was a real Turtle.")
	ents := h.loadEntities()
	sigs := h.signMessage(msg, ents)
	keyfile := h.tempFile()
	defer rm(t, keyfile)
	h.saveKeys(ents, keyfile)
	polfile := h.tempFile()
	defer rm(t, polfile)
	userSig := sigs[0]
	adminSig := sigs[1]
	h.saveYaml(
		RawDeployPol{
			Groups: map[DpGroup][]DpUserEmail{
				"users":  {"test1@testing"},
				"admins": {"test2@testing"},
			},
			Apps: map[string][]DpGroup{
				// Hooks should be treated like the preparer
				"preparer": {"admins"},
				// ...even if there is an app with the same name as a hook
				"myhook":  {"users"},
				"someapp": {"users"},
			},
		},
		polfile,
	)
	if h.Err != nil {
		t.Error(h.Err)
		return
	}
	policy, err := NewTestUserPolicy(keyfile, polfile)
	if err != nil {
		t.Error("error creating user policy: ", err)
		return
	}
	logger := logging.TestLogger()

	// P2 admins should be able to deploy the preparer and its hooks.
	err = policy.AuthorizeApp(TestSigned{"preparer", "preparer", msg, adminSig}, logger)
	if err != nil {
		t.Error("expected authorized, got error: ", err)
	}
	err = policy.AuthorizeApp(TestSigned{"preparer", "root", msg, adminSig}, logger)
	if err != nil {
		t.Error("expected authorized, got error: ", err)
	}
	err = policy.AuthorizeHook(TestSigned{"myhook", "myhook", msg, adminSig}, logger)
	if err != nil {
		t.Error("expected authorized, got error: ", err)
	}

	// Users should not be able to modify the preparer or run hooks
	err = policy.AuthorizeApp(TestSigned{"preparer", "preparer", msg, userSig}, logger)
	if err == nil {
		t.Error("expected failure, got authorization")
	}
	err = policy.AuthorizeApp(TestSigned{"preparer", "root", msg, userSig}, logger)
	if err == nil {
		t.Error("expected failure, got authorization")
	}
	err = policy.AuthorizeApp(TestSigned{"preparer", "someapp", msg, userSig}, logger)
	if err == nil {
		t.Error("expected failure, got authorization")
	}
	err = policy.AuthorizeHook(TestSigned{"myhook", "myhook", msg, userSig}, logger)
	if err == nil {
		t.Error("expected failure, got authorization")
	}
	err = policy.AuthorizeHook(TestSigned{"myhook", "preparer", msg, userSig}, logger)
	if err == nil {
		t.Error("expected failure, got authorization")
	}
}
