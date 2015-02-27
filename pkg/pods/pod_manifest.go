// Package pods borrows heavily from the Kubernetes definition of pods to provide
// p2 with a convenient way to colocate several related launchable artifacts, as well
// as basic shared runtime configuration. Pod manifests are written as YAML files
// that describe what to launch.
package pods

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/square/p2/pkg/util"
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/clearsign"
	"gopkg.in/yaml.v2"
)

type LaunchableStanza struct {
	LaunchableType string `yaml:"launchable_type"`
	LaunchableId   string `yaml:"launchable_id"`
	Location       string `yaml:"location"`
}

type PodManifest struct {
	Id                string                      `yaml:"id"` // public for yaml marshaling access. Use ID() instead.
	LaunchableStanzas map[string]LaunchableStanza `yaml:"launchables"`
	Config            map[string]interface{}      `yaml:"config"`
	StatusPort        int                         `yaml:"status_port,omitempty"`
	StatusHTTP        bool                        `yaml:"status_http,omitempty"`
	// these fields are required to track the original text if it was signed
	raw       []byte
	plaintext []byte
	signature []byte
}

func (manifest *PodManifest) ID() string {
	return manifest.Id
}

func PodManifestFromPath(path string) (*PodManifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return PodManifestFromBytes(bytes)
}

func PodManifestFromString(str string) (*PodManifest, error) {
	return PodManifestFromBytes(bytes.NewBufferString(str).Bytes())
}

func PodManifestFromBytes(bytes []byte) (*PodManifest, error) {
	podManifest := &PodManifest{}

	signed, _ := clearsign.Decode(bytes)
	if signed != nil {
		signature, err := ioutil.ReadAll(signed.ArmoredSignature.Body)
		if err != nil {
			return nil, fmt.Errorf("Could not read signature from pod manifest: %s", err)
		}
		podManifest.signature = signature

		podManifest.raw = make([]byte, len(bytes))
		copy(podManifest.raw, bytes)

		// the original plaintext is in signed.Plaintext, but the signature
		// corresponds to signed.Bytes, so that's what we need to save
		podManifest.plaintext = signed.Bytes

		// parse YAML from the message's plaintext instead
		bytes = signed.Plaintext
	}

	if err := yaml.Unmarshal(bytes, podManifest); err != nil {
		return nil, fmt.Errorf("Could not read pod manifest: %s", err)
	}
	return podManifest, nil
}

func (manifest *PodManifest) Write(out io.Writer) error {
	bytes, err := manifest.Bytes()
	if err != nil {
		return util.Errorf("Could not write manifest for %s: %s", manifest.ID(), err)
	}
	_, err = out.Write(bytes)
	if err != nil {
		return util.Errorf("Could not write manifest for %s: %s", manifest.ID(), err)
	}
	return nil
}

func (manifest *PodManifest) Bytes() ([]byte, error) {
	if manifest.raw != nil {
		// if it's signed, we must recycle the original content to preserve the
		// signature's validity. remarshaling it might change the exact text of
		// the YAML, which would invalidate the signature.
		ret := make([]byte, len(manifest.raw))
		copy(ret, manifest.raw)
		return ret, nil
	}
	return yaml.Marshal(manifest)
}

func (manifest *PodManifest) WriteConfig(out io.Writer) error {
	bytes, err := yaml.Marshal(manifest.Config)
	if err != nil {
		return util.Errorf("Could not write config for %s: %s", manifest.ID(), err)
	}
	_, err = out.Write(bytes)
	if err != nil {
		return util.Errorf("Could not write config for %s: %s", manifest.ID(), err)
	}
	return nil
}

// SHA() returns a string containing a hex encoded SHA-1 checksum of the
// manifest's contents. The contents are normalized, such that all equivalent
// YAML structures have the same SHA (despite differences in comments,
// indentation, etc).
func (manifest *PodManifest) SHA() (string, error) {
	if manifest == nil {
		return "", util.Errorf("the manifest is nil")
	}
	buf, err := yaml.Marshal(manifest) // always remarshal
	if err != nil {
		return "", err
	}
	hasher := sha1.New()
	hasher.Write(buf)
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (manifest *PodManifest) ConfigFileName() (string, error) {
	sha, err := manifest.SHA()
	if err != nil {
		return "", err
	}
	return manifest.Id + "_" + sha + ".yaml", nil
}

// Returns the entity that signed the manifest, if any. If there was no
// signature, both returns are nil. If the signer is not in the given keyring,
// an openpgp.ErrUnknownIssuer will be returned.
func (manifest *PodManifest) Signer(keyring openpgp.KeyRing) (*openpgp.Entity, error) {
	if manifest.signature == nil {
		return nil, nil
	}
	return openpgp.CheckDetachedSignature(keyring, bytes.NewReader(manifest.plaintext), bytes.NewReader(manifest.signature))
}
