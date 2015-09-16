// Package pods borrows heavily from the Kubernetes definition of pods to provide
// p2 with a convenient way to colocate several related launchable artifacts, as well
// as basic shared runtime configuration. Pod manifests are written as YAML files
// that describe what to launch.
package pods

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp/clearsign"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"
	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/util"
)

type LaunchableStanza struct {
	LaunchableType          string         `yaml:"launchable_type"`
	LaunchableId            string         `yaml:"launchable_id"`
	Location                string         `yaml:"location"`
	DigestLocation          string         `yaml:"digest_location,omitempty"`
	DigestSignatureLocation string         `yaml:"digest_signature_location,omitempty"`
	RestartTimeout          string         `yaml:"restart_timeout,omitempty"`
	CgroupConfig            cgroups.Config `yaml:"cgroup,omitempty"`
}

type Manifest struct {
	Id                string                      `yaml:"id"` // public for yaml marshaling access. Use ID() instead.
	RunAs             string                      `yaml:"run_as,omitempty"`
	LaunchableStanzas map[string]LaunchableStanza `yaml:"launchables"`
	Config            map[interface{}]interface{} `yaml:"config"`
	StatusPort        int                         `yaml:"status_port,omitempty"`
	StatusHTTP        bool                        `yaml:"status_http,omitempty"`
	// these fields are required to track the original text if it was signed
	raw       []byte
	plaintext []byte
	signature []byte
}

func (manifest *Manifest) ID() string {
	return manifest.Id
}

func (manifest *Manifest) RunAsUser() string {
	if manifest.RunAs != "" {
		return manifest.RunAs
	}
	return manifest.ID()
}

func ManifestFromPath(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return ManifestFromBytes(bytes)
}

func ManifestFromString(str string) (*Manifest, error) {
	return ManifestFromBytes(bytes.NewBufferString(str).Bytes())
}

func ManifestFromBytes(bytes []byte) (*Manifest, error) {
	manifest := &Manifest{}

	// Preserve the raw manifest so that manifest.Bytes() returns bytes in
	// the same order that they were passed to this function
	manifest.raw = make([]byte, len(bytes))
	copy(manifest.raw, bytes)

	signed, _ := clearsign.Decode(bytes)
	if signed != nil {
		signature, err := ioutil.ReadAll(signed.ArmoredSignature.Body)
		if err != nil {
			return nil, fmt.Errorf("Could not read signature from pod manifest: %s", err)
		}
		manifest.signature = signature

		// the original plaintext is in signed.Plaintext, but the signature
		// corresponds to signed.Bytes, so that's what we need to save
		manifest.plaintext = signed.Bytes

		// parse YAML from the message's plaintext instead
		bytes = signed.Plaintext
	}

	if err := yaml.Unmarshal(bytes, manifest); err != nil {
		return nil, fmt.Errorf("Could not read pod manifest: %s", err)
	}
	return manifest, nil
}

func (manifest *Manifest) Write(out io.Writer) error {
	bytes, err := manifest.OriginalBytes()
	if err != nil {
		return util.Errorf("Could not write manifest for %s: %s", manifest.ID(), err)
	}
	_, err = out.Write(bytes)
	if err != nil {
		return util.Errorf("Could not write manifest for %s: %s", manifest.ID(), err)
	}
	return nil
}

// OriginalBytes() always returns the bytes from the original manifest file
// that was passed to ManifestFromBytes(). Any mutations to the struct will not
// appear in the returned bytes. If mutations are desired, use Marshal()
func (manifest *Manifest) OriginalBytes() ([]byte, error) {
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

// Returns bytes correlating to the (potentially mutated) struct, unlike
// Bytes() which guarantees that the bytes returned will be the same bytes the
// manifest struct was built from
func (manifest *Manifest) Marshal() ([]byte, error) {
	return yaml.Marshal(manifest)
}

func (manifest *Manifest) WriteConfig(out io.Writer) error {
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

func (manifest *Manifest) WritePlatformConfig(out io.Writer) error {
	platConf := make(map[string]interface{})
	for _, stanza := range manifest.LaunchableStanzas {
		platConf[stanza.LaunchableId] = map[string]interface{}{
			"cgroup": stanza.CgroupConfig,
		}
	}

	bytes, err := yaml.Marshal(platConf)
	if err != nil {
		return util.Errorf("Could not write config for %s: %s", manifest.ID(), err)
	}
	_, err = out.Write(bytes)
	if err != nil {
		return util.Errorf("Could not write config for %s: %s", manifest.ID(), err)
	}
	return nil
}

// SHA() returns a string containing a hex encoded SHA256 checksum of the
// manifest's contents. The contents are normalized, such that all equivalent
// YAML structures have the same SHA (despite differences in comments,
// indentation, etc).
func (manifest *Manifest) SHA() (string, error) {
	if manifest == nil {
		return "", util.Errorf("the manifest is nil")
	}
	buf, err := yaml.Marshal(manifest) // always remarshal
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	hasher.Write(buf)
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (manifest *Manifest) ConfigFileName() (string, error) {
	sha, err := manifest.SHA()
	if err != nil {
		return "", err
	}
	return manifest.Id + "_" + sha + ".yaml", nil
}

func (manifest *Manifest) PlatformConfigFileName() (string, error) {
	sha, err := manifest.SHA()
	if err != nil {
		return "", err
	}
	return manifest.Id + "_" + sha + ".platform.yaml", nil
}

// Returns readers needed to verify the signature on the
// manifest. These readers do not need closing.
func (manifest Manifest) SignatureData() (plaintext, signature []byte) {
	if manifest.signature == nil {
		return nil, nil
	}
	return manifest.plaintext, manifest.signature
}
