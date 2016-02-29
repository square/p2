// Package pods borrows heavily from the Kubernetes definition of pods to provide
// p2 with a convenient way to colocate several related launchable artifacts, as well
// as basic shared runtime configuration. Pod manifests are written as YAML files
// that describe what to launch.
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp/clearsign"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"
	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

type PodID string

type Launchable struct {
	Type                    string            `yaml:"launchable_type"`
	ID                      string            `yaml:"launchable_id"`
	Location                string            `yaml:"location"`
	DigestLocation          string            `yaml:"digest_location,omitempty"`
	DigestSignatureLocation string            `yaml:"digest_signature_location,omitempty"`
	RestartTimeout          string            `yaml:"restart_timeout,omitempty"`
	Cgroup                  cgroups.Config    `yaml:"cgroup,omitempty"`
	Env                     map[string]string `yaml:"env,omitempty"`
}

type Status struct {
	HTTP bool   `yaml:"http,omitempty"`
	Path string `yaml:"path,omitempty"`
	Port int    `yaml:"port,omitempty"`
}

type ManifestBuilder interface {
	GetManifest() Manifest
	SetID(PodID)
	SetConfig(config map[interface{}]interface{}) error
	SetRunAsUser(user string)
	SetStatusHTTP(statusHTTP bool)
	SetStatusPath(statusPath string)
	SetStatusPort(port int)
	SetLaunchables(launchables map[string]Launchable)
}

var _ ManifestBuilder = manifestBuilder{}

func NewManifestBuilder() ManifestBuilder {
	return manifestBuilder{&manifest{}}
}

func (m manifestBuilder) GetManifest() Manifest {
	return m.manifest
}

type manifestBuilder struct {
	*manifest
}

// Read-only immutable interface for manifests. To programatically build a
// manifest, use ManifestBuilder
type Manifest interface {
	ID() PodID
	RunAsUser() string
	Write(out io.Writer) error
	ConfigFileName() (string, error)
	WriteConfig(out io.Writer) error
	PlatformConfigFileName() (string, error)
	WritePlatformConfig(out io.Writer) error
	GetLaunchables() map[string]Launchable
	GetConfig() map[interface{}]interface{}
	SHA() (string, error)
	GetStatusHTTP() bool
	GetStatusPath() string
	GetStatusPort() int
	Marshal() ([]byte, error)
	SignatureData() (plaintext, signature []byte)
	GetRestartPolicy() runit.RestartPolicy

	GetBuilder() ManifestBuilder
}

// assert manifest implements Manifest
var _ Manifest = &manifest{}

type manifest struct {
	Id            PodID                       `yaml:"id"` // public for yaml marshaling access. Use ID() instead.
	RunAs         string                      `yaml:"run_as,omitempty"`
	Launchables   map[string]Launchable       `yaml:"launchables"`
	Config        map[interface{}]interface{} `yaml:"config"`
	StatusPort    int                         `yaml:"status_port,omitempty"` // DEPRECATED
	StatusHTTP    bool                        `yaml:"status_http,omitempty"` // DEPRECATED
	Status        Status                      `yaml:"status,omitempty"`
	RestartPolicy runit.RestartPolicy         `yaml:"restart_policy,omitempty"`

	// Used to track the original bytes so that we don't reorder them when
	// doing a yaml.Unmarshal and a yaml.Marshal in succession
	raw []byte

	// Signature related fields, may be empty if manifest is not signed
	plaintext []byte
	signature []byte
}

func (m *manifest) GetBuilder() ManifestBuilder {
	builder := manifestBuilder{
		&manifest{},
	}
	*builder.manifest = *m
	builder.manifest.plaintext = nil
	builder.manifest.signature = nil
	builder.manifest.raw = nil
	return builder
}

func (manifest *manifest) ID() PodID {
	return manifest.Id
}

func (m manifestBuilder) SetID(id PodID) {
	m.manifest.Id = id
}

func (manifest *manifest) GetLaunchables() map[string]Launchable {
	return manifest.Launchables
}

func (manifest *manifest) SetLaunchables(launchables map[string]Launchable) {
	manifest.Launchables = launchables
}

func (manifest *manifest) GetConfig() map[interface{}]interface{} {
	configCopy := make(map[interface{}]interface{})

	// We want to make a deep copy of the config and return that. We will
	// take advantage of YAML marshaling to do this by first serializing
	// the config data, and then unmarshaling it into a new map
	bytes, err := yaml.Marshal(manifest.Config)
	if err != nil {
		// We panic here because our code maintains an invariant that
		// manifest.Config can be serialized to yaml successfully. See
		// the test in SetConfig()
		panic(err)
	}

	err = yaml.Unmarshal(bytes, &configCopy)
	if err != nil {
		// We panic here because our code maintains an invariant that
		// manifest.Config can be unserialized from yaml successfully.
		// See the test in SetConfig()
		panic(err)
	}

	return configCopy
}

func (m manifestBuilder) SetConfig(config map[interface{}]interface{}) error {
	// Confirm that the data passed in can be successfully serialized as YAML
	bytes, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	configCopy := make(map[interface{}]interface{})
	err = yaml.Unmarshal(bytes, &configCopy)
	if err != nil {
		return err
	}

	m.Config = configCopy
	return nil
}

func (manifest *manifest) GetStatusHTTP() bool {
	if manifest.StatusHTTP {
		return true
	}
	return manifest.Status.HTTP
}

func (manifest *manifest) SetStatusHTTP(statusHTTP bool) {
	manifest.StatusHTTP = false
	manifest.Status.HTTP = statusHTTP
}

func (manifest *manifest) GetStatusPath() string {
	if manifest.Status.Path != "" {
		return path.Join("/", manifest.Status.Path)
	}
	return "/_status"
}

func (manifest *manifest) SetStatusPath(statusPath string) {
	manifest.Status.Path = statusPath
}

func (manifest *manifest) GetStatusPort() int {
	if manifest.StatusPort != 0 {
		return manifest.StatusPort
	}
	return manifest.Status.Port
}

func (manifest *manifest) SetStatusPort(port int) {
	manifest.StatusPort = 0
	manifest.Status.Port = port
}

func (manifest *manifest) RunAsUser() string {
	if manifest.RunAs != "" {
		return manifest.RunAs
	}
	return string(manifest.ID())
}

func (mb manifestBuilder) SetRunAsUser(user string) {
	mb.manifest.RunAs = user
}

// ManifestFromPath constructs a Manifest from a local file. This function is a helper for
// ManifestFromBytes().
func ManifestFromPath(path string) (Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ManifestFromReader(f)
}

// ManifestFromURI constructs a Manifest from data located at a URI. This function is a
// helper for ManifestFromBytes().
func ManifestFromURI(manifestUri string) (Manifest, error) {
	f, err := uri.DefaultFetcher.Open(manifestUri)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ManifestFromReader(f)
}

// ManifestFromReader constructs a Manifest from an open Reader. All bytes will be read
// from the Reader. The caller is responsible for closing the Reader, if necessary. This
// function is a helper for ManifestFromBytes().
func ManifestFromReader(reader io.Reader) (Manifest, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return ManifestFromBytes(bytes)
}

// ManifestFromBytes constructs a Manifest by parsing its serialized representation. The
// manifest can be a raw YAML document or a PGP clearsigned YAML document. If signed, the
// signature components will be stored inside the Manifest instance.
func ManifestFromBytes(bytes []byte) (Manifest, error) {
	manifest := &manifest{}

	// Preserve the raw manifest so that manifest.Bytes() returns bytes in
	// the same order that they were passed to this function
	manifest.raw = make([]byte, len(bytes))
	copy(manifest.raw, bytes)

	signed, _ := clearsign.Decode(bytes)
	if signed != nil {
		signature, err := ioutil.ReadAll(signed.ArmoredSignature.Body)
		if err != nil {
			return nil, util.Errorf("Could not read signature from pod manifest: %s", err)
		}
		manifest.signature = signature

		// the original plaintext is in signed.Plaintext, but the signature
		// corresponds to signed.Bytes, so that's what we need to save
		manifest.plaintext = signed.Bytes

		// parse YAML from the message's plaintext instead
		bytes = signed.Plaintext
	}

	if err := yaml.Unmarshal(bytes, manifest); err != nil {
		return nil, util.Errorf("Could not read pod manifest: %s", err)
	}
	if err := ValidManifest(manifest); err != nil {
		return nil, util.Errorf("invalid manifest: %s", err)
	}
	return manifest, nil
}

func (manifest *manifest) Write(out io.Writer) error {
	bytes, err := manifest.Marshal()
	if err != nil {
		return util.Errorf("Could not write manifest for %s: %s", manifest.ID(), err)
	}
	_, err = out.Write(bytes)
	if err != nil {
		return util.Errorf("Could not write manifest for %s: %s", manifest.ID(), err)
	}
	return nil
}

func (manifest *manifest) Marshal() ([]byte, error) {
	// if it's signed, we must recycle the original content to preserve the
	// signature's validity. remarshaling it might change the exact text of
	// the YAML, which would invalidate the signature.
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

func (manifest *manifest) WriteConfig(out io.Writer) error {
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

func (manifest *manifest) WritePlatformConfig(out io.Writer) error {
	platConf := make(map[string]interface{})
	for _, launchable := range manifest.Launchables {
		platConf[launchable.ID] = map[string]interface{}{
			"cgroup": launchable.Cgroup,
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
func (manifest *manifest) SHA() (string, error) {
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

func (manifest *manifest) ConfigFileName() (string, error) {
	sha, err := manifest.SHA()
	if err != nil {
		return "", err
	}
	return string(manifest.Id) + "_" + sha + ".yaml", nil
}

func (manifest *manifest) PlatformConfigFileName() (string, error) {
	sha, err := manifest.SHA()
	if err != nil {
		return "", err
	}
	return string(manifest.Id) + "_" + sha + ".platform.yaml", nil
}

// Returns readers needed to verify the signature on the
// manifest. These readers do not need closing.
func (m manifest) SignatureData() (plaintext, signature []byte) {
	if m.signature == nil {
		return nil, nil
	}
	return m.plaintext, m.signature
}

func (m manifest) GetRestartPolicy() runit.RestartPolicy {
	if m.RestartPolicy == "" {
		return runit.DefaultRestartPolicy
	}
	return m.RestartPolicy
}

// ValidManifest checks the internal consistency of a manifest. Returns an error if the
// data is inconsistent or "nil" otherwise.
func ValidManifest(m Manifest) error {
	if m.ID() == "" {
		return fmt.Errorf("manifest must contain an 'id'")
	}
	for key, launchable := range m.GetLaunchables() {
		switch {
		case launchable.Type == "":
			return fmt.Errorf("'%s': launchable must contain a 'launchable_type'", key)
		case launchable.ID == "":
			return fmt.Errorf("'%s': launchable must contain a 'launchable_id'", key)
		case launchable.Location == "":
			return fmt.Errorf("'%s': launchable must contain a 'location'", key)
		}
	}
	return nil
}
