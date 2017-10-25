// Package pods borrows heavily from the Kubernetes definition of pods to provide
// p2 with a convenient way to colocate several related launchable artifacts, as well
// as basic shared runtime configuration. Pod manifests are written as YAML files
// that describe what to launch.
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"

	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
	"golang.org/x/crypto/openpgp/clearsign"
	"gopkg.in/yaml.v2"
)

type StatusStanza struct {
	HTTP          bool   `yaml:"http,omitempty"`
	Path          string `yaml:"path,omitempty"`
	Port          int    `yaml:"port,omitempty"`
	LocalhostOnly bool   `yaml:"localhost_only,omitempty"`
}

type Builder interface {
	GetManifest() Manifest
	SetID(types.PodID)
	SetConfig(config map[interface{}]interface{}) error
	SetRunAsUser(user string)
	SetStatusHTTP(statusHTTP bool)
	SetStatusPath(statusPath string)
	SetStatusPort(port int)
	SetLaunchables(launchableStanzas map[launch.LaunchableID]launch.LaunchableStanza)
}

var _ Builder = builder{}

func NewBuilder() Builder {
	return builder{&manifest{}}
}

func (m builder) GetManifest() Manifest {
	return m.manifest
}

type builder struct {
	*manifest
}

// Read-only immutable interface for manifests. To programmatically build a
// manifest, use Builder
type Manifest interface {
	ID() types.PodID
	RunAsUser() string
	Write(out io.Writer) error
	ConfigFileName() (string, error)
	WriteConfig(out io.Writer) error
	MergedConfig(with map[interface{}]interface{}) (map[interface{}]interface{}, error)
	WriteMergedConfig(out io.Writer, mergeWith map[interface{}]interface{}) error
	PlatformConfigFileName() (string, error)
	WritePlatformConfig(out io.Writer) error
	GetLaunchableStanzas() map[launch.LaunchableID]launch.LaunchableStanza
	GetConfig() map[interface{}]interface{}
	SHA() (string, error)
	GetStatusHTTP() bool
	GetStatusPath() string
	GetStatusPort() int
	GetStatusLocalhostOnly() bool
	Marshal() ([]byte, error)
	SignatureData() (plaintext, signature []byte)

	GetBuilder() Builder
}

// assert manifest implements Manifest and UnsignedManifest
var _ Manifest = &manifest{}

type manifest struct {
	Id                types.PodID                                     `yaml:"id"` // public for yaml marshaling access. Use ID() instead.
	RunAs             string                                          `yaml:"run_as,omitempty"`
	LaunchableStanzas map[launch.LaunchableID]launch.LaunchableStanza `yaml:"launchables"`
	Config            map[interface{}]interface{}                     `yaml:"config"`
	StatusPort        int                                             `yaml:"status_port,omitempty"`
	StatusHTTP        bool                                            `yaml:"status_http,omitempty"`
	Status            StatusStanza                                    `yaml:"status,omitempty"`

	// Used to track the original bytes so that we don't reorder them when
	// doing a yaml.Unmarshal and a yaml.Marshal in succession
	raw []byte

	// Signature related fields, may be empty if manifest is not signed
	plaintext []byte
	signature []byte
}

func (m *manifest) GetBuilder() Builder {
	builder := builder{
		&manifest{},
	}
	*builder.manifest = *m
	builder.manifest.plaintext = nil
	builder.manifest.signature = nil
	builder.manifest.raw = nil
	return builder
}

func (manifest *manifest) ID() types.PodID {
	return manifest.Id
}

func (m builder) SetID(id types.PodID) {
	m.manifest.Id = id
}

func (manifest *manifest) GetLaunchableStanzas() map[launch.LaunchableID]launch.LaunchableStanza {
	return manifest.LaunchableStanzas
}

func (manifest *manifest) SetLaunchables(launchableStanzas map[launch.LaunchableID]launch.LaunchableStanza) {
	manifest.LaunchableStanzas = launchableStanzas
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

func (m builder) SetConfig(config map[interface{}]interface{}) error {
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

func (manifest *manifest) GetStatusLocalhostOnly() bool {
	return manifest.Status.LocalhostOnly
}

func (manifest *manifest) SetStatusLocalhostOnly(localhostOnly bool) {
	manifest.Status.LocalhostOnly = localhostOnly
}

func (manifest *manifest) RunAsUser() string {
	if manifest.RunAs != "" {
		return manifest.RunAs
	}
	return string(manifest.ID())
}

func (mb builder) SetRunAsUser(user string) {
	mb.manifest.RunAs = user
}

// FromPath constructs a Manifest from a local file. This function is a helper for
// FromBytes().
func FromPath(path string) (Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return FromReader(f)
}

// FromURI constructs a Manifest from data located at a URI. This function is a
// helper for FromBytes().
func FromURI(manifestUri *url.URL) (Manifest, error) {
	f, err := uri.DefaultFetcher.Open(manifestUri)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return FromReader(f)
}

// FromReader constructs a Manifest from an open Reader. All bytes will be read
// from the Reader. The caller is responsible for closing the Reader, if necessary. This
// function is a helper for FromBytes().
func FromReader(reader io.Reader) (Manifest, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return FromBytes(bytes)
}

// FromBytes constructs a Manifest by parsing its serialized representation. The
// manifest can be a raw YAML document or a PGP clearsigned YAML document. If signed, the
// signature components will be stored inside the Manifest instance.
func FromBytes(bytes []byte) (Manifest, error) {
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
	return writeConfig(manifest.ID(), manifest.Config, out)
}

// MergedConfig merges this manifest's config with the given map, returning a new config map.
// The config of the manifest is not modified.
func (manifest *manifest) MergedConfig(with map[interface{}]interface{}) (map[interface{}]interface{}, error) {
	merged := manifest.GetConfig()

	// Tried just marshalling the second config, then unmarshalling it into the same map.
	// However, that doesn't preserve inner maps correctly.
	mergeInto(merged, with)

	return merged, nil
}

// mutates base.
func mergeInto(base, overlay map[interface{}]interface{}) {
	for k, v := range overlay {
		if rightMap, rightIsMap := v.(map[interface{}]interface{}); rightIsMap {
			if leftMap, leftIsMap := base[k].(map[interface{}]interface{}); leftMap != nil && leftIsMap {
				// Both are maps; merge them.
				mergeInto(leftMap, rightMap)
			} else {
				// right is map and left is non-map: overwrite left with right.
				// OR right is map and left is nonexistent: use right.
				base[k] = v
			}
		} else {
			// Right isn't map. Overwrite left with right.
			// Note that if left is map and right isn't,
			// the (left) map gets overwritten with the (right) non-map.
			base[k] = v
		}
	}
}

func (manifest *manifest) WriteMergedConfig(out io.Writer, mergeWith map[interface{}]interface{}) error {
	merged, err := manifest.MergedConfig(mergeWith)
	if err != nil {
		return util.Errorf("Couldn't merge config for %s: %s", manifest.ID(), err)
	}
	return writeConfig(manifest.ID(), merged, out)
}

func writeConfig(id types.PodID, config map[interface{}]interface{}, out io.Writer) error {
	bytes, err := yaml.Marshal(config)
	if err != nil {
		return util.Errorf("Could not write config for %s: %s", id, err)
	}
	_, err = out.Write(bytes)
	if err != nil {
		return util.Errorf("Could not write config for %s: %s", id, err)
	}
	return nil
}

func (manifest *manifest) WritePlatformConfig(out io.Writer) error {
	platConf := make(map[launch.LaunchableID]interface{})
	for launchableID, stanza := range manifest.LaunchableStanzas {
		platConf[launchableID] = map[launch.LaunchableID]interface{}{
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
func (manifest *manifest) SHA() (string, error) {
	if manifest == nil {
		return "", util.Errorf("the manifest is nil")
	}
	buf, err := yaml.Marshal(manifest) // always remarshal
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	if _, err := hasher.Write(buf); err != nil {
		return "", err
	}
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

// ValidManifest checks the internal consistency of a manifest. Returns an error if the
// data is inconsistent or "nil" otherwise.
func ValidManifest(m Manifest) error {
	if m.ID() == "" {
		return fmt.Errorf("manifest must contain an 'id'")
	}
	for launchableID, stanza := range m.GetLaunchableStanzas() {
		switch {
		case stanza.LaunchableType == "":
			return fmt.Errorf("'%s': launchable must contain a 'launchable_type'", launchableID)
		case stanza.Location == "" && stanza.Version.ID == "":
			return fmt.Errorf("'%s': launchable must contain a 'location' or 'version'", launchableID)
		case stanza.Location != "" && stanza.Version.ID != "":
			return fmt.Errorf("'%s': launchable must not contain both 'location' and 'version'", launchableID)
		}
	}
	return nil
}
