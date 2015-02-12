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

// SHA() returns a string containing a hex encoded SHA-1
// checksum of the manifest's contents
func (manifest *PodManifest) SHA() (string, error) {
	if manifest == nil {
		return "", util.Errorf("the manifest is nil")
	}
	valueBuf := bytes.Buffer{}
	err := manifest.Write(&valueBuf)
	if err != nil {
		return "", err
	}
	hasher := sha1.New()
	hasher.Write(valueBuf.Bytes())
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (manifest *PodManifest) ConfigFileName() (string, error) {
	sha, err := manifest.SHA()
	if err != nil {
		return "", err
	}
	return manifest.Id + "_" + sha + ".yaml", nil
}
