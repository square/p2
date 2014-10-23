// Package pods borrows heavily from the Kubernetes definition of pods to provide
// p2 with a convenient way to colocate several related launchable artifacts, as well
// as basic shared runtime configuration. Pod manifests are written as YAML files
// that describe what to launch.
package pods

import (
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
	Id                string                      `yaml:"id"`
	LaunchableStanzas map[string]LaunchableStanza `yaml:"launchables"`
	Config            map[string]interface{}      `yaml:"config"`
}

func PodManifestFromPath(path string) (*PodManifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return PodManifestFromBytes(bytes)
}

func PodManifestFromBytes(bytes []byte) (*PodManifest, error) {
	podManifest := &PodManifest{}
	if err := yaml.Unmarshal(bytes, podManifest); err != nil {
		return nil, fmt.Errorf("Could not read pod manifest: %s", err)
	}
	return podManifest, nil
}

func (manifest *PodManifest) Write(out io.Writer) error {
	bytes, err := yaml.Marshal(manifest)
	if err != nil {
		return util.Errorf("Could not write manifest for %s: %s", manifest.Id, err)
	}
	_, err = out.Write(bytes)
	if err != nil {
		return util.Errorf("Could not write manifest for %s: %s", manifest.Id, err)
	}
	return nil
}

func (manifest *PodManifest) WriteConfig(out io.Writer) error {
	bytes, err := yaml.Marshal(manifest.Config)
	if err != nil {
		return util.Errorf("Could not write config for %s: %s", manifest.Id, err)
	}
	_, err = out.Write(bytes)
	if err != nil {
		return util.Errorf("Could not write config for %s: %s", manifest.Id, err)
	}
	return nil
}
