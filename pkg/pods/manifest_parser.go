package pods

import (
	"fmt"
	"io/ioutil"
	"os"

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
}

func ManifestFromPath(path string) (*PodManifest, error) {
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
