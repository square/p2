package artifact

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type AppManifest struct {
	Ports           map[int][]string `yaml:"ports"`
	Owners          []string         `yaml:"owners"`
	RolloutStrategy string           `yaml:"rollout_strategy"`
}

func ManifestFromPath(path string) (*AppManifest, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ManifestFromBytes(bytes)
}

func ManifestFromBytes(bytes []byte) (*AppManifest, error) {
	manifest := &AppManifest{}
	if err := yaml.Unmarshal(bytes, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}
