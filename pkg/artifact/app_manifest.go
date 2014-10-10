package artifact

import (
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

type AppManifest struct {
	Ports map[int][]string `yaml:"ports"`
}

func ManifestFromPath(path string) (*AppManifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	bytes, err := ioutil.ReadAll(f)
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
