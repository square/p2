/*
Package artifact provides utilities to configure a system to use
a Platypus artifact. Platypus artifacts are simple tar files with
some extra metadata in the form of YAML files.
*/
package artifact

import (
	"archive/tar"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
)

var (
	tarAppNameParse = regexp.MustCompile("([a-zA-Z\\d\\-\\_]+)\\_([a-f0-9]+)\\.tar(\\.gz)?")
)

type Artifact struct {
	Path string
}

type Application struct {
	Name string
}

func NewArtifact(path string) (*Artifact, error) {
	if !tarAppNameParse.MatchString(path) {
		return nil, fmt.Errorf("The path %s is not a valid path: it does ", path)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("The path %s does not exist", path)
	}
	return &Artifact{path}, nil
}

func (a *Artifact) App() *Application {
	_, tar := path.Split(a.Path)
	splitUp := tarAppNameParse.FindStringSubmatch(tar)
	return &Application{
		Name: splitUp[1],
	}
}

func (a *Artifact) AppManifest() (*AppManifest, error) {
	reader, err := os.Open(a.Path)
	if err != nil {
		return nil, err
	}
	tarReader := tar.NewReader(reader)
	for {
		next, err := tarReader.Next()
		if err != nil {
			return nil, fmt.Errorf("No app manifest was found in the tar at path %s", a.Path)
		}
		if strings.HasSuffix(next.Name, "app-manifest.yml") || strings.HasSuffix(next.Name, "app-manifest.yaml") {
			break
		}
	}
	content, err := ioutil.ReadAll(tarReader)
	if err != nil {
		return nil, err
	}
	return ManifestFromBytes(content)

}
