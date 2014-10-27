// Package runit provides a programmatic way to interact with runit and
// servicebuilder (https://github.com/square/prodeng/blob/master/servicebuilder/README.md).
// You can use this package to make it easy to write new sb configs and exec sb.
//
// Example usage:
//
// import "github.com/square/p2/pkg/runit"
//
// template := runit.NewSBTemplate("exampleapp")
// template.AddEntry("exampleapp_web", []string{"python", "-m", "SimpleHTTPServer"})
// template.AddEntry("exampleapp_redis", []string{"redis", "-p", "6603"})
// outPath, err := runit.DefaultBuilder.Write(template) // write the new config
// if err != nil {
//    return nil, err
// }
// err = runit.DefaultBuilder.Rebuild() // rebuild
//
package runit

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"

	"github.com/square/p2/pkg/util"

	"gopkg.in/yaml.v2"
)

type SBTemplate struct {
	name       string
	runClauses map[string]map[string][]string
}

type SBTemplateEntry struct {
	baseCmd []string
}

func NewSBTemplate(name string) *SBTemplate {
	return &SBTemplate{
		name:       name,
		runClauses: make(map[string]map[string][]string),
	}
}

func (template *SBTemplate) AddEntry(app string, run []string) {
	template.runClauses[app] = map[string][]string{"run": run}
}

func (template *SBTemplate) Write(out io.Writer) error {
	b, err := yaml.Marshal(template.runClauses)
	if err != nil {
		return util.Errorf("Could not marshal servicebuilder template %s as YAML: %s", template.name, err)
	}
	out.Write(b)
	return nil
}

type ServiceBuilder struct {
	ConfigRoot  string
	StagingRoot string
	RunitRoot   string
	Bin         string
}

var DefaultBuilder = &ServiceBuilder{
	ConfigRoot:  "/etc/servicebuilder.d",
	StagingRoot: "/var/service-stage",
	RunitRoot:   "/var/service",
	Bin:         "/usr/bin/servicebuilder",
}

func (b *ServiceBuilder) serviceYamlPath(name string) string {
	return path.Join(b.ConfigRoot, fmt.Sprintf("%s.yaml", name))
}

func (b *ServiceBuilder) Write(template *SBTemplate) (string, error) {
	yamlPath := b.serviceYamlPath(template.name)
	fileinfo, _ := os.Stat(yamlPath)
	if fileinfo != nil && fileinfo.IsDir() {
		return "", util.Errorf("%s is a directory, but should be empty or a file", yamlPath)
	}

	f, err := os.OpenFile(yamlPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer f.Close()
	if err != nil {
		return "", util.Errorf("Could not write servicebuilder template %s: %s", template.name, err)
	}

	return yamlPath, template.Write(f)
}

func (b *ServiceBuilder) Remove(template *SBTemplate) error {
	yamlPath := b.serviceYamlPath(template.name)

	err := os.Remove(yamlPath)
	if err != nil {
		return err
	}

	return nil
}

func (b *ServiceBuilder) Rebuild() (string, error) {
	return b.RebuildWithStreams(os.Stdin)
}

func (b *ServiceBuilder) RebuildWithStreams(stdin io.Reader) (string, error) {
	cmd := exec.Command(b.Bin, "-c", b.ConfigRoot, "-s", b.StagingRoot, "-d", b.RunitRoot)
	cmd.Stdin = stdin
	buffer := bytes.Buffer{}
	cmd.Stdout = &buffer
	err := cmd.Run()
	if err != nil {
		return "", util.Errorf("Could not run servicebuilder rebuild: %s", err)
	}
	return buffer.String(), nil
}
