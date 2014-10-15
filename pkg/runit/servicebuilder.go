// Package runit provides a programmatic way to interact with runit and
// servicebuilder (https://github.com/square/prodeng/blob/master/servicebuilder/README.md).
// You can use this package to make it easy to write new sb configs and exec sb.
//
// Example usage:
//
// import "github.com/platypus-platform/pp/pkg/runit"
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
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"

	"gopkg.in/yaml.v2"
)

type SBTemplate struct {
	name       string
	runClauses map[string]map[string][]string
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
		return fmt.Errorf("Could not marshal servicebuilder template %s as YAML: %s", template.name, err)
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

func (b *ServiceBuilder) Write(template *SBTemplate) (string, error) {
	yamlPath := path.Join(b.ConfigRoot, fmt.Sprintf("%s.yaml", template.name))
	fileinfo, _ := os.Stat(yamlPath)
	if fileinfo != nil && fileinfo.IsDir() {
		return "", fmt.Errorf("%s is a directory, but should be empty or a file", yamlPath)
	}

	f, err := os.OpenFile(yamlPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer f.Close()
	if err != nil {
		return "", fmt.Errorf("Could not write servicebuilder template %s: %s", template.name, err)
	}

	return yamlPath, template.Write(f)
}

func (b *ServiceBuilder) Rebuild() error {
	return b.RebuildWithStreams(os.Stdin, os.Stdout)
}

func (b *ServiceBuilder) RebuildWithStreams(stdin io.Reader, stdout io.Writer) error {
	cmd := exec.Command(b.Bin, "-c", b.ConfigRoot, "-s", b.StagingRoot, "-d", b.RunitRoot)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	return cmd.Run()
}
