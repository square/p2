// Package runit provides a programmatic way to interact with runit and
// servicebuilder (https://github.com/square/prodeng/blob/master/servicebuilder/README.md).
// You can use this package to make it easy to write new sb configs and exec sb.
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
		return err
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
		return "", err
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
