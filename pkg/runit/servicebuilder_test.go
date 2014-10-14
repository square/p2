package runit

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"gopkg.in/yaml.v2"
)

func TestServiceBuilderOutputsValidYaml(t *testing.T) {
	template := NewSBTemplate("exemplar")
	template.AddEntry("exemplar", []string{"ls", "-lah", "/foo/bar"})
	out := bytes.Buffer{}
	err := template.Write(&out)
	Assert(t).IsNil(err, "Did not expect to error")
	bytes, err := ioutil.ReadAll(&out)
	Assert(t).IsNil(err, "Did not expect to error")
	outMap := make(map[interface{}]interface{})
	yaml.Unmarshal(bytes, &outMap)
	expected := `exemplar:
  run:
  - ls
  - -lah
  - /foo/bar
`
	Assert(t).AreEqual(expected, string(bytes), "The generated YAML was not what was expected")
}

func expandLocal(local string) string {
	_, file, _, _ := runtime.Caller(0)
	return path.Join(path.Dir(file), local)
}

func fakeServiceBuilder(t *testing.T) ServiceBuilder {
	confDir, err := ioutil.TempDir("", "sb-conf")
	Assert(t).IsNil(err, "confdir should have been created (test setup error)")

	stagingDir, err := ioutil.TempDir("", "sb-stage")
	Assert(t).IsNil(err, "stagingdir should have been created (test setup error)")
	defer os.RemoveAll(stagingDir)
	runitDir, err := ioutil.TempDir("", "sb-runit")
	Assert(t).IsNil(err, "runitdir should have been created (test setup error)")
	defer os.RemoveAll(runitDir)

	fakeSb := expandLocal("fake_servicebuilder")

	return ServiceBuilder{
		ConfigRoot:  confDir,
		StagingRoot: stagingDir,
		RunitRoot:   runitDir,
		Bin:         fakeSb,
	}
}

func TestServiceBuilderCanTakeSBTemplates(t *testing.T) {
	template := NewSBTemplate("exemplar")
	template.AddEntry("exemplar", []string{"ls", "-lah", "/foo/bar"})

	builder := fakeServiceBuilder(t)
	defer os.RemoveAll(builder.ConfigRoot)
	defer os.RemoveAll(builder.StagingRoot)
	defer os.RemoveAll(builder.RunitRoot)
	confDir := builder.ConfigRoot

	outPath, err := builder.Write(template)
	Assert(t).IsNil(err, "there should not have been an error when writing the template")
	Assert(t).AreEqual(path.Join(confDir, "exemplar.yaml"), outPath, "the written servicebuilder path was not what we expected")

}

func TestServiceBuilderWillExecuteRebuild(t *testing.T) {
	builder := fakeServiceBuilder(t)
	defer os.RemoveAll(builder.ConfigRoot)
	defer os.RemoveAll(builder.StagingRoot)
	defer os.RemoveAll(builder.RunitRoot)

	b := bytes.Buffer{}
	err := builder.RebuildWithStreams(os.Stdin, &b)
	Assert(t).IsNil(err, "Could not execute fake servicebuilder")
	result := b.String()

	Assert(t).IsTrue(strings.Contains(result, builder.ConfigRoot), fmt.Sprintf("the config root should have been passed in cmd: %s", result))
}
