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

	f, err := os.Open(outPath)
	defer f.Close()
	Assert(t).IsNil(err, "Could not open out path for exemplar")
	content, err := ioutil.ReadAll(f)
	Assert(t).IsNil(err, "Could not read file")

	expected := `exemplar:
  run:
  - ls
  - -lah
  - /foo/bar
`
	Assert(t).AreEqual(expected, string(content), "The generated YAML was not what was expected")

	// attempt to write the file a second time. This verifies that we are actually able to do it without erring
	_, err = builder.Write(template)
	Assert(t).IsNil(err, "Was not able to write the servicebuilder file a second time")
}

func TestServiceBuilderWillExecuteRebuild(t *testing.T) {
	builder := fakeServiceBuilder(t)
	defer os.RemoveAll(builder.ConfigRoot)
	defer os.RemoveAll(builder.StagingRoot)
	defer os.RemoveAll(builder.RunitRoot)

	output, err := builder.RebuildWithStreams(os.Stdin)
	Assert(t).IsNil(err, "Could not execute fake servicebuilder")

	Assert(t).IsTrue(strings.Contains(output, builder.ConfigRoot), fmt.Sprintf("the config root should have been passed in cmd: %s", output))
}

func TestServiceBuilderCanRemoveServiceFiles(t *testing.T) {
	builder := fakeServiceBuilder(t)
	defer os.RemoveAll(builder.ConfigRoot)
	defer os.RemoveAll(builder.StagingRoot)
	defer os.RemoveAll(builder.RunitRoot)
	template := NewSBTemplate("exemplar")
	filePath := path.Join(builder.ConfigRoot, "exemplar.yaml")

	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	Assert(t).IsNil(err, "unable to open fake servicebuilder file for writing")

	_, err = f.WriteString("foo")
	Assert(t).IsNil(err, "unable to write fake servicebuilder file")

	_, err = os.Stat(filePath)
	Assert(t).IsNil(err, "Unable to stat the fake servicebuilder file")
	builder.Remove(template)
	_, err = os.Stat(filePath)
	Assert(t).IsNotNil(err, "File still exists after it was supposed to be removed")

}
