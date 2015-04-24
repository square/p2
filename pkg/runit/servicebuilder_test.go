package runit

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"gopkg.in/yaml.v2"
)

var fakeTemplate map[string]ServiceTemplate = map[string]ServiceTemplate{
	"foo": ServiceTemplate{
		Run: []string{"foo", "one", "two"},
	},
}

func TestWriteTemplate(t *testing.T) {
	sb := FakeServiceBuilder()
	defer os.RemoveAll(sb.ConfigRoot)
	defer os.RemoveAll(sb.StagingRoot)
	defer os.RemoveAll(sb.RunitRoot)

	fakePath := filepath.Join(sb.ConfigRoot, "test.yaml")
	err := sb.write(fakePath, fakeTemplate)
	Assert(t).IsNil(err, "should have written templates")

	fakeYaml, err := ioutil.ReadFile(fakePath)
	Assert(t).IsNil(err, "should have read templates")

	test := make(map[string]ServiceTemplate)
	err = yaml.Unmarshal(fakeYaml, test)
	Assert(t).IsNil(err, "should have parsed templates")

	Assert(t).IsTrue(reflect.DeepEqual(test, fakeTemplate), "should have been equal")
}

func TestStagedContents(t *testing.T) {
	sb := FakeServiceBuilder()
	defer os.RemoveAll(sb.ConfigRoot)
	defer os.RemoveAll(sb.StagingRoot)
	defer os.RemoveAll(sb.RunitRoot)

	err := sb.stage(fakeTemplate)
	Assert(t).IsNil(err, "should have staged")

	info, err := os.Stat(filepath.Join(sb.StagingRoot, "foo"))
	Assert(t).IsNil(err, "should have statted staging dir")
	Assert(t).IsTrue(info.IsDir(), "staging dir should have been a dir")

	info, err = os.Stat(filepath.Join(sb.StagingRoot, "foo", "run"))
	Assert(t).IsNil(err, "should have statted staging run")
	Assert(t).IsFalse(info.IsDir(), "staging run should have been a file")
	Assert(t).IsTrue(info.Mode()&0100 == 0100, "staging run should have been executable")

	info, err = os.Stat(filepath.Join(sb.StagingRoot, "foo", "log"))
	Assert(t).IsNil(err, "should have statted staging log dir")
	Assert(t).IsTrue(info.IsDir(), "staging log dir should have been a dir")

	info, err = os.Stat(filepath.Join(sb.StagingRoot, "foo", "log", "run"))
	Assert(t).IsNil(err, "should have statted staging log run")
	Assert(t).IsFalse(info.IsDir(), "staging log run should have been a file")
	Assert(t).IsTrue(info.Mode()&0100 == 0100, "staging log run should have been executable")
}

func TestActivateSucceedsTwice(t *testing.T) {
	sb := FakeServiceBuilder()
	defer os.RemoveAll(sb.ConfigRoot)
	defer os.RemoveAll(sb.StagingRoot)
	defer os.RemoveAll(sb.RunitRoot)

	err := sb.activate(fakeTemplate)
	Assert(t).IsNil(err, "should have activated service")
	target, err := os.Readlink(filepath.Join(sb.RunitRoot, "foo"))
	Assert(t).IsNil(err, "should have read link for activated service")
	Assert(t).IsTrue(target == filepath.Join(sb.StagingRoot, "foo"), "should have symlinked to the staging dir")

	// do it again to make sure repeated activations work fine
	err = sb.activate(fakeTemplate)
	Assert(t).IsNil(err, "should have activated service")
	target, err = os.Readlink(filepath.Join(sb.RunitRoot, "foo"))
	Assert(t).IsNil(err, "should have read link for activated service")
	Assert(t).IsTrue(target == filepath.Join(sb.StagingRoot, "foo"), "should have symlinked to the staging dir")
}

func TestPrune(t *testing.T) {
	sb := FakeServiceBuilder()
	defer os.RemoveAll(sb.ConfigRoot)
	defer os.RemoveAll(sb.StagingRoot)
	defer os.RemoveAll(sb.RunitRoot)

	// first create the service
	err := sb.Activate("foo", fakeTemplate)
	Assert(t).IsNil(err, "should have activated service")
	_, err = os.Readlink(filepath.Join(sb.RunitRoot, "foo"))
	Assert(t).IsNil(err, "should have created activation symlink")

	// now remove the service's yaml file
	err = sb.Activate("foo", map[string]ServiceTemplate{})
	Assert(t).IsNil(err, "should have activated service")
	// symlink should still exist, haven't pruned yet
	_, err = os.Readlink(filepath.Join(sb.RunitRoot, "foo"))
	Assert(t).IsNil(err, "should have created activation symlink")

	err = sb.Prune()
	Assert(t).IsNil(err, "should have pruned")
	_, err = os.Readlink(filepath.Join(sb.RunitRoot, "foo"))
	Assert(t).IsTrue(os.IsNotExist(err), "should have removed activation symlink")
	_, err = os.Stat(filepath.Join(sb.StagingRoot, "foo"))
	Assert(t).IsTrue(os.IsNotExist(err), "should have removed staging dir")
}
