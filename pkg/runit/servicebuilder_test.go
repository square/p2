package runit

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"
)

var fakeTemplate map[string]ServiceTemplate = map[string]ServiceTemplate{
	"foo": ServiceTemplate{
		Run: []string{"foo", "one", "two"},
		Log: []string{"log", "three", "four"},
	},
}

func TestWriteTemplate(t *testing.T) {
	sb := FakeServiceBuilder()
	defer sb.Cleanup()

	fakePath := filepath.Join(sb.ConfigRoot, "test.yaml")
	err := sb.write(fakePath, fakeTemplate)
	Assert(t).IsNil(err, "should have written templates")

	fakeYaml, err := ioutil.ReadFile(fakePath)
	Assert(t).IsNil(err, "should have read templates")

	test := make(map[string]ServiceTemplate)
	err = yaml.Unmarshal(fakeYaml, test)
	Assert(t).IsNil(err, "should have parsed templates")

	Assert(t).IsTrue(reflect.DeepEqual(test, fakeTemplate), "should have been equal")

	// write an empty template, the file should disappear
	err = sb.write(fakePath, map[string]ServiceTemplate{})
	Assert(t).IsNil(err, "should not erred when writing empty template")

	_, err = os.Stat(fakePath)
	Assert(t).IsTrue(os.IsNotExist(err), "The empty template should have removed the servicebuilder file")

	// if the file doesn't exist, no-op
	err = sb.write(fakePath, map[string]ServiceTemplate{})
	Assert(t).IsNil(err, "should not erred when writing empty template")

	_, err = os.Stat(fakePath)
	Assert(t).IsTrue(os.IsNotExist(err), "The empty template should have removed the servicebuilder file")
}

func TestStagedContents(t *testing.T) {
	sb := FakeServiceBuilder()
	defer sb.Cleanup()

	err := sb.stage(fakeTemplate, RestartPolicyAlways)
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

	info, err = os.Stat(filepath.Join(sb.StagingRoot, "foo", "log", "main"))
	Assert(t).IsNil(err, "should have statted staging log main")
	Assert(t).IsTrue(info.IsDir(), "staging log main should have been a dir")

	// There should be no 'down' file if the restart policy is 'always'
	_, err = os.Stat(filepath.Join(sb.StagingRoot, "foo", "down"))
	Assert(t).IsNotNil(err, "down file should not have existed when restart policy is 'always'")
	Assert(t).IsTrue(os.IsNotExist(err), "down file should not have existed when restart policy is 'always'")
}

func TestDownFile(t *testing.T) {
	sb := FakeServiceBuilder()
	defer sb.Cleanup()

	err := sb.stage(fakeTemplate, RestartPolicyNever)
	Assert(t).IsNil(err, "should have staged")

	// There should be a 'down' file if the restart policy is 'never' to
	// prevent runit from starting the process automatically
	_, err = os.Stat(filepath.Join(sb.StagingRoot, "foo", "down"))
	Assert(t).IsNil(err, "down file should have existed when restart policy is 'always'")

	// Now stage it again as RestartPolicyAlways and make sure 'down' file
	// is removed
	err = sb.stage(fakeTemplate, RestartPolicyAlways)
	Assert(t).IsNil(err, "should have staged")

	// There should be a 'down' file if the restart policy is 'never' to
	// prevent runit from starting the process automatically
	_, err = os.Stat(filepath.Join(sb.StagingRoot, "foo", "down"))
	Assert(t).IsNotNil(err, "down file should have been removed when restart policy is 'always'")
	Assert(t).IsTrue(os.IsNotExist(err), "down file should not have existed when restart policy is 'always'")
}

func verifyRuby18(t *testing.T, filename, displayName string) {
	binData, err := ioutil.ReadFile(filename)
	Assert(t).IsNil(err, fmt.Sprintf("should have been able to read %s", displayName))
	data := string(binData)

	if strings.Contains(data, "\n---\n") {
		t.Errorf("trailing space was removed from \"--- \" in %s", displayName)
	} else if !strings.Contains(data, "\n--- \n") {
		t.Errorf("expected \"--- \" YAML separator in %s", displayName)
	}
}

func TestRuby18Yaml(t *testing.T) {
	sb := FakeServiceBuilder()
	defer sb.Cleanup()

	err := sb.stage(fakeTemplate, RestartPolicyAlways)
	Assert(t).IsNil(err, "should have staged")

	verifyRuby18(t, filepath.Join(sb.StagingRoot, "foo", "run"), "run script")
	verifyRuby18(t, filepath.Join(sb.StagingRoot, "foo", "log", "run"), "log/run script")
}

func TestActivateSucceedsTwice(t *testing.T) {
	sb := FakeServiceBuilder()
	defer sb.Cleanup()

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
	defer sb.Cleanup()

	// first create the service
	err := sb.Activate("foo", fakeTemplate, RestartPolicyAlways)
	Assert(t).IsNil(err, "should have activated service")
	_, err = os.Readlink(filepath.Join(sb.RunitRoot, "foo"))
	Assert(t).IsNil(err, "should have created activation symlink")

	// now remove the service's yaml file
	err = sb.Activate("foo", map[string]ServiceTemplate{}, RestartPolicyAlways)
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
