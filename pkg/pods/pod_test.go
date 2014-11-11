package pods

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"

	"github.com/square/p2/pkg/util"

	. "github.com/anthonybishopric/gotcha"
)

func getTestPod() *Pod {
	return &Pod{"/data/pods/test"}
}

func getTestPodManifest() *PodManifest {
	_, filename, _, _ := runtime.Caller(0)
	testPath := path.Join(path.Dir(filename), "test_manifest.yaml")
	pod, _ := PodManifestFromPath(testPath)
	return pod
}

func getLaunchableStanzasFromTestManifest() map[string]LaunchableStanza {
	return getTestPodManifest().LaunchableStanzas
}

func getPodIdFromTestManifest() string {
	return getTestPodManifest().Id
}

func TestGetLaunchable(t *testing.T) {
	launchableStanzas := getLaunchableStanzasFromTestManifest()
	pod := getTestPod()
	manifest := getTestPodManifest()
	Assert(t).AreNotEqual(0, len(launchableStanzas), "Expected there to be at least one launchable stanza in the test manifest")
	for _, stanza := range launchableStanzas {
		launchable, _ := pod.getLaunchable(stanza, manifest)
		Assert(t).AreEqual("hello__hello", launchable.Id, "LaunchableId did not have expected value")
		Assert(t).AreEqual("http://localhost:8000/foo/bar/baz/hello_abc123_vagrant.tar.gz", launchable.Location, "Launchable location did not have expected value")
	}
}

func TestPodCanWriteEnvFile(t *testing.T) {
	envDir, err := ioutil.TempDir("", "envdir")
	Assert(t).IsNil(err, "Should not have been an error writing the env dir")
	defer os.RemoveAll(envDir)

	err = writeEnvFile(envDir, "ENVIRONMENT", "staging")
	Assert(t).IsNil(err, "There should not have been an error writing the config file")

	expectedWritten := path.Join(envDir, "ENVIRONMENT")
	file, err := os.Open(expectedWritten)
	defer file.Close()
	Assert(t).IsNil(err, "There should not have been an error when opening the config file")
	contents, err := ioutil.ReadAll(file)
	Assert(t).IsNil(err, "There should not have been an error reading the content of the config file")
	Assert(t).AreEqual("staging", string(contents), "the config file should have been the value 'staging'")
}

func TestPodSetupConfigWritesFiles(t *testing.T) {
	envDir, err := ioutil.TempDir("", "envdir")
	Assert(t).IsNil(err, "Should not have been an error writing the env dir")
	configDir, err := ioutil.TempDir("", "confdir")
	Assert(t).IsNil(err, "Should not have been an error writing the env dir")
	defer os.RemoveAll(envDir)
	defer os.RemoveAll(configDir)
	manifestStr := `id: thepod
launchables:
  my-app:
    launchable_type: hoist
    launchable_id: web
    location: https://localhost:4444/foo/bar/baz.tar.gz
config:
  ENVIRONMENT: staging
`
	manifest, err := PodManifestFromBytes(bytes.NewBufferString(manifestStr).Bytes())
	Assert(t).IsNil(err, "should not have erred reading the manifest")

	err = setupConfig(envDir, configDir, manifest)
	Assert(t).IsNil(err, "There shouldn't have been an error setting up config")

	configFileName, err := manifest.ConfigFileName()
	Assert(t).IsNil(err, "Couldn't generate config filename")
	configPath := path.Join(configDir, configFileName)
	config, err := ioutil.ReadFile(configPath)
	Assert(t).IsNil(err, "should not have erred reading the config")
	Assert(t).AreEqual("ENVIRONMENT: staging\n", string(config), "the config didn't match")

	env, err := ioutil.ReadFile(path.Join(envDir, "CONFIG_PATH"))
	Assert(t).IsNil(err, "should not have erred reading the env file")
	Assert(t).AreEqual(configPath, string(env), "The env path to config didn't match")
}

func TestLogLaunchableError(t *testing.T) {
	out := bytes.Buffer{}
	SetLogOut(&out)

	testLaunchable := &HoistLaunchable{Id: "TestLaunchable"}
	testManifest := getTestPodManifest()
	testErr := util.Errorf("Unable to do something")
	message := "Test error occurred"
	logLaunchableError(testManifest.ID(), testLaunchable.Id, testErr, message)

	output, err := ioutil.ReadAll(&out)
	Assert(t).IsNil(err, "Got an error reading the logging output")
	outputString := bytes.NewBuffer(output).String()
	Assert(t).Matches(outputString, ContainsString("TestLaunchable"), "Expected 'TestLaunchable' to appear somewhere in log output")
	Assert(t).Matches(outputString, ContainsString("hello"), "Expected 'hello' to appear somewhere in log output")
	Assert(t).Matches(outputString, ContainsString("Test error occurred"), "Expected error message to appear somewhere in log output")
}

func TestLogPodError(t *testing.T) {
	out := bytes.Buffer{}
	SetLogOut(&out)

	testManifest := getTestPodManifest()
	testErr := util.Errorf("Unable to do something")
	message := "Test error occurred"
	logPodError(testManifest.ID(), testErr, message)

	output, err := ioutil.ReadAll(&out)
	Assert(t).IsNil(err, "Got an error reading the logging output")
	outputString := bytes.NewBuffer(output).String()
	Assert(t).Matches(outputString, ContainsString("hello"), "Expected 'hello' to appear somewhere in log output")
	Assert(t).Matches(outputString, ContainsString("Test error occurred"), "Expected error message to appear somewhere in log output")
}

func TestLogPodInfo(t *testing.T) {
	out := bytes.Buffer{}
	SetLogOut(&out)

	testManifest := getTestPodManifest()
	message := "Pod did something good"
	logPodInfo(testManifest.ID(), message)

	output, err := ioutil.ReadAll(&out)
	Assert(t).IsNil(err, "Got an error reading the logging output")
	outputString := bytes.NewBuffer(output).String()
	Assert(t).Matches(outputString, ContainsString("hello"), "Expected 'hello' to appear somewhere in log output")
	Assert(t).Matches(outputString, ContainsString("Pod did something good"), "Expected error message to appear somewhere in log output")
}

func ContainsString(test string) func(interface{}) bool {
	return func(subject interface{}) bool {
		if subjectString, ok := subject.(string); ok {
			return strings.Contains(subjectString, test)
		} else {
			return false
		}
	}
}
