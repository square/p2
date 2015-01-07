package pods

import (
	"bytes"
	"fmt"
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
	return NewPod("test", "/data/pods/test")
}

func getTestPodManifest(t *testing.T) *PodManifest {
	testPath := util.From(runtime.Caller(0)).ExpandPath("test_manifest.yaml")
	pod, err := PodManifestFromPath(testPath)
	Assert(t).IsNil(err, "couldn't read test manifest")
	return pod
}

func getUpdatedManifest(t *testing.T) *PodManifest {
	podPath := util.From(runtime.Caller(0)).ExpandPath("updated_manifest.yaml")
	pod, err := PodManifestFromPath(podPath)
	Assert(t).IsNil(err, "couldn't read test manifest")
	return pod
}

func getLaunchableStanzasFromTestManifest(t *testing.T) map[string]LaunchableStanza {
	return getTestPodManifest(t).LaunchableStanzas
}

func TestGetLaunchable(t *testing.T) {
	launchableStanzas := getLaunchableStanzasFromTestManifest(t)
	pod := getTestPod()
	manifest := getTestPodManifest(t)
	Assert(t).AreNotEqual(0, len(launchableStanzas), "Expected there to be at least one launchable stanza in the test manifest")
	for _, stanza := range launchableStanzas {
		launchable, _ := pod.getLaunchable(stanza, manifest)
		Assert(t).AreEqual("hello__hello", launchable.Id, "LaunchableId did not have expected value")
		Assert(t).AreEqual("hoisted-hello_def456.tar.gz", launchable.Location, "Launchable location did not have expected value")
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
	Log.SetLogOut(&out)

	testLaunchable := &HoistLaunchable{Id: "TestLaunchable"}
	testManifest := getTestPodManifest(t)
	testErr := util.Errorf("Unable to do something")
	message := "Test error occurred"
	pod := PodFromManifestId(testManifest.Id)
	pod.logLaunchableError(testLaunchable.Id, testErr, message)

	output, err := ioutil.ReadAll(&out)
	Assert(t).IsNil(err, "Got an error reading the logging output")
	outputString := bytes.NewBuffer(output).String()
	Assert(t).Matches(outputString, ContainsString("TestLaunchable"), "Expected 'TestLaunchable' to appear somewhere in log output")
	Assert(t).Matches(outputString, ContainsString("hello"), "Expected 'hello' to appear somewhere in log output")
	Assert(t).Matches(outputString, ContainsString("Test error occurred"), "Expected error message to appear somewhere in log output")
}

func TestLogError(t *testing.T) {
	out := bytes.Buffer{}
	Log.SetLogOut(&out)

	testManifest := getTestPodManifest(t)
	testErr := util.Errorf("Unable to do something")
	message := "Test error occurred"
	pod := PodFromManifestId(testManifest.Id)
	pod.logError(testErr, message)

	output, err := ioutil.ReadAll(&out)
	Assert(t).IsNil(err, "Got an error reading the logging output")
	outputString := bytes.NewBuffer(output).String()
	Assert(t).Matches(outputString, ContainsString("hello"), "Expected 'hello' to appear somewhere in log output")
	Assert(t).Matches(outputString, ContainsString("Test error occurred"), "Expected error message to appear somewhere in log output")
}

func TestLogInfo(t *testing.T) {
	out := bytes.Buffer{}
	Log.SetLogOut(&out)

	testManifest := getTestPodManifest(t)
	pod := PodFromManifestId(testManifest.Id)
	message := "Pod did something good"
	pod.logInfo(message)

	output, err := ioutil.ReadAll(&out)
	Assert(t).IsNil(err, "Got an error reading the logging output")
	outputString := bytes.NewBuffer(output).String()
	Assert(t).Matches(outputString, ContainsString("hello"), "Expected 'hello' to appear somewhere in log output")
	Assert(t).Matches(outputString, ContainsString("Pod did something good"), "Expected error message to appear somewhere in log output")
}

func TestWriteManifestWillReturnOldManifestTempPath(t *testing.T) {
	existing := getTestPodManifest(t)
	updated := getUpdatedManifest(t)
	poddir, err := ioutil.TempDir("", "poddir")
	Assert(t).IsNil(err, "couldn't create tempdir")
	pod := NewPod("testPod", poddir)
	manifestContent, err := existing.Bytes()
	Assert(t).IsNil(err, "couldn't get manifest bytes")
	err = ioutil.WriteFile(pod.CurrentPodManifestPath(), manifestContent, 0744)
	Assert(t).IsNil(err, "should have written current manifest")

	oldPath, err := pod.writeCurrentManifest(updated)
	Assert(t).IsNil(err, "should have writtne the current manifest and linked the old one")

	writtenOld, err := PodManifestFromPath(oldPath)
	Assert(t).IsNil(err, "should have written a manifest to the old path")
	manifestMustEqual(existing, writtenOld, t)

	writtenCurrent, err := pod.CurrentManifest()
	Assert(t).IsNil(err, "the manifest was not written properly")
	manifestMustEqual(updated, writtenCurrent, t)
}

func TestBuildRunitServices(t *testing.T) {
	serviceBuilder := FakeServiceBuilder()
	serviceBuilderDir, err := ioutil.TempDir("", "servicebuilderDir")
	Assert(t).IsNil(err, "Got an unexpected error creating a temp directory")
	serviceBuilder.ConfigRoot = serviceBuilderDir
	pod := Pod{
		Id:             "testPod",
		path:           "/data/pods/testPod",
		ServiceBuilder: serviceBuilder,
	}
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	executables, err := hoistLaunchable.Executables(serviceBuilder)
	outFilePath := path.Join(serviceBuilder.ConfigRoot, "testPod.yaml")

	Assert(t).IsNil(err, "Got an unexpected error when attempting to start runit services")

	pod.BuildRunitServices([]HoistLaunchable{*hoistLaunchable})
	expected := fmt.Sprintf(`%s:
  run:
  - /usr/bin/nolimit
  - /usr/bin/chpst
  - -u
  - testPod:testPod
  - -e
  - %s
  - %s
%s:
  run:
  - /usr/bin/nolimit
  - /usr/bin/chpst
  - -u
  - testPod:testPod
  - -e
  - %s
  - %s
`, executables[0].Name,
		hoistLaunchable.ConfigDir,
		executables[0].execPath,
		executables[1].Name,
		hoistLaunchable.ConfigDir,
		executables[1].execPath)

	f, err := os.Open(outFilePath)
	defer f.Close()
	bytes, err := ioutil.ReadAll(f)
	Assert(t).IsNil(err, "Got an unexpected error reading the servicebuilder yaml file")
	Assert(t).AreEqual(string(bytes), expected, "Servicebuilder yaml file didn't have expected contents")
}

func TestUninstall(t *testing.T) {
	serviceBuilder := FakeServiceBuilder()
	serviceBuilderDir, err := ioutil.TempDir("", "servicebuilderDir")
	Assert(t).IsNil(err, "Got an unexpected error creating a temp directory")
	serviceBuilder.ConfigRoot = serviceBuilderDir
	testPodDir, err := ioutil.TempDir("", "testPodDir")
	Assert(t).IsNil(err, "Got an unexpected error creating a temp directory")
	pod := Pod{
		Id:             "testPod",
		path:           testPodDir,
		ServiceBuilder: serviceBuilder,
	}
	manifest := getTestPodManifest(t)
	manifestContent, err := manifest.Bytes()
	Assert(t).IsNil(err, "couldn't get manifest bytes")
	err = ioutil.WriteFile(pod.CurrentPodManifestPath(), manifestContent, 0744)
	Assert(t).IsNil(err, "should have written current manifest")

	serviceBuilderFilePath := path.Join(serviceBuilder.ConfigRoot, "testPod.yaml")
	err = ioutil.WriteFile(serviceBuilderFilePath, []byte("stuff"), 0744)
	Assert(t).IsNil(err, "Error writing fake servicebuilder file")

	err = pod.Uninstall()
	Assert(t).IsNil(err, "Error uninstalling pod")
	_, err = os.Stat(serviceBuilderFilePath)
	Assert(t).IsTrue(os.IsNotExist(err), "Expected file to not exist after uninstall")
	_, err = os.Stat(pod.CurrentPodManifestPath())
	Assert(t).IsTrue(os.IsNotExist(err), "Expected file to not exist after uninstall")
}

func manifestMustEqual(expected, actual *PodManifest, t *testing.T) {
	actualSha, err := actual.SHA()
	Assert(t).IsNil(err, "should have gotten SHA from old manifest")
	expectedSha, err := expected.SHA()
	Assert(t).IsNil(err, "should have gotten SHA from known old manifest")
	manifestBytes, err := expected.Bytes()
	Assert(t).IsNil(err, "should have gotten bytes from manifest")
	actualBytes, err := actual.Bytes()
	Assert(t).IsNil(err, "should have gotten bytes from writtenOld")
	Assert(t).AreEqual(expectedSha, actualSha, fmt.Sprintf("known: \n\n%s\n\nactual:\n\n%s\n", string(manifestBytes), string(actualBytes)))
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
