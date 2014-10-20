package pods

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"

	. "github.com/anthonybishopric/gotcha"
)

type fakeCurl struct {
	url     string
	outPath string
}

func (fc *fakeCurl) File(url string, outPath string, args ...interface{}) error {
	fc.url = url
	fc.outPath = outPath

	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer f.Close()
	if err != nil {
		return err
	}
	f.Write([]byte("test worked!"))

	return nil
}

func TestInstall(t *testing.T) {
	tempDir := os.TempDir()
	testPath := path.Join(tempDir, "test_launchable.tar.gz")
	testLocation := "http://someserver/test_launchable.tar.gz"
	os.Remove(testPath)
	launchableStanzas := getLaunchableStanzasFromTestManifest()
	podId := getPodIdFromTestManifest()
	for _, stanza := range launchableStanzas {
		fc := new(fakeCurl)
		launchable := &HoistLaunchable{testLocation, stanza.LaunchableId, podId, fc.File, tempDir}

		launchable.Install()

		Assert(t).AreEqual(fc.url, testLocation, "The correct url wasn't set for the curl library")
		Assert(t).AreEqual(fc.outPath, testPath, "The correct url wasn't set for the curl library")
		fileContents, err := ioutil.ReadFile(testPath)
		Assert(t).IsNil(err, "Didn't expect an error when reading the test file")
		Assert(t).AreEqual(string(fileContents), "test worked!", "Test file didn't have the expected contents")
	}
}

func TestInstallDir(t *testing.T) {
	tempDir := os.TempDir()
	testLocation := "http://someserver/test_launchable_abc123.tar.gz"
	launchable := &HoistLaunchable{testLocation, "testLaunchable", "testPod", new(fakeCurl).File, tempDir}

	installDir := launchable.InstallDir()

	expectedDir := path.Join(tempDir, "installs", "test_launchable_abc123")
	Assert(t).AreEqual(installDir, expectedDir, "Install dir did not have expected value")
}

func FakeHoistLaunchableForDir(dirName string) *HoistLaunchable {
	_, filename, _, _ := runtime.Caller(0)
	launchableInstallDir := path.Join(path.Dir(filename), dirName)
	launchable := &HoistLaunchable{"testLaunchable.tar.gz", "testLaunchable", "testPod", new(fakeCurl).File, launchableInstallDir}

	return launchable
}

func TestMultipleExecutables(t *testing.T) {
	executables, err := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable").Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1", "/var/service/testPod__testLaunchable__script2"}
	Assert(t).AreEqual(2, len(executables), "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
	Assert(t).AreEqual(executables[1].Path, expectedServicePaths[1], "Runit service paths from launchable did not match expected")
}

func TestSingleRunitService(t *testing.T) {
	executables, err := FakeHoistLaunchableForDir("single_script_test_hoist_launchable").Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1"}
	Assert(t).AreEqual(len(executables), 1, "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
}

func TestDisable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")

	disableOutput, err := hoistLaunchable.Disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := "disable invoked\n"

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

func TestFailingDisable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")

	disableOutput, err := hoistLaunchable.Disable()
	Assert(t).IsNotNil(err, "Expected disable to fail for this test, but it didn't")

	expectedDisableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

// providing a disable script is optional, make sure we don't error
func TestNonexistentDisable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("nonexistent_scripts_test_hoist_launchable")

	disableOutput, err := hoistLaunchable.Disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := ""

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

func TestEnable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")

	enableOutput, err := hoistLaunchable.Enable()
	Assert(t).IsNil(err, "Got an unexpected error when calling enable on the test hoist launchable")

	expectedEnableOutput := "enable invoked\n"

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

func TestFailingEnable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")

	enableOutput, err := hoistLaunchable.Enable()
	Assert(t).IsNotNil(err, "Expected enable to fail for this test, but it didn't")

	expectedEnableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

// providing an enable script is optional, make sure we don't error
func TestNonexistentEnable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("nonexistent_scripts_test_hoist_launchable")

	enableOutput, err := hoistLaunchable.Enable()
	Assert(t).IsNil(err, "Got an unexpected error when calling enable on the test hoist launchable")

	expectedEnableOutput := ""

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

func TestStop(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")

	sv := runit.SV{util.From(runtime.Caller(0)).ExpandPath("../runit/fake_sv")}
	stopOutputs, err := hoistLaunchable.Stop(runit.DefaultBuilder, &sv)

	Assert(t).IsNil(err, "Got an unexpected error when attempting to stop runit services")
	Assert(t).AreEqual(stopOutputs[0], "stop /var/service/testPod__testLaunchable__script1\n", "sv invoked with incorrect arguments")
	Assert(t).AreEqual(stopOutputs[1], "stop /var/service/testPod__testLaunchable__script2\n", "sv invoked with incorrect arguments")
}

func TestFailingStop(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")

	sv := runit.SV{util.From(runtime.Caller(0)).ExpandPath("../runit/erring_sv")}
	stopOutputs, err := hoistLaunchable.Stop(runit.DefaultBuilder, &sv)

	Assert(t).AreEqual(len(stopOutputs), 2, "Got an unexpected number of outputs from sv stop")
	Assert(t).IsNotNil(err, "Expected sv stop to fail for this test, but it didn't")

	// erring script gives nothing on stdout, so we expect empty strings
	Assert(t).AreEqual(stopOutputs[0], "", "sv invoked with incorrect arguments")

	// we didn't even attempt to stop script2 because script1 fails, but output is
	// initialized to a length equaling number of services
	Assert(t).AreEqual(stopOutputs[1], "", "sv invoked with incorrect arguments")
}

func FakeServiceBuilder() *runit.ServiceBuilder {
	testDir := os.TempDir()
	fakeSBBinPath := util.From(runtime.Caller(0)).ExpandPath("../runit/fake_servicebuilder")
	configRoot := path.Join(testDir, "/etc/servicebuilder.d")
	os.MkdirAll(configRoot, 0755)
	_, err := os.Stat(configRoot)
	if err != nil {
		panic("unable to create test dir")
	}
	stagingRoot := path.Join(testDir, "/var/service-stage")
	os.MkdirAll(stagingRoot, 0755)
	runitRoot := path.Join(testDir, "/var/service")
	os.MkdirAll(runitRoot, 0755)

	return &runit.ServiceBuilder{
		ConfigRoot:  configRoot,
		StagingRoot: stagingRoot,
		RunitRoot:   runitRoot,
		Bin:         fakeSBBinPath,
	}
}

func TestStart(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	serviceBuilder := FakeServiceBuilder()
	executables, err := hoistLaunchable.Executables(serviceBuilder)
	err = hoistLaunchable.Start(serviceBuilder)
	outFilePath := path.Join(serviceBuilder.ConfigRoot, "testPod__testLaunchable.yaml")

	Assert(t).IsNil(err, "Got an unexpected error when attempting to start runit services")
	expectedLines := []string{
		fmt.Sprintf("%s:", executables[0].Name),
		"  run:",
		fmt.Sprintf("  - %s", executables[0].execPath),
		fmt.Sprintf("%s:", executables[1].Name),
		"  run:",
		fmt.Sprintf("  - %s", executables[1].execPath),
		"",
	}
	expected := strings.Join(expectedLines, "\n")

	f, err := os.Open(outFilePath)
	defer f.Close()
	bytes, err := ioutil.ReadAll(f)
	Assert(t).IsNil(err, "Got an unexpected error reading the servicebuilder yaml file")
	Assert(t).AreEqual(string(bytes), expected, "Servicebuilder yaml file didn't have expected contents")
}
