package pods

import (
	"os"
	"path"
	"runtime"

	"github.com/square/p2/pkg/runit"

	curl "github.com/andelf/go-curl"

	"io/ioutil"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

type fakeCurlEasyInit struct {
	callback func([]byte, interface{}) bool
	url      string
	fp       *os.File
}

func (fcei *fakeCurlEasyInit) Setopt(opt int, arg interface{}) error {
	if opt == curl.OPT_WRITEFUNCTION {
		fcei.callback = arg.(func([]byte, interface{}) bool)
	} else if opt == curl.OPT_URL {
		fcei.url = arg.(string)
	} else if opt == curl.OPT_WRITEDATA {
		fcei.fp = arg.(*os.File)
	}
	return nil
}

func (*fakeCurlEasyInit) Cleanup() {
}

func (fcei *fakeCurlEasyInit) Perform() error {
	fp := fcei.fp

	fcei.callback([]byte("test worked!"), fp)
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
		fcei := new(fakeCurlEasyInit)
		launchable := &HoistLaunchable{testLocation, stanza.LaunchableId, podId, fcei, tempDir}

		launchable.Install()

		Assert(t).AreEqual(testLocation, fcei.url, "The correct url wasn't set for the curl library")
		fileContents, err := ioutil.ReadFile(testPath)
		Assert(t).IsNil(err, "Didn't expect an error when reading the test file")
		Assert(t).AreEqual("test worked!", string(fileContents), "Test file didn't have the expected contents")
		Assert(t).AreEqual(testLocation, fcei.url, "Curl wasn't set to fetch the correct url")
	}
}

func TestInstallDir(t *testing.T) {
	tempDir := os.TempDir()
	testLocation := "http://someserver/test_launchable_abc123.tar.gz"
	launchable := &HoistLaunchable{testLocation, "testLaunchable", "testPod", new(fakeCurlEasyInit), tempDir}

	installDir := launchable.InstallDir()

	expectedDir := path.Join(tempDir, "installs", "test_launchable_abc123")
	Assert(t).AreEqual(expectedDir, installDir, "Install dir did not have expected value")
}

func FakeHoistLaunchableForDir(dirName string) *HoistLaunchable {
	_, filename, _, _ := runtime.Caller(0)
	launchableInstallDir := path.Join(path.Dir(filename), dirName)
	launchable := &HoistLaunchable{"testLaunchable.tar.gz", "testLaunchable", "testPod", new(fakeCurlEasyInit), launchableInstallDir}

	return launchable
}

func TestMultipleRunitServices(t *testing.T) {
	runitServices, err := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable").RunitServices(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1", "/var/service/testPod__testLaunchable__script2"}
	Assert(t).AreEqual(2, len(runitServices), "Found an unexpected number of runit services")
	Assert(t).AreEqual(expectedServicePaths[0], runitServices[0].Path, "Runit service paths from launchable did not match expected")
	Assert(t).AreEqual(expectedServicePaths[1], runitServices[1].Path, "Runit service paths from launchable did not match expected")
}

func TestSingleRunitService(t *testing.T) {
	runitServices, err := FakeHoistLaunchableForDir("single_script_test_hoist_launchable").RunitServices(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1"}
	Assert(t).AreEqual(1, len(runitServices), "Found an unexpected number of runit services")
	Assert(t).AreEqual(expectedServicePaths[0], runitServices[0].Path, "Runit service paths from launchable did not match expected")
}

func TestDisable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("successful_disable_test_hoist_launchable")

	disableOutput, err := hoistLaunchable.Disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := "disable invoked\n"

	Assert(t).AreEqual(expectedDisableOutput, disableOutput, "Did not get expected output from test disable script")
}

func TestFailingDisable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("failing_disable_test_hoist_launchable")

	disableOutput, err := hoistLaunchable.Disable()
	Assert(t).IsNotNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(expectedDisableOutput, disableOutput, "Did not get expected output from test disable script")
}
