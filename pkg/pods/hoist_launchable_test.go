package pods

import (
	"os"
	"path"
	"runtime"

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
		if err != nil {
			panic(err)
		}
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

func RunitServicesForFakeHoistLaunchableForDir(dirName string) []string {
	_, filename, _, _ := runtime.Caller(0)
	launchableInstallDir := path.Join(path.Dir(filename), dirName)
	launchable := &HoistLaunchable{"testLaunchable.tar.gz", "testLaunchable", "testPod", new(fakeCurlEasyInit), launchableInstallDir}

	runitServices, err := launchable.RunitServices()
	if err != nil {
		panic(err)
	}
	return runitServices
}

func TestMultipleRunitServices(t *testing.T) {
	runitServices := RunitServicesForFakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")

	expectedServices := []string{"/var/service/testPod__testLaunchable__script1", "/var/service/testPod__testLaunchable__script2"}
	Assert(t).AreEqual(2, len(runitServices), "Found an unexpected number of runit services")
	Assert(t).AreEqual(expectedServices[0], runitServices[0], "Runit service paths from launchable did not match expected")
	Assert(t).AreEqual(expectedServices[1], runitServices[1], "Runit service paths from launchable did not match expected")
}

func TestSingleRunitService(t *testing.T) {
	runitServices := RunitServicesForFakeHoistLaunchableForDir("single_script_test_hoist_launchable")

	expectedServices := []string{"/var/service/testPod__testLaunchable__script1"}
	Assert(t).AreEqual(1, len(runitServices), "Found an unexpected number of runit services")
	Assert(t).AreEqual(expectedServices[0], runitServices[0], "Runit service paths from launchable did not match expected")
}
