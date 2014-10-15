package pods

import (
	"os"
	"path"

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
		launchable := &HoistLaunchable{testLocation, stanza.LaunchableId, podId, fcei}

		launchable.Install(tempDir)

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
	launchable := &HoistLaunchable{testLocation, "testLaunchable", "testPod", new(fakeCurlEasyInit)}

	installDir := launchable.InstallDir(path.Join(tempDir, "testPod"))

	expectedDir := path.Join(tempDir, "testPod", "testLaunchable", "installs", "test_launchable_abc123")
	Assert(t).AreEqual(expectedDir, installDir, "Install dir did not have expected value")
}
