package pods

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"runtime"
	"testing"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"

	. "github.com/anthonybishopric/gotcha"
)

func TestInstall(t *testing.T) {
	testDir := util.From(runtime.Caller(0))
	launchableHome, err := ioutil.TempDir("", "test_launchable")
	defer os.RemoveAll(launchableHome)
	expectedDownloadPath := path.Join(os.TempDir(), "hoisted-hello_def456")
	testLocation := testDir.ExpandPath("hoisted-hello_def456.tar.gz")
	if _, err = os.Stat(testLocation); os.IsNotExist(err) {
		t.Fatalf("test setup: %s is not a valid hoist artifact", testLocation)
	}
	os.Remove(expectedDownloadPath)
	manifest, err := PodManifestFromPath(testDir.ExpandPath("test_manifest.yaml"))
	Assert(t).IsNil(err, "should not have erred trying to read manifest")
	launchableStanzas := manifest.LaunchableStanzas
	Assert(t).IsTrue(len(manifest.LaunchableStanzas) > 0, "test setup: should have been more than zero launchables")
	currentUser, err := user.Current()
	Assert(t).IsNil(err, "test setup: couldn't get current user")
	for _, stanza := range launchableStanzas {
		fc := new(FakeCurl)

		launchable := &HoistLaunchable{testLocation, stanza.LaunchableId, currentUser.Username, launchableHome, fc.File, launchableHome}

		err = launchable.Install()
		Assert(t).IsNil(err, "there should not have been an error")

		Assert(t).AreEqual(fc.url, testLocation, "The correct url wasn't set for the curl library")
		Assert(t).AreEqual(fc.outPath, expectedDownloadPath, "The correct outPath wasn't set for the curl library")

		targetDownloadedFile, err := ioutil.ReadFile(expectedDownloadPath)
		Assert(t).IsNil(err, "Didn't expect an error when reading the test file")

		sourceDownloadedFile, err := ioutil.ReadFile(testLocation)
		Assert(t).IsNil(err, "Didn't expect an error when reading the test file")

		Assert(t).AreEqual(string(targetDownloadedFile), string(sourceDownloadedFile), "Test file didn't have the expected contents")

		hoistedHelloUnpacked := path.Join(launchableHome, "installs", "hoisted-hello_def456")
		if info, err := os.Stat(hoistedHelloUnpacked); err != nil || !info.IsDir() {

			t.Fatalf("Expected %s to be the unpacked artifact location", hoistedHelloUnpacked)
		}
	}
}

func TestInstallDir(t *testing.T) {
	tempDir := os.TempDir()
	testLocation := "http://someserver/test_launchable_abc123.tar.gz"
	launchable := &HoistLaunchable{testLocation, "testLaunchable", "testuser", tempDir, new(FakeCurl).File, tempDir}

	installDir := launchable.InstallDir()

	expectedDir := path.Join(tempDir, "installs", "test_launchable_abc123")
	Assert(t).AreEqual(installDir, expectedDir, "Install dir did not have expected value")
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
	launchable := FakeHoistLaunchableForDir("single_script_test_hoist_launchable")
	Assert(t).IsNil(launchable.MakeCurrent(), "Should have been made current")
	executables, err := launchable.Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1"}
	Assert(t).AreEqual(len(executables), 1, "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
}

func TestLaunchExecutableOnlyRunitService(t *testing.T) {
	launchable := FakeHoistLaunchableForDir("launch_script_only_test_hoist_launchable")
	Assert(t).IsNil(launchable.MakeCurrent(), "Should have been made current")
	executables, err := launchable.Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__launch"}
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

func TestFailingStop(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")

	sv := runit.SV{util.From(runtime.Caller(0)).ExpandPath("erring_sv")}
	err := hoistLaunchable.Stop(runit.DefaultBuilder, &sv)

	Assert(t).IsNotNil(err, "Expected sv stop to fail for this test, but it didn't")
}

func TestStart(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	serviceBuilder := FakeServiceBuilder()
	sv := runit.SV{util.From(runtime.Caller(0)).ExpandPath("fake_sv")}
	executables, err := hoistLaunchable.Executables(serviceBuilder)
	sbContents := fmt.Sprintf(`%s:
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
		executables[0].ExecPath,
		executables[1].Name,
		hoistLaunchable.ConfigDir,
		executables[1].ExecPath)

	outFilePath := path.Join(serviceBuilder.ConfigRoot, "testPod__testLaunchable.yaml")
	f, err := os.Open(outFilePath)
	defer f.Close()
	f.Write([]byte(sbContents))

	err = hoistLaunchable.Start(serviceBuilder, &sv)

	Assert(t).IsNil(err, "Got an unexpected error when attempting to start runit services")

}

func TestFailingStart(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	serviceBuilder := FakeServiceBuilder()
	sv := runit.SV{util.From(runtime.Caller(0)).ExpandPath("erring_sv")}
	executables, _ := hoistLaunchable.Executables(serviceBuilder)
	outFilePath := path.Join(serviceBuilder.ConfigRoot, "testPod__testLaunchable.yaml")

	sbContents := fmt.Sprintf(`%s:
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
		executables[0].ExecPath,
		executables[1].Name,
		hoistLaunchable.ConfigDir,
		executables[1].ExecPath)

	f, err := os.Open(outFilePath)
	defer f.Close()
	f.Write([]byte(sbContents))

	err = hoistLaunchable.Start(serviceBuilder, &sv)
	Assert(t).IsNotNil(err, "Expected an error starting runit services")
}

func TestStop(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")

	sv := runit.SV{util.From(runtime.Caller(0)).ExpandPath("fake_sv")}
	err := hoistLaunchable.Stop(runit.DefaultBuilder, &sv)

	Assert(t).IsNil(err, "Got an unexpected error when attempting to stop runit services")
}
