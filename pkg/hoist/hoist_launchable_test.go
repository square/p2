package hoist

import (
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"runtime"
	"testing"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
)

func TestInstall(t *testing.T) {
	fetcher := uri.NewLoggedFetcher(nil)
	testContext := util.From(runtime.Caller(0))

	currentUser, err := user.Current()
	Assert(t).IsNil(err, "test setup: couldn't get current user")

	testLocation := testContext.ExpandPath("hoisted-hello_def456.tar.gz")
	launchableHome, err := ioutil.TempDir("", "launchable_home")
	defer os.RemoveAll(launchableHome)

	launchable := &Launchable{
		Location:  testLocation,
		Id:        "hello",
		RunAs:     currentUser.Username,
		ConfigDir: launchableHome,
		Fetcher:   fetcher,
		RootDir:   launchableHome,
	}

	err = launchable.Install()
	Assert(t).IsNil(err, "there should not have been an error when installing")

	Assert(t).AreEqual(
		fetcher.SrcUri,
		testLocation,
		"The correct url wasn't set for the curl library",
	)

	hoistedHelloUnpacked := path.Join(launchableHome, "installs", "hoisted-hello_def456")
	if info, err := os.Stat(hoistedHelloUnpacked); err != nil || !info.IsDir() {
		t.Fatalf("Expected %s to be the unpacked artifact location", hoistedHelloUnpacked)
	}
	helloLaunch := path.Join(hoistedHelloUnpacked, "bin", "launch")
	if info, err := os.Stat(helloLaunch); err != nil || info.IsDir() {
		t.Fatalf("Expected %s to be a the launch script for hello", helloLaunch)
	}
}

func TestInstallDir(t *testing.T) {
	tempDir := os.TempDir()
	testLocation := "http://someserver/test_launchable_abc123.tar.gz"
	launchable := &Launchable{
		Location:  testLocation,
		Id:        "testLaunchable",
		RunAs:     "testuser",
		ConfigDir: tempDir,
		Fetcher:   uri.DefaultFetcher,
		RootDir:   tempDir,
	}

	installDir := launchable.InstallDir()

	expectedDir := path.Join(tempDir, "installs", "test_launchable_abc123")
	Assert(t).AreEqual(installDir, expectedDir, "Install dir did not have expected value")
}

func TestMultipleExecutables(t *testing.T) {
	fakeLaunchable, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(fakeLaunchable, sb)
	executables, err := fakeLaunchable.Executables(runit.DefaultBuilder)

	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1", "/var/service/testPod__testLaunchable__script2"}
	Assert(t).AreEqual(2, len(executables), "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
	Assert(t).AreEqual(executables[1].Service.Path, expectedServicePaths[1], "Runit service paths from launchable did not match expected")
}

func TestSingleRunitService(t *testing.T) {
	launchable, sb := FakeHoistLaunchableForDir("single_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(launchable, sb)
	Assert(t).IsNil(launchable.MakeCurrent(), "Should have been made current")
	executables, err := launchable.Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1"}
	Assert(t).AreEqual(len(executables), 1, "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
}

func TestLaunchExecutableOnlyRunitService(t *testing.T) {
	launchable, sb := FakeHoistLaunchableForDir("launch_script_only_test_hoist_launchable")
	defer CleanupFakeLaunchable(launchable, sb)
	Assert(t).IsNil(launchable.MakeCurrent(), "Should have been made current")
	executables, err := launchable.Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__launch"}
	Assert(t).AreEqual(len(executables), 1, "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
}

func TestDisable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	disableOutput, err := hl.disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := "disable invoked\n"

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

func TestFailingDisable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	disableOutput, err := hl.disable()
	Assert(t).IsNotNil(err, "Expected disable to fail for this test, but it didn't")

	expectedDisableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

// providing a disable script is optional, make sure we don't error
func TestNonexistentDisable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("nonexistent_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	disableOutput, err := hl.disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := ""

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

func TestEnable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	enableOutput, err := hl.enable()
	Assert(t).IsNil(err, "Got an unexpected error when calling enable on the test hoist launchable")

	expectedEnableOutput := "enable invoked\n"

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

func TestFailingEnable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	enableOutput, err := hl.enable()
	Assert(t).IsNotNil(err, "Expected enable to fail for this test, but it didn't")

	expectedEnableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

// providing an enable script is optional, make sure we don't error
func TestNonexistentEnable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("nonexistent_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	enableOutput, err := hl.enable()
	Assert(t).IsNil(err, "Got an unexpected error when calling enable on the test hoist launchable")

	expectedEnableOutput := ""

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

func TestFailingStop(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.ErringSV()

	err := hl.stop(sb, sv)

	Assert(t).IsNotNil(err, "Expected sv stop to fail for this test, but it didn't")
}

func TestStart(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)
	sv := runit.FakeSV()
	executables, err := hl.Executables(sb)
	outFilePath := path.Join(sb.ConfigRoot, "testPod__testLaunchable.yaml")

	sbContentsMap := map[string]interface{}{
		executables[0].Service.Name: map[string]interface{}{
			"run": executables[0].Exec,
		},
		executables[1].Service.Name: map[string]interface{}{
			"run": executables[1].Exec,
		},
	}
	sbContents, err := yaml.Marshal(sbContentsMap)
	Assert(t).IsNil(err, "should have no error marshalling servicebuilder map")
	f, err := os.Open(outFilePath)
	defer f.Close()
	f.Write(sbContents)

	err = hl.start(sb, sv)

	Assert(t).IsNil(err, "Got an unexpected error when attempting to start runit services")

}

func TestFailingStart(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.ErringSV()
	executables, _ := hl.Executables(sb)
	outFilePath := path.Join(sb.ConfigRoot, "testPod__testLaunchable.yaml")

	sbContentsMap := map[string]interface{}{
		executables[0].Service.Name: map[string]interface{}{
			"run": executables[0].Exec,
		},
		executables[1].Service.Name: map[string]interface{}{
			"run": executables[1].Exec,
		},
	}
	sbContents, err := yaml.Marshal(sbContentsMap)
	Assert(t).IsNil(err, "should have no error marshalling servicebuilder map")
	f, err := os.Open(outFilePath)
	defer f.Close()
	f.Write(sbContents)

	err = hl.start(sb, sv)
	Assert(t).IsNotNil(err, "Expected an error starting runit services")
}

func TestStop(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.FakeSV()
	err := hl.stop(sb, sv)

	Assert(t).IsNil(err, "Got an unexpected error when attempting to stop runit services")
}
