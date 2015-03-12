package hoist

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"runtime"
	"testing"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"

	. "github.com/anthonybishopric/gotcha"
)

func TestInstall(t *testing.T) {
	fc := new(FakeCurl)
	testContext := util.From(runtime.Caller(0))

	currentUser, err := user.Current()
	Assert(t).IsNil(err, "test setup: couldn't get current user")

	testLocation := testContext.ExpandPath("hoisted-hello_def456.tar.gz")
	launchableHome, err := ioutil.TempDir("", "launchable_home")
	defer os.RemoveAll(launchableHome)

	launchable := &HoistLaunchable{
		Location:    testLocation,
		Id:          "hello",
		RunAs:       currentUser.Username,
		ConfigDir:   launchableHome,
		FetchToFile: fc.File,
		RootDir:     launchableHome,
		Chpst:       FakeChpst(),
		Cgexec:      cgroups.FakeCgexec(),
	}

	err = launchable.Install()
	Assert(t).IsNil(err, "there should not have been an error when installing")

	Assert(t).AreEqual(fc.url, testLocation, "The correct url wasn't set for the curl library")

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
	launchable := &HoistLaunchable{
		Location:    testLocation,
		Id:          "testLaunchable",
		RunAs:       "testuser",
		ConfigDir:   tempDir,
		FetchToFile: new(FakeCurl).File,
		RootDir:     tempDir,
		Chpst:       "",
		Cgexec:      "",
	}

	installDir := launchable.InstallDir()

	expectedDir := path.Join(tempDir, "installs", "test_launchable_abc123")
	Assert(t).AreEqual(installDir, expectedDir, "Install dir did not have expected value")
}

func TestMultipleExecutables(t *testing.T) {
	fakeLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer cleanupFakeLaunchable(fakeLaunchable)
	executables, err := fakeLaunchable.Executables(runit.DefaultBuilder)

	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1", "/var/service/testPod__testLaunchable__script2"}
	Assert(t).AreEqual(2, len(executables), "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
	Assert(t).AreEqual(executables[1].Service.Path, expectedServicePaths[1], "Runit service paths from launchable did not match expected")
}

func TestSingleRunitService(t *testing.T) {
	launchable := FakeHoistLaunchableForDir("single_script_test_hoist_launchable")
	defer cleanupFakeLaunchable(launchable)
	Assert(t).IsNil(launchable.MakeCurrent(), "Should have been made current")
	executables, err := launchable.Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1"}
	Assert(t).AreEqual(len(executables), 1, "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
}

func TestLaunchExecutableOnlyRunitService(t *testing.T) {
	launchable := FakeHoistLaunchableForDir("launch_script_only_test_hoist_launchable")
	defer cleanupFakeLaunchable(launchable)
	Assert(t).IsNil(launchable.MakeCurrent(), "Should have been made current")
	executables, err := launchable.Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__launch"}
	Assert(t).AreEqual(len(executables), 1, "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
}

func TestDisable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer cleanupFakeLaunchable(hoistLaunchable)

	disableOutput, err := hoistLaunchable.Disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := "disable invoked\n"

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

func TestFailingDisable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	defer cleanupFakeLaunchable(hoistLaunchable)

	disableOutput, err := hoistLaunchable.Disable()
	Assert(t).IsNotNil(err, "Expected disable to fail for this test, but it didn't")

	expectedDisableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

// providing a disable script is optional, make sure we don't error
func TestNonexistentDisable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("nonexistent_scripts_test_hoist_launchable")
	defer cleanupFakeLaunchable(hoistLaunchable)

	disableOutput, err := hoistLaunchable.Disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := ""

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

func TestEnable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer cleanupFakeLaunchable(hoistLaunchable)

	enableOutput, err := hoistLaunchable.Enable()
	Assert(t).IsNil(err, "Got an unexpected error when calling enable on the test hoist launchable")

	expectedEnableOutput := "enable invoked\n"

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

func TestFailingEnable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	defer cleanupFakeLaunchable(hoistLaunchable)

	enableOutput, err := hoistLaunchable.Enable()
	Assert(t).IsNotNil(err, "Expected enable to fail for this test, but it didn't")

	expectedEnableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

// providing an enable script is optional, make sure we don't error
func TestNonexistentEnable(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("nonexistent_scripts_test_hoist_launchable")
	defer cleanupFakeLaunchable(hoistLaunchable)

	enableOutput, err := hoistLaunchable.Enable()
	Assert(t).IsNil(err, "Got an unexpected error when calling enable on the test hoist launchable")

	expectedEnableOutput := ""

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

func TestFailingStop(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer cleanupFakeLaunchable(hoistLaunchable)

	sv := runit.SV{util.From(runtime.Caller(0)).ExpandPath("erring_sv")}
	err := hoistLaunchable.Stop(runit.DefaultBuilder, &sv)

	Assert(t).IsNotNil(err, "Expected sv stop to fail for this test, but it didn't")
}

func TestStart(t *testing.T) {
	hoistLaunchable := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer cleanupFakeLaunchable(hoistLaunchable)
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
`, executables[0].Service.Name,
		hoistLaunchable.ConfigDir,
		executables[0].ExecPath,
		executables[1].Service.Name,
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
	defer cleanupFakeLaunchable(hoistLaunchable)
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
`, executables[0].Service.Name,
		hoistLaunchable.ConfigDir,
		executables[0].ExecPath,
		executables[1].Service.Name,
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
	defer cleanupFakeLaunchable(hoistLaunchable)

	sv := runit.SV{util.From(runtime.Caller(0)).ExpandPath("fake_sv")}
	err := hoistLaunchable.Stop(runit.DefaultBuilder, &sv)

	Assert(t).IsNil(err, "Got an unexpected error when attempting to stop runit services")
}
